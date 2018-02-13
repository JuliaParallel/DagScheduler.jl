# simple scheduler metadata store

const SHAREMODE_KEY = "S"
const TIMEOUT_KEY = ""

"""
Metadata store using broker in-memory datastructures
"""
mutable struct SimpleExecutorMeta <: ExecutorMeta
    path::String
    brokerid::Int
    proclocal::Dict{String,Any}
    donetasks::Set{TaskIdType}
    sharemode::ShareMode
    results_channel::Union{Void,RemoteChannel{Channel{Tuple{String,String}}}}
    gen::Int
    add_annotation::Function
    del_annotation::Function
    result_callback::Union{Function,Void}

    function SimpleExecutorMeta(path::String, sharethreshold::Int)
        new(path, myid(),
            Dict{String,Any}(),
            Set{TaskIdType}(),
            ShareMode(sharethreshold),
            nothing, 0,
            identity, identity, nothing)
    end
end

function Base.show(io::IO, M::SimpleExecutorMeta)
    print(io, "SimpleExecutorMeta(", M.path, ")")
end

function init(M::SimpleExecutorMeta, brokerid::Int; add_annotation=identity, del_annotation=identity, result_callback=nothing)
    M.brokerid = brokerid
    M.gen = 0
    M.add_annotation = add_annotation
    M.del_annotation = del_annotation
    M.result_callback = result_callback
    M.results_channel = brokercall(broker_register_for_results, M)
    donetasks = brokercall(broker_get_donetasks, M)::Set{TaskIdType}
    union!(M.donetasks, donetasks)
    nothing
end

function process_trigger(M::SimpleExecutorMeta, k::String, v::String)
    if k == TIMEOUT_KEY
        # ignore
    elseif k == SHAREMODE_KEY
        ncreated,ndeleted = meta_deser(v)
        if (ncreated > M.sharemode.ncreated) || (ndeleted > M.sharemode.ndeleted)
            M.sharemode.nshared = ncreated - ndeleted
            M.sharemode.ncreated = ncreated
            M.sharemode.ndeleted = ndeleted
        end
    else
        val, refcount = meta_deser(v)
        M.proclocal[k] = val
        id = parse(TaskIdType, basename(k))
        push!(M.donetasks, id)
        if M.result_callback !== nothing
            M.result_callback(id, val)
        end
    end
    nothing
end

function process_triggers(M::SimpleExecutorMeta)
    while isready(M.results_channel)
        (k,v) = take!(M.results_channel)
        process_trigger(M, k, v)
    end
end

function wait_trigger(M::SimpleExecutorMeta; timeoutsec::Int=5)
    trigger = M.results_channel
    fire = true
    if !isready(trigger)
        @schedule begin
            sleep(timeoutsec)
            fire && !isready(trigger) && put!(trigger, (TIMEOUT_KEY,""))
        end
    end

    # process results and sharemode notifications
    (k,v) = take!(M.results_channel)
    fire = false
    process_trigger(M, k, v)
    process_triggers(M)

    nothing
end

function delete!(M::SimpleExecutorMeta)
    reset(M)
    if myid() === M.brokerid
        empty!(META)
        empty!(TASKS)
        deregister(RESULTS)
    end
    nothing
end

function reset(M::SimpleExecutorMeta)
    reset(M.sharemode)
    empty!(M.proclocal)
    empty!(M.donetasks)
    M.results_channel = nothing
    M.add_annotation = identity
    M.del_annotation = identity
    M.result_callback = nothing
    M.gen = 0
    
    nothing
end

function cleanup(M::SimpleExecutorMeta)
end

function share_task(M::SimpleExecutorMeta, id::TaskIdType, allow_dup::Bool)
    annotated_task = M.add_annotation(id)
    brokercall(broker_share_task, M, id, annotated_task, allow_dup)
    nothing
end

function steal_task(M::SimpleExecutorMeta)
    taskid = brokercall(broker_steal_task, M)::TaskIdType
    ((taskid === NoTask) ? taskid : M.del_annotation(taskid))::TaskIdType
end

function set_result(M::SimpleExecutorMeta, id::TaskIdType, val; refcount::UInt64=UInt64(1), processlocal::Bool=true)
    process_triggers(M)
    k = resultpath(M, id)
    M.proclocal[k] = val
    if !processlocal
        serval = meta_ser((val,refcount))
        brokercall(broker_set_result, M, k, serval)
    end
    push!(M.donetasks, id)
    nothing
end

function get_result(M::SimpleExecutorMeta, id::TaskIdType)
    process_triggers(M)
    k = resultpath(M, id)
    if k in keys(M.proclocal)
        M.proclocal[k]
    else
        v = brokercall(broker_get_result, M, k)::String
        val, refcount = meta_deser(v)
        M.proclocal[k] = val
        val
    end
end

function has_result(M::SimpleExecutorMeta, id::TaskIdType)
    #process_triggers(M)
    id in M.donetasks
end

function decr_result_ref(M::SimpleExecutorMeta, id::TaskIdType)
    2
end

function export_local_result(M::SimpleExecutorMeta, id::TaskIdType, executable, refcount::UInt64)
    k = resultpath(M, id)
    (k in keys(M.proclocal)) || return

    exists = brokercall(broker_has_result, M, k)::Bool
    exists && return

    val = repurpose_result_to_export(executable, M.proclocal[k])
    serval = meta_ser((val,refcount))
    brokercall(broker_set_result, M, k, ser_val)
    nothing
end

# --------------------------------------------------
# methods invoked at the broker
# --------------------------------------------------
function withtaskmutex(f)
    lock(taskmutex[])
    try
        return f()
    finally
        unlock(taskmutex[])
    end
end

function broker_has_result(k)
    k in keys(META)
end

function broker_share_task(id::TaskIdType, annotated::TaskIdType, allow_dup::Bool)
    M = (DagScheduler.genv[].meta)::SimpleExecutorMeta
    s = sharepath(M, id)
    canshare = withtaskmutex() do
        if !(s in keys(META))
            META[s] = ""
            true
        else
            false
        end
    end
    canshare |= allow_dup
    if canshare
        push!(TASKS, annotated)
        M.sharemode.ncreated += 1
        M.sharemode.nshared += 1
        broker_send_sharestats(M)
    end
    nothing
end

function broker_steal_task()
    genv = DagScheduler.genv[]
    (genv === nothing) && (return NoTask)
    M = (genv.meta)::SimpleExecutorMeta
    taskid = withtaskmutex() do
        taskid = NoTask
        if !isempty(TASKS)
            taskid = shift!(TASKS)
            M.sharemode.nshared -= 1
            M.sharemode.ndeleted += 1
        end
        taskid
    end
    (taskid !== NoTask) && broker_send_sharestats(M)
    taskid
end

function broker_send_sharestats(M)
    gen = M.gen
    @schedule begin
        if M.gen <= gen
            M.gen = gen + 1
            sm = M.sharemode
            c,d = sm.ncreated,sm.ndeleted
            put!(RESULTS, (SHAREMODE_KEY, meta_ser((c,d))))
        end
    end
    nothing
end

function broker_set_result(k::String, val::String)
    META[k] = val
    put!(RESULTS, (k,val))
    nothing
end

function broker_get_result(k::String)
    META[k]
end

function broker_register_for_results()
    register(RESULTS)
end

function broker_get_donetasks()
    genv = DagScheduler.genv[]
    (genv === nothing) && (return Set{TaskIdType}())
    M = (genv.meta)::SimpleExecutorMeta
    M.donetasks
end
