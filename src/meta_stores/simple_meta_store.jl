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
    add_annotation::Function
    del_annotation::Function
    result_callback::Union{Function,Void}
    cachingpool::Union{Base.Distributed.CachingPool, Void}

    function SimpleExecutorMeta(path::String, sharethreshold::Int)
        new(path, myid(),
            Dict{String,Any}(),
            Set{TaskIdType}(),
            ShareMode(sharethreshold),
            nothing,
            identity, identity, nothing, nothing)
    end
end

function Base.show(io::IO, M::SimpleExecutorMeta)
    print(io, "SimpleExecutorMeta(", M.path, ")")
end

function init(M::SimpleExecutorMeta, brokerid::Int; add_annotation=identity, del_annotation=identity, result_callback=nothing)
    M.brokerid = brokerid
    M.add_annotation = add_annotation
    M.del_annotation = del_annotation
    M.result_callback = result_callback
    pid = myid()
    M.results_channel = brokercall(broker_register_for_results, M, pid)
    M.cachingpool = Base.Distributed.CachingPool([brokerid])
    donetasks = brokercall(broker_get_donetasks, M)::Set{TaskIdType}
    union!(M.donetasks, donetasks)
    nothing
end

function process_trigger(M::SimpleExecutorMeta, k::String, v::String)
    if k == TIMEOUT_KEY
        # ignore
    elseif k == SHAREMODE_KEY
        ncreated,ndeleted = meta_deser(v)
        S = M.sharemode
        if ncreated > S.ncreated
            S.ncreated = ncreated
        end
        if ndeleted > S.ndeleted
            S.ndeleted = ndeleted
        end
        S.nshared = ncreated - ndeleted
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
        empty!(PID_RR_MAP)
        empty!(PID_TASK_MAP)
        deregister(RESULTS)
    end
    nothing
end

function reset(M::SimpleExecutorMeta)
    reset(M.sharemode)
    empty!(M.proclocal)
    empty!(M.donetasks)
    M.cachingpool = nothing
    M.results_channel = nothing
    M.add_annotation = identity
    M.del_annotation = identity
    M.result_callback = nothing
    
    nothing
end

function cleanup(M::SimpleExecutorMeta)
end

function detach(M::SimpleExecutorMeta, pid)
    taskmap = Dict{TaskIdType,TaskIdType}()
    if (myid() === M.brokerid) && (pid in keys(PID_RR_MAP))
        rr = PID_RR_MAP[pid]
        delete!(PID_RR_MAP, pid)
        deregister(RESULTS, rr)
        if pid in keys(PID_TASK_MAP)
            taskmap = PID_TASK_MAP[pid]
            delete!(PID_TASK_MAP, pid)
        end
    end
    taskmap
end

@timetrack function share_task(M::SimpleExecutorMeta, id::TaskIdType, allow_dup::Bool)
    annotated_task = M.add_annotation(id)
    pid = myid()
    brokercall(broker_share_task, M, id, annotated_task, allow_dup, pid)
    nothing
end

@timetrack function steal_task(M::SimpleExecutorMeta, selector=default_task_scheduler)
    pid = myid()
    taskid = remotecall_fetch(broker_steal_task, M.cachingpool, selector, pid)::TaskIdType
    ((taskid === NoTask) ? taskid : M.del_annotation(taskid))::TaskIdType
end

@timetrack function set_result(M::SimpleExecutorMeta, id::TaskIdType, val; refcount::UInt64=UInt64(1), processlocal::Bool=true)
    k = resultpath(M, id)
    M.proclocal[k] = val
    if !processlocal
        val = meta_pack(val)
        serval = meta_ser((val,refcount))
        pid = myid()
        brokercall(broker_set_result, M, k, serval, id, pid)
    end
    push!(M.donetasks, id)
    nothing
end

@timetrack function get_result(M::SimpleExecutorMeta, id::TaskIdType)
    k = resultpath(M, id)
    if k in keys(M.proclocal)
        val = M.proclocal[k]
    else
        v = brokercall(broker_get_result, M, k)::String
        val, refcount = meta_deser(v)
        M.proclocal[k] = val
    end
    meta_unpack(val)
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
    val = meta_pack(val)
    serval = meta_ser((val,refcount))
    pid = myid()
    brokercall(broker_set_result, M, k, serval, id, pid)
    nothing
end

# --------------------------------------------------
# methods invoked at the broker
# --------------------------------------------------
function broker_has_result(k)
    k in keys(META)
end

function broker_share_task(id::TaskIdType, annotated::TaskIdType, allow_dup::Bool, pid::Int)
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
        broker_remove_pending_task(id, pid)
    end
    nothing
end

function broker_steal_task(selector, executorid)
    genv = DagScheduler.genv[]
    (genv === nothing) && (return NoTask)
    M = (genv.meta)::SimpleExecutorMeta
    taskid = withtaskmutex() do
        taskid = NoTask
        if !isempty(TASKS)
            pos = selector(TASKS)
            taskid = splice!(TASKS, pos)
            M.sharemode.nshared -= 1
            M.sharemode.ndeleted += 1
        end
        taskid
    end
    if taskid !== NoTask
        broker_send_sharestats(M)
        broker_add_pending_task(M.del_annotation(taskid), taskid, executorid)
    end
    taskid
end

function broker_send_sharestats(M)
    @schedule begin
        sm = M.sharemode
        c,d = sm.ncreated,sm.ndeleted
        put!(RESULTS, (SHAREMODE_KEY, meta_ser((c,d))))
    end
    nothing
end

function broker_set_result(k::String, val::String, id, pid)
    META[k] = val
    put!(RESULTS, (k,val))
    broker_remove_pending_task(id, pid)
    nothing
end

# remove task id from pending list of broker id
broker_remove_pending_task(id, pid) = broker_remove_pending_task(TaskIdType(id), Int(pid))
function broker_remove_pending_task(id::TaskIdType, pid::Int)
    (pid in keys(PID_TASK_MAP)) && delete!(PID_TASK_MAP[pid], id)
    nothing
end

# add task id as pending from executor
broker_add_pending_task(id, annotatedid, pid) = broker_add_pending_task(TaskIdType(id), TaskIdType(annotatedid), Int(pid))
function broker_add_pending_task(id::TaskIdType, annotatedid::TaskIdType, pid::Int)
    tasks = get!(()->Dict{TaskIdType,TaskIdType}(), PID_TASK_MAP, pid)
    tasks[id] = annotatedid
    nothing
end

function broker_get_result(k::String)
    META[k]
end

function broker_register_for_results(pid)
    rr = register(RESULTS)
    PID_RR_MAP[pid] = rr
    rr
end

function broker_get_donetasks()
    genv = DagScheduler.genv[]
    (genv === nothing) && (return Set{TaskIdType}())
    M = (genv.meta)::SimpleExecutorMeta
    M.donetasks
end
