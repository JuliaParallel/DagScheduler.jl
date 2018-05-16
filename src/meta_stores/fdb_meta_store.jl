# foundationdb scheduler metadata store

"""
Metadata store using Foundation DB
"""
mutable struct FdbExecutorMeta <: ExecutorMeta
    path::String
    brokerid::Int
    proclocal::Dict{String,Any}
    donetasks::Set{TaskIdType}
    sharemode::ShareMode
    add_annotation::Function
    del_annotation::Function
    result_callback::Union{Function,Void}
    taskstore::FdbTaskStore
    resultstore::FdbResultStore
    trigger::Channel

    function FdbExecutorMeta(path::String, sharethreshold::Int)
        sharemode = ShareMode(sharethreshold)
        trigger = Channel{Void}(64)
        proclocal = Dict{String,Any}()
        donetasks = Set{TaskIdType}()

        taskstore = FdbTaskStore(fdb_db[], path, sharemode)
        resultstore = FdbResultStore(fdb_db[], path)
        obj = new(path, myid(), proclocal, donetasks, sharemode,
            identity, identity, nothing,
            taskstore, resultstore, trigger)

        task_trigger = (x)->begin
            isready(trigger) || put!(trigger, nothing)
            nothing
        end
        result_trigger = (tid, resultbytes)->begin
            # TODO: avoid String, we don't need base64 encoding here
            val,refcount = meta_deser(resultbytes)
            k = resultpath(obj, tid)
            proclocal[k] = val
            push!(donetasks, tid)
            if obj.result_callback !== nothing
                obj.result_callback(tid, val)
            end
            isready(trigger) || put!(trigger, nothing)
            nothing
        end

        set_callback(taskstore, task_trigger)
        set_callback(resultstore, result_trigger)
        obj
    end
end

function Base.show(io::IO, M::FdbExecutorMeta)
    print(io, "FdbExecutorMeta(", M.path, ")")
end

function init(M::FdbExecutorMeta, brokerid::Int; add_annotation=identity, del_annotation=identity, result_callback=nothing)
    M.brokerid = brokerid
    M.add_annotation = add_annotation
    M.del_annotation = del_annotation
    M.result_callback = result_callback

    init(M.taskstore)
    init(M.resultstore)
    start_processing_events(M.taskstore)
    start_processing_events(M.resultstore)
    nothing
end

function wait_trigger(M::FdbExecutorMeta; timeoutsec::Int=5)
    trigger = M.trigger
    fire = true
    if !isready(trigger)
        @schedule begin
            sleep(timeoutsec)
            fire && !isready(trigger) && put!(trigger, nothing)
        end
    end

    # process results and sharemode notifications
    take!(M.trigger)
    fire = false

    nothing
end

function delete!(M::FdbExecutorMeta)
    reset(M)
    if myid() === M.brokerid
        clear(M.taskstore)
        clear(M.resultstore)
    end
    nothing
end

function reset(M::FdbExecutorMeta)
    stop_processing_events(M.taskstore)
    stop_processing_events(M.resultstore)

    reset(M.sharemode)
    empty!(M.proclocal)
    empty!(M.donetasks)
    M.add_annotation = identity
    M.del_annotation = identity
    M.result_callback = nothing
    
    nothing
end

function cleanup(M::FdbExecutorMeta)
end

function detach(M::FdbExecutorMeta, pid)
    taskmap = Dict{TaskIdType,TaskIdType}()
    if myid() === M.brokerid
        for tid in keys(M.taskstore.taskprops)
            key, annotated_tid, reservation = M.taskstore.taskprops[tid]
            if reservation == pid
                taskmap[tid] = annotated_tid
            end
        end
    end
    taskmap
end

@timetrack function share_task(M::FdbExecutorMeta, id::TaskIdType, allow_dup::Bool)
    annotated_task = M.add_annotation(id)
    new_task(M.taskstore, id, annotated_task; allow_duplicate=allow_dup)
    yield()  # so that the event thread gets to pick up notifications if any
    nothing
end

@timetrack function steal_task(M::FdbExecutorMeta, selector=default_task_scheduler)
    taskid = NoTask
    while !isempty(M.taskstore.taskids)
        pos = selector(M.taskstore.taskids)
        trytaskid = M.taskstore.taskids[pos]
        if reserve_task(M.taskstore, trytaskid, myid())
            key, annotated_tid, reservation = M.taskstore.taskprops[trytaskid]
            taskid = M.del_annotation(annotated_tid)::TaskIdType
            (trytaskid in M.taskstore.taskids) && splice!(M.taskstore.taskids, pos)
            break
        else
            (trytaskid in M.taskstore.taskids) && splice!(M.taskstore.taskids, pos)
        end
    end
    taskid
end

@timetrack function set_result(M::FdbExecutorMeta, id::TaskIdType, val; refcount::UInt64=UInt64(1), processlocal::Bool=true)
    k = resultpath(M, id)
    M.proclocal[k] = val
    if !processlocal
        val = meta_pack(val)
        serval = meta_ser((val,refcount))
        publish_result(M.resultstore, id, convert(Vector{UInt8}, serval))
        finish_task(M.taskstore, id)
    end
    push!(M.donetasks, id)
    yield()  # so that the event thread gets to pick up notifications if any
    nothing
end

@timetrack function get_result(M::FdbExecutorMeta, id::TaskIdType)
    k = resultpath(M, id)
    (k in keys(M.proclocal)) || on_event(M.resultstore)
    val = M.proclocal[k]
    meta_unpack(val)
end

function has_result(M::FdbExecutorMeta, id::TaskIdType)
    id in M.donetasks
end

function decr_result_ref(M::FdbExecutorMeta, id::TaskIdType)
    2
end

function export_local_result(M::FdbExecutorMeta, id::TaskIdType, executable, refcount::UInt64)
    k = resultpath(M, id)
    (k in keys(M.proclocal)) || return

    val = repurpose_result_to_export(executable, M.proclocal[k])
    val = meta_pack(val)
    serval = meta_ser((val,refcount))

    publish_result(M.resultstore, id, convert(Vector{UInt8}, serval))
    finish_task(M.taskstore, id)

    nothing
end

