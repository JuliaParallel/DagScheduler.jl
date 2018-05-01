# scheduler metadata store

#const MAP_NUM_ENTRIES = 1024*5     # max number of results to store in shared dict
const MAP_NUM_ENTRIES = 1024*100  # max number of results to store in shared dict
const MAP_ENTRY_SZ = 256           # max size of each result stored in shared dict
const DONE_TASKS_SZ = 1024*100     # size of shm, limits the max number of nodes in dag (roughly > (total_dag_nodes / nphyiscal nodes))
const SHARED_TASKS_SZ = 1024*100   # size of shm, limits the max number of nodes in dag (roughly > (total_dag_nodes / nphyiscal nodes))

mutable struct ShmemExecutorMeta <: ExecutorMeta
    path::String
    brokerid::Int
    shmdict::ShmDict
    proclocal::Dict{String,Any}
    allsharedtasks::SharedCircularDeque{TaskIdType}
    sharedtasks::SharedCircularDeque{TaskIdType}
    donetasks::SharedCircularDeque{TaskIdType}
    sharemode::ShareMode
    sharedcounter::ResourceCounter
    trigger::Union{Void,RemoteChannel{Channel{Void}}}
    add_annotation::Function
    del_annotation::Function
    result_callback_pos::Int
    result_callback::Union{Function,Void}

    function ShmemExecutorMeta(path::String, sharethreshold::Int)
        path = joinpath("/dev/shm", startswith(path, '/') ? path[2:end] : path)
        isdir(path) || mkpath(path)

        mkpath(shmdictpath(path))
        mkpath(donetaskspath(path))
        mkpath(sharedtaskspath(path))
        mkpath(allsharedtaskspath(path))
        mkpath(sharedcounterpath(path))

        nentries = get(DagScheduler.META_IMPL, :map_num_entries, MAP_NUM_ENTRIES)
        entry_sz = get(DagScheduler.META_IMPL, :map_entry_sz, MAP_ENTRY_SZ)
        shared_tasks_sz = get(DagScheduler.META_IMPL, :shared_tasks_sz, SHARED_TASKS_SZ)
        done_tasks_sz = get(DagScheduler.META_IMPL, :done_tasks_sz, DONE_TASKS_SZ)

        shmdict = ShmDict(shmdictpath(path), nentries, entry_sz; create=true)
        sharedtasks = SharedCircularDeque{TaskIdType}(sharedtaskspath(path), shared_tasks_sz; create=false)
        allsharedtasks = SharedCircularDeque{TaskIdType}(allsharedtaskspath(path), shared_tasks_sz; create=false)
        donetasks = SharedCircularDeque{TaskIdType}(donetaskspath(path), done_tasks_sz; create=false)
        sharedcounter = ResourceCounter(sharedcounterpath(path), 2; create=true)
        trigger = nothing
        new(path, 0, shmdict, Dict{String,Any}(),
            allsharedtasks, sharedtasks, donetasks,
            ShareMode(sharethreshold), sharedcounter, trigger,
            identity, identity, 0, nothing)
    end
end

donetaskspath(M::ShmemExecutorMeta) = donetaskspath(M.path)
donetaskspath(path::String) = joinpath(path, "tasks.done")
sharedtaskspath(M::ShmemExecutorMeta) = sharedtaskspath(M.path)
sharedtaskspath(path::String) = joinpath(path, "tasks.shared")
allsharedtaskspath(M::ShmemExecutorMeta) = allsharedtaskspath(M.path)
allsharedtaskspath(path::String) = joinpath(path, "tasks.allshared")
sharedcounterpath(M::ShmemExecutorMeta) = sharedcounterpath(M.path)
sharedcounterpath(path::String) = joinpath(path, "counter")
shmdictpath(M::ShmemExecutorMeta) = shmdictpath(M.path)
shmdictpath(path::String) = joinpath(path, "shmdict")

detach(M::ShmemExecutorMeta, pid) = Dict{TaskIdType,TaskIdType}()

function init(M::ShmemExecutorMeta, brokerid::Int; add_annotation=identity, del_annotation=identity, result_callback=nothing)
    M.brokerid = brokerid
    M.add_annotation = add_annotation
    M.del_annotation = del_annotation
    M.result_callback = result_callback
    M.trigger = brokercall(broker_register, M)
    nothing
end

function sync_sharemode(M::ShmemExecutorMeta)
    created, deleted = count(M.sharedcounter)
    if created < deleted
        created = deleted
    end
    shared = created - deleted
    M.sharemode.nshared = shared
    M.sharemode.ncreated = created
    M.sharemode.ndeleted = deleted
    nothing
end

function wait_trigger(M::ShmemExecutorMeta; timeoutsec::Int=5)
    trigger = M.trigger
    fire = true
    if !isready(trigger)
        @schedule begin
            sleep(timeoutsec)
            fire && !isready(trigger) && put!(trigger, nothing)
        end
    end
    take!(trigger)
    fire = false
    if M.result_callback !== nothing
        invoke_result_callbacks(M)
    end
    sync_sharemode(M)
    nothing
end

function pull_trigger(M::ShmemExecutorMeta)
    brokercall(broker_ping, M)
    nothing
end

function invoke_result_callbacks(M::ShmemExecutorMeta)
    L = withlock(M.donetasks.lck) do
        length(M.donetasks)
    end
    ids = TaskIdType[M.donetasks[idx] for idx in (M.result_callback_pos+1):L]
    M.result_callback_pos += length(ids)
    for id in ids
        val = get_result(M, id)
        M.result_callback(id, val)
    end
    nothing
end

function delete!(M::ShmemExecutorMeta)
    empty!(M.donetasks)
    empty!(M.sharedtasks)
    empty!(M.allsharedtasks)
    Semaphores.reset(M.sharedcounter, Cushort[0,0])

    shared_tasks_sz = get(DagScheduler.META_IMPL, :shared_tasks_sz, SHARED_TASKS_SZ)
    done_tasks_sz = get(DagScheduler.META_IMPL, :done_tasks_sz, DONE_TASKS_SZ)

    M.allsharedtasks = SharedCircularDeque{TaskIdType}(allsharedtaskspath(M), shared_tasks_sz; create=true)
    M.sharedtasks = SharedCircularDeque{TaskIdType}(sharedtaskspath(M), shared_tasks_sz; create=true)
    M.donetasks = SharedCircularDeque{TaskIdType}(donetaskspath(M), done_tasks_sz; create=true)
    if myid() === M.brokerid
        deregister(pinger)
        withlock(M.shmdict.lck) do
            empty!(M.shmdict)
        end
    end

    nothing
end

function reset(M::ShmemExecutorMeta; delete::Bool=false, dropdb::Bool=true)
    empty!(M.proclocal)
    DagScheduler.reset(M.sharemode)
    M.add_annotation = identity
    M.del_annotation = identity
    M.result_callback = nothing
    M.result_callback_pos = 0
    nothing
end

function cleanup(M::ShmemExecutorMeta)
    delete!(M.allsharedtasks)
    delete!(M.sharedtasks)
    delete!(M.donetasks)
    delete!(M.sharedcounter)
    delete!(M.shmdict)
    rm(M.path; force=true, recursive=true)
    nothing
end

function share_task(M::ShmemExecutorMeta, id::TaskIdType, allow_dup::Bool)
    canshare = false
    withlock(M.allsharedtasks.lck) do
        if !(id in M.allsharedtasks)
            canshare = true
            push!(M.allsharedtasks, id)
        end
    end

    canshare |= allow_dup

    if canshare
        annotated = M.add_annotation(id)
        withlock(M.sharedtasks.lck) do
            push!(M.sharedtasks, annotated)
        end
        change(M.sharedcounter, [SemBuf(0,1)])
        sync_sharemode(M)
        pull_trigger(M)
    end
    nothing
end

function steal_task(M::ShmemExecutorMeta, selector=default_task_scheduler)
    task = NoTask
    withlock(M.sharedtasks.lck) do
        if !isempty(M.sharedtasks)
            pos = selector(M.sharedtasks[1:end])
            task = splice!(M.sharedtasks, pos)
        end
    end
    if task !== NoTask
        change(M.sharedcounter, [SemBuf(1,1)])
        sync_sharemode(M)
        pull_trigger(M)
        task = M.del_annotation(task)
    end
    task
end

function set_result(M::ShmemExecutorMeta, id::TaskIdType, val; refcount::UInt64=UInt64(1), processlocal::Bool=true)
    k = resultpath(M, id)
    M.proclocal[k] = val

    if !processlocal
        val = meta_pack(val)
        withlock(M.shmdict.lck) do
            M.shmdict[k] = (val,refcount)
        end

        withlock(M.donetasks.lck) do
            push!(M.donetasks, id)
        end

        pull_trigger(M)
    end

    sync_sharemode(M)

    nothing
end

function get_result(M::ShmemExecutorMeta, id::TaskIdType)
    k = resultpath(M, id)
    if k in keys(M.proclocal)
        val = M.proclocal[k]
    else
        sval = withlock(M.shmdict.lck) do
            M.shmdict[k]
        end
        val, refcount = deserialize(IOBuffer(sval))
    end
    meta_unpack(val)
end

function has_result(M::ShmemExecutorMeta, id::TaskIdType)
    k = resultpath(M, id)
    (k in keys(M.proclocal)) && (return true)

    withlock(M.donetasks.lck) do
        id in M.donetasks
    end
end

function decr_result_ref(M::ShmemExecutorMeta, id::TaskIdType)
    k = resultpath(M, id)

    withlock(M.shmdict.lck) do
        val, refcount = deserialize(IOBuffer(M.shmdict[k]))
        refcount -= 1
        if refcount > 0
            M.shmdict[k] = (val,refcount)
        else
            delete!(M.shmdict, k)
            (k in keys(M.proclocal)) && delete!(M.proclocal, k)
        end
        refcount
    end
end

function export_local_result(M::ShmemExecutorMeta, id::TaskIdType, executable, refcount::UInt64)
    k = resultpath(M, id)
    (k in keys(M.proclocal)) || return

    val = repurpose_result_to_export(executable, M.proclocal[k])
    val = meta_pack(val)

    withlock(M.shmdict.lck) do
        M.shmdict[k] = (val,refcount)
    end

    withlock(M.donetasks.lck) do
        push!(M.donetasks, id)
    end

    pull_trigger(M)

    nothing
end

ifpingneeded(recepient::RemoteChannel{Channel{Void}}, val::Void) = !isready(recepient)

function broker_ping()
    put!(pinger, nothing, ifpingneeded)
    nothing
end

function broker_register()
    register(pinger)
end
