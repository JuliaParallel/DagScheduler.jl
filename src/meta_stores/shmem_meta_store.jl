# scheduler metadata store

const MAP_SZ = 1000^3              # size of shared dict for results and ref counts
const DONE_TASKS_SZ = 1024*100     # size of shm, limits the max number of nodes in dag (roughly > (total_dag_nodes / nphyical nodes))
const SHARED_TASKS_SZ = 1024*100   # size of shm, limits the max number of nodes in dag (roughly > (total_dag_nodes / nphyical nodes))

mutable struct ShmemExecutorMeta <: ExecutorMeta
    path::String
    brokerid::Int
    env::Environment
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
        env = LMDB.create()

        # we want it to be sync, since we are using across processes
        isflagset(env[:Flags], Cuint(LMDB.NOSYNC)) && unset!(env, LMDB.NOSYNC)

        env[:Readers] = 1
        env[:MapSize] = MAP_SZ
        env[:DBs] = 1

        dbpath = lmdbpath(path)
        isdir(dbpath) || mkpath(dbpath)
        open(env, dbpath)

        mkpath(donetaskspath(path))
        mkpath(sharedtaskspath(path))
        mkpath(allsharedtaskspath(path))
        mkpath(sharedcounterpath(path))
        sharedtasks = SharedCircularDeque{TaskIdType}(sharedtaskspath(path), SHARED_TASKS_SZ; create=false)
        allsharedtasks = SharedCircularDeque{TaskIdType}(allsharedtaskspath(path), SHARED_TASKS_SZ; create=false)
        donetasks = SharedCircularDeque{TaskIdType}(donetaskspath(path), DONE_TASKS_SZ; create=false)
        sharedcounter = ResourceCounter(sharedcounterpath(path), 2; create=true)
        trigger = nothing
        new(path, 0, env, Dict{String,Any}(),
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
lmdbpath(path::String) = joinpath(path, "lmdb")

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
    M.allsharedtasks = SharedCircularDeque{TaskIdType}(allsharedtaskspath(M), SHARED_TASKS_SZ; create=true)
    M.sharedtasks = SharedCircularDeque{TaskIdType}(sharedtaskspath(M), SHARED_TASKS_SZ; create=true)
    M.donetasks = SharedCircularDeque{TaskIdType}(donetaskspath(M), DONE_TASKS_SZ; create=true)
    if myid() === M.brokerid
        deregister(pinger)
    end
    txn = start(M.env)
    dbi = open(txn)
    drop(txn, dbi; delete=true)
    commit(txn)
    close(M.env, dbi)
    close(M.env)

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

function steal_task(M::ShmemExecutorMeta)
    task = NoTask
    withlock(M.sharedtasks.lck) do
        isempty(M.sharedtasks) || (task = shift!(M.sharedtasks))
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
        txn = start(M.env)
        dbi = open(txn)
        try
            put!(txn, dbi, k, meta_ser((val,refcount)))
        catch ex
            rethrow(ex)
        finally
            commit(txn)
            close(M.env, dbi)
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
        M.proclocal[k]
    else
        txn = start(M.env)
        dbi = open(txn)
        try
            val, refcount = meta_deser(get(txn, dbi, k, String))
            M.proclocal[k] = val
            val
        catch ex
            rethrow(ex)
        finally
            commit(txn)
            close(M.env, dbi)
        end
    end
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

    txn = start(M.env)
    dbi = open(txn)
    try
        val, refcount = meta_deser(get(txn, dbi, k, String))
        refcount -= 1
        if refcount > 0
            put!(txn, dbi, k, meta_ser((val,refcount)))
        else
            delete!(txn, dbi, k, C_NULL)
            (k in keys(M.proclocal)) && delete!(M.proclocal, k)
        end
        return refcount
    catch ex
        rethrow(ex)
    finally
        commit(txn)
        close(M.env, dbi)
    end
end

function export_local_result(M::ShmemExecutorMeta, id::TaskIdType, executable, refcount::UInt64)
    k = resultpath(M, id)
    (k in keys(M.proclocal)) || return

    val = repurpose_result_to_export(executable, M.proclocal[k])

    txn = start(M.env)
    dbi = open(txn)
    try
        put!(txn, dbi, k, meta_ser((val,refcount)))
    catch ex
        rethrow(ex)
    finally
        commit(txn)
        close(M.env, dbi)
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
