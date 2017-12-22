ping(env::Sched, event=nothing) = isready(env.pinger) || put!(env.pinger, event)

function do_broking(env::Sched, root::TaskIdType, maxpings::Int)
    tasklog(env, "broker waiting for ping")
    pinger = env.pinger
    take!(pinger)
    while isready(pinger) && (maxpings > 0)
        take!(pinger)
        maxpings -= 1
    end
    !has_result(env.meta, root)
end

function steal_from_peers(env::Sched, peers::Vector{SchedPeer})
    stealbufsz = length(peers)^2
    nstolen = 0

    peers = shuffle(peers)

    if !has_shared(env, stealbufsz)
        tasklog(env, "broker trying to steal tasks")
        for idx in 1:length(peers)
            has_shared(env, stealbufsz) && break
            peer_env = peers[idx]
            while !has_shared(env, stealbufsz)
                task = steal(env, peer_env)
                (task === NoTask) && break
                keep(env, task, 0, false)
                nstolen += 1
                tasklog(env, "broker stole task ", task, " from executor ", peer_env.id, " (", peer_env.name, ")")
            end
        end
    end
    nstolen
end

#------------------------------------------------------------------
# per process scheduler context
#------------------------------------------------------------------
const genv = Ref{Union{Sched,Void}}(nothing)
const maxbackoff = 0.2
const startbackoff = 0.001

#-------------------------------------------------------------------
# broker provides the initial tasks and coordinates among executors
# by stealing spare tasks from all peers and letting peers steal
#--------------------------------------------------------------------
function runbroker(broker_name::String, t, executors::Vector{String}, pinger::RemoteChannel; metastore::String="/dev/shm/scheduler", debug::Bool=false)
    if genv[] === nothing
        env = genv[] = Sched(broker_name, :broker, pinger, metastore, typemax(Int); debug=debug)
    else
        env = (genv[])::Sched
    end

    init(env, t)
    tasklog(env, "broker invoked")

    nstolen = 0
    peers = SchedPeer[]
    try
        task = root = taskid(t)
        keep(env, t, 0, false)
        tasklog(env, "broker started with ", task)

        # connect to the executor deques
        for executor_name in executors
            push!(peers, SchedPeer(executor_name))
            tasklog(env, "broker connected to peer ", executor_name)
        end

        # steal tasks from executors and make them available to other executors
        while do_broking(env, root, length(executors))
            nstolen += steal_from_peers(env, peers)
        end

        tasklog(env, "broker stole ", nstolen, " tasks")
        #info("broker stole $nstolen, shared $(env.nshared[]) tasks")
        res = get_result(env.meta, root)
        return isa(res, Chunk) ? collect(res) : res
    catch ex
        taskexception(env, ex, catch_backtrace())
        rethrow(ex)
    finally
        async_reset(env)
    end
end

function runexecutor(broker_name::String, executor_name::String, root_t, pinger::RemoteChannel; metastore::String="/dev/shm/scheduler", debug::Bool=false, help_threshold::Int=typemax(Int))
    if genv[] === nothing
        env = genv[] = Sched(executor_name, :executor, pinger, metastore, help_threshold; debug=debug)
    else
        env = (genv[])::Sched
    end
    init(env, root_t)
    tasklog(env, "executor starting")
    broker = SchedPeer(broker_name)
    nstolen = 0
    nexecuted = 0

    try
        root = taskid(root_t)
        task = NoTask
        lasttask = task
        backoff = startbackoff

        while !has_result(env.meta, root)
            nodeshareqsz = env.nodeshareqsz[] = withlock(broker.shared.lck) do
                length(broker.shared)
            end
            tasklog(env, "executor trying to do tasks, remaining ", length(env.reserved), ", shared ", length(env.shared), ", shared in node ", nodeshareqsz)

            if should_share_reserved(env, broker)
                reserve_to_share(env)
                (nodeshareqsz < help_threshold) && ping(env)
            end

            task = reserve(env)

            # if no tasks in own queue, steal tasks from broker
            if task === NoTask || task === lasttask
                tasklog(env, "got notask or lasttask, will steal")
                (nodeshareqsz > 0) && (task = steal(env, broker))
                (nodeshareqsz < help_threshold) && ping(env)
                if task === NoTask
                    tasklog(env, "stole notask")
                    backoff = min(maxbackoff, backoff+startbackoff)
                    sleep(backoff)  # do not overwhelm the broker
                else
                    keep(env, task, 0, true)
                    nstolen += 1
                    tasklog(env, "executor stole ", task, ", remaining ", length(env.reserved))
                    backoff = startbackoff
                end
            end

            if task !== NoTask
                # execute task
                if !(complete = has_result(env.meta, task))
                    complete = keep(env, task, 1, true)
                    if !complete
                        if runnable(env, task)
                            complete = exec(env, task)
                            complete && (nexecuted += 1)
                        end
                    end
                end
                complete && (backoff = startbackoff)
                release(env, task, complete)
                tasklog(env, "executor ", task, " is complete: ", complete, ", remaining ", length(env.reserved))
            end
            lasttask = task
        end
        ping(env)
        tasklog(env, "executor ", env.name, " stole ", nstolen, ", shared ", env.nshared[], ", completed ", nexecuted, " tasks")
    catch ex
        taskexception(env, ex, catch_backtrace())
        rethrow(ex)
    finally
        async_reset(env)
    end
    #tasklog(env, "executor done")
    (nstolen, env.nshared[], nexecuted)
end

struct RunEnv
    runpath::String
    broker::String
    executors::Vector{String}
    metastore::String
    executor_tasks::Vector{Future}
    deques::Vector{SharedCircularDeque{TaskIdType}}
    pinger::RemoteChannel{Channel{Any}}
    pinger_task::Ref{Union{Task,Void}}
    debug::Bool

    function RunEnv(nexecutors::Int=nworkers(), debug::Bool=false)
        ((nexecutors > 1) || (nworkers() < nexecutors)) || error("need at least two workers")
        executor_tasks = Future[]
        executors = String[]
        deques = SharedCircularDeque{TaskIdType}[]
        pinger = RemoteChannel()

        runpath = "/dev/shm/jsch$(getpid())"

        # create and initialize the circular deques
        brokerpath = joinpath(runpath, "broker")
        mkpath(brokerpath)
        push!(deques, SharedCircularDeque{TaskIdType}(brokerpath, 1024; create=true))
        for idx in 1:nexecutors
            executorpath = joinpath(runpath, "executor$idx")
            mkpath(executorpath)
            push!(deques, SharedCircularDeque{TaskIdType}(executorpath, 1024; create=true))
            push!(executors, executorpath)
        end

        # create and initialize the shared dict
        metastore = joinpath(runpath, "scheduler")
        create_metadata_ipc(metastore, deques)

        @everywhere MemPool.enable_who_has_read[] = false
        @everywhere Dagger.use_shared_array[] = false

        new(runpath, brokerpath, executors, metastore, executor_tasks, deques, pinger, Ref{Union{Task,Void}}(nothing), debug)
    end
end

function wait_for_executors(runenv::RunEnv)
    tasks = runenv.executor_tasks
    pinger = runenv.pinger
    while sum(map(isready, tasks)) < length(tasks)
        isready(pinger) && take!(pinger)
    end
    #map(x->info("executor stole $(x[1]), shared $(x[2]) and executed $(x[3])"), tasks)
    M = SchedulerNodeMetadata(runenv.metastore)
    reset(M)
    close(M)
    empty!(runenv.executor_tasks)
    nothing
end

function cleanup(runenv::RunEnv)
    if nothing !== runenv.pinger_task[]
        wait(runenv.pinger_task[])
        runenv.pinger_task[] = nothing
    end
    map(SharedDataStructures.delete!, runenv.deques)
    map(rm, runenv.executors)
    rm("/dev/shm/jsch$(getpid())"; recursive=true, force=true)
    #map((path)->rm(path; recursive=true, force=true), [runenv.metastore, runenv.broker])
    map(empty!, (runenv.executors, runenv.executor_tasks, runenv.deques))
    nothing
end

function rundag(runenv::RunEnv, dag::Thunk)
    dag, _elapsedtime, _bytes, _gctime, _memallocs = @timed begin
        dref_to_fref(dag)
    end
    #info("dag preparation time: $_elapsedtime")

    if nothing !== runenv.pinger_task[]
        wait(runenv.pinger_task[])
        runenv.pinger_task[] = nothing
    end

    metastore = runenv.metastore
    pinger = runenv.pinger
    broker = runenv.broker
    help_threshold = length(runenv.executors) - 1

    for idx in 1:length(runenv.executors)
        executorpath = runenv.executors[idx]
        #info("spawning executor $executorpath")
        executor_task = @spawnat (idx+1) runexecutor(broker, executorpath, dag, pinger; debug=runenv.debug, metastore=metastore, help_threshold=help_threshold)
        push!(runenv.executor_tasks, executor_task)
    end

    #info("spawning broker")
    res = runbroker(broker, dag, runenv.executors, pinger; debug=runenv.debug, metastore=metastore)
    runenv.pinger_task[] = @async wait_for_executors(runenv)
    res
end
