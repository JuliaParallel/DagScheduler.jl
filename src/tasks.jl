ping(env::Sched, event=nothing) = isready(env.pinger) || put!(env.pinger, event)

function do_broking(env::Sched, root)
    tasklog(env, "broker waiting for ping")
    pinger = env.pinger
    take!(pinger)
    while isready(pinger)
        take!(pinger)
    end
    !has_result(env.meta, root)
end

function steal_from_peers(env::Sched, peers::Vector{SchedPeer})
    stealbufsz = length(peers) - 1  # do not steal when there's only one peer!
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
                tasklog(env, "broker stole task $task from executor $(peer_env.id) ($(peer_env.name))")
            end
        end
    end
    nstolen
end

#-------------------------------------------------------------------
# broker provides the initial tasks and coordinates among executors
# by stealing spare tasks from all peers and letting peers steal
#--------------------------------------------------------------------
function runbroker(broker_name::String, t, executors::Vector{String}, pinger::RemoteChannel; metastore::String="/dev/shm/scheduler", debug::Bool=false)
    env = Sched(broker_name, :broker, pinger, metastore, typemax(Int); debug=debug)
    tasklog(env, "broker invoked")
    t, _elapsedtime, _bytes, _gctime, _memallocs = @timed begin
        @everywhere MemPool.enable_who_has_read[] = false
        @everywhere Dagger.use_shared_array[] = false
        dref_to_fref(t)
    end
    info("broker preparation time: $_elapsedtime")


    nstolen = 0
    peers = SchedPeer[]
    try
        task = root = taskid(t)
        keep(env, t, 0, false)
        tasklog(env, "broker started with $(task)")

        # connect to the executor deques
        for executor_name in executors
            push!(peers, SchedPeer(executor_name))
            tasklog(env, "broker connected to peer $executor_name")
        end

        # steal tasks from executors and make them available to other executors
        while do_broking(env, root)
            nstolen += steal_from_peers(env, peers)
        end

        #tasklog(env, "broker stole $nstolen tasks")
        info("broker stole $nstolen, shared $(env.nshared[]) tasks")
        res = get_result(env.meta, root)
        return isa(res, Chunk) ? collect(res) : res
    catch ex
        taskexception(env, ex, catch_backtrace())
        rethrow(ex)
    #finally
    #    @everywhere MemPool.cleanup()
    end
end

function runexecutor(broker_name::String, executor_name::String, root_t, pinger::RemoteChannel; metastore::String="/dev/shm/scheduler", debug::Bool=false, help_threshold::Int=typemax(Int))
    env = Sched(executor_name, :executor, pinger, metastore, help_threshold; debug=debug)
    tasklog(env, "executor starting")
    broker = SchedPeer(broker_name)
    nstolen = 0
    nexecuted = 0

    try
        root = taskid(root_t)
        task = NoTask
        lasttask = task

        while !has_result(env.meta, root)
            tasklog(env, "executor trying to do tasks, remaining $(length(env.reserved)), shared $(length(env.shared))")
            task = reserve(env)

            # if no tasks in own queue, steal tasks from broker
            if task === NoTask || task === lasttask
                tasklog(env, "got notask or lasttask, will steal")
                task = steal(env, broker)
                ping(env)
                if task === NoTask
                    tasklog(env, "stole notask")
                    sleep(0.2)  # do not overwhelm the broker
                else
                    keep(env, task, 0, true)
                    nstolen += 1
                    tasklog(env, "executor stole $(task), remaining $(length(env.reserved))")
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
                release(env, task, complete)
                tasklog(env, "executor $(task) is complete: $complete, remaining $(length(env.reserved))")
            end
            lasttask = task
        end
        ping(env)
        #tasklog(env, "executor $(env.name) stole $nstolen, shared $(env.nshared[]), completed $nexecuted tasks")
    catch ex
        taskexception(env, ex, catch_backtrace())
        rethrow(ex)
    end
    #tasklog(env, "executor done")
    (nstolen, env.nshared[], nexecuted)
end

function rundag(dag; nexecutors::Int=nworkers(), debug::Bool=false)
    executor_tasks = Future[]
    executors = String[]
    deques = SharedCircularDeque{TaskIdType}[]
    pinger = RemoteChannel()

    runpath = "/dev/shm"
    # create and initialize the shared dict
    metastore = joinpath(runpath, "scheduler")
    isdir(metastore) && rm(metastore; recursive=true)
    mkpath(metastore)

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

    try
        for idx in 1:length(executors)
            executorpath = executors[idx]
            info("spawning executor $executorpath")
            executor_task = @spawnat (idx+1) runexecutor(brokerpath, executorpath, dag, pinger; debug=debug, metastore=metastore, help_threshold=(nexecutors-1))
            push!(executor_tasks, executor_task)
        end

        info("spawning broker")
        res = runbroker(brokerpath, dag, executors, pinger; debug=debug, metastore=metastore)
        while sum(map(isready, executor_tasks)) < length(executor_tasks)
            isready(pinger) && take!(pinger)
        end
        map(x->info("executor stole $(x[1]), shared $(x[2]) and executed $(x[3])"), executor_tasks)
        res
    finally
        map(SharedDataStructures.delete!, deques)
        map(rm, executors)
        map((path)->rm(path; recursive=true), [metastore, brokerpath])
    end
end
