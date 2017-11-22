#-------------------------------------------------------------------
# broker provides the initial tasks and coordinates among executors
# by stealing spare tasks from all peers and letting peers steal
#--------------------------------------------------------------------
function runbroker(broker_name::String, t, executors::Vector{String}; metastore::String="/dev/shm/scheduler", slowdown::Bool=false, debug::Bool=false)
    env = Sched(broker_name, metastore, typemax(Int); debug=debug)
    tasklog(env, "broker invoked")
    t = dref_to_fref(t)

    nstolen = 0
    peers = SchedPeer[]
    try
        task = root = taskid(t)
        keep(env, t, 0, false)
        tasklog(env, "broker started with $(task)")
        executable = get_executable(env.meta, task)
        tasklog(env, "broker verified $(task) as $executable")

        # connect to the executor deques
        for executor_name in executors
            push!(peers, SchedPeer(executor_name))
            tasklog(env, "broker connected to peer $executor_name")
        end
        stealbufsz = length(peers) - 1  # do not steal when there's only one peer!
        stealinterval = 0.3

        # steal tasks from executors and make them available to other executors
        while !has_result(env.meta, root)
            if !has_shared(env, stealbufsz)
                tasklog(env, "broker trying to steal tasks")
                for idx in 1:length(peers)
                    has_shared(env, stealbufsz) && break
                    peer_env = peers[idx]
                    task = steal(env, peer_env)
                    if task !== NoTask
                        keep(env, task, 0, false)
                        nstolen += 1
                        tasklog(env, "broker stole task $task from executor $(peer_env.id) ($(peer_env.name))")
                    end
                end
            end
            yield()
            sleep(stealinterval)
        end

        #tasklog(env, "broker stole $nstolen tasks")
        info("broker stole $nstolen tasks")
        res = get_result(env.meta, root)
        isa(res, Chunk) ? collect(res) : res
    catch ex
        taskexception(env, ex, catch_backtrace())
    end
end

function runexecutor(broker_name::String, executor_name::String, root_t; metastore::String="/dev/shm/scheduler", slowdown::Bool=false, debug::Bool=false, help_threshold::Int=typemax(Int))
    env = Sched(executor_name, metastore, help_threshold; debug=debug)
    tasklog(env, "executor starting")
    broker = SchedPeer(broker_name)
    nstolen = 0
    nexecuted = 0

    try
        root = taskid(root_t)
        task = NoTask
        lasttask = task

        while !has_result(env.meta, root)
            slowdown && sleep(0.3)
            tasklog(env, "executor trying to do tasks, remaining $(length(env.reserved)), shared $(length(env.shared))")
            task = reserve(env)

            # if no tasks in own queue, steal tasks from broker
            if task === NoTask || task === lasttask
                tasklog(env, "got notask or lasttask, will steal")
                task = steal(env, broker)
                if task === NoTask
                    tasklog(env, "stole notask")
                    ## if no runnable tasks, update results of stolen tasks
                    #for stolentask in filter(x->isstolen(executor_env,x,executor_id), executor_env.stack.data)
                    #    release(executor_env, stolentask, iscomplete(executor_env.results, stolentask))
                    #end
                else
                    tasklog(env, "keeping task $task")
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
            yield()
        end
        #tasklog(env, "executor stole $nstolen and completed $nexecuted")
        info("executor stole $nstolen and completed $nexecuted")
    catch ex
        taskexception(env, ex, catch_backtrace())
    end
    tasklog(env, "executor done")
end

function rundag(dag; nexecutors::Int=nworkers(), slowdown::Bool=false, debug::Bool=false)
    executor_tasks = Future[]
    executors = String[]
    deques = SharedCircularDeque{TaskIdType}[]

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
            executor_task = @spawnat (idx+1) runexecutor(brokerpath, executorpath, dag; slowdown=slowdown, debug=debug, metastore=metastore, help_threshold=1)
            push!(executor_tasks, executor_task)
        end

        info("spawning broker")
        runbroker(brokerpath, dag, executors; slowdown=slowdown, debug=debug, metastore=metastore)
    finally
        map(SharedDataStructures.delete!, deques)
        map(rm, executors)
        map((path)->rm(path; recursive=true), [metastore, brokerpath])
    end
end
