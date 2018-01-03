mutable struct RunEnv
    rootpath::String
    brokerid::UInt64
    executorids::Vector{UInt64}
    executor_tasks::Vector{Future}
    last_task_stat::Vector{Tuple{UInt64,Future}}
    reset_task::Union{Task,Void}
    debug::Bool

    function RunEnv(; rootpath::String="/dagscheduler", brokerid::Int=myid(), executorids::Vector{Int}=workers(), debug::Bool=false)
        nexecutors = length(executorids)
        ((nexecutors > 1) || (nworkers() < nexecutors)) || error("need at least two workers")

        # ensure clean path
        delete!(EtcdSchedMeta(rootpath, 0))

        @everywhere MemPool.enable_who_has_read[] = false
        @everywhere Dagger.use_shared_array[] = false

        new(rootpath, brokerid, executorids, Vector{Future}(), Vector{Tuple{UInt64,Future}}(), nothing, debug)
    end
end

function wait_for_executors(runenv::RunEnv)
    empty!(runenv.last_task_stat)
    append!(runenv.last_task_stat, zip(runenv.executorids, runenv.executor_tasks))
    empty!(runenv.executor_tasks)
    for (pid,task) in runenv.last_task_stat
        isready(task) || wait(task)
    end
    delete!(EtcdSchedMeta(runenv.rootpath, 0))
    nothing
end

function print_stats(runenv::RunEnv)
    map((x)->begin
        pid = x[1]
        result = fetch(x[2])
        if isa(result, Tuple)
            stole, shared, executed = fetch(x[2])
            info("executor $pid stole $stole, shared $shared, executed $executed tasks") 
        else
            info("executor $pid: $result")
        end
    end, runenv.last_task_stat)
    nothing
end

#------------------------------------------------------------------
# per process scheduler context
#------------------------------------------------------------------
const genv = Ref{Union{Sched,Void}}(nothing)

#-------------------------------------------------------------------
# broker provides the initial tasks and coordinates among executors
# by stealing spare tasks from all peers and letting peers steal
#--------------------------------------------------------------------
function runbroker(rootpath::String, id::UInt64, brokerid::UInt64, root_t; debug::Bool=false)
    if genv[] === nothing
        env = genv[] = Sched(rootpath, id, brokerid, :broker, typemax(Int); debug=debug)
    else
        env = (genv[])::Sched
    end
    env.debug = debug
    init(env, root_t)
    tasklog(env, "invoked")

    try
        task = root = taskid(root_t)
        keep(env, root_t, 0, false)
        tasklog(env, "started with ", task)

        while !has_result(env.meta, root)
            wait_trigger(env.meta)
        end

        tasklog(env, "stole ", env.nstolen, " shared ", env.nshared, " tasks")
        #info("broker stole $(env.nstolen), shared $(env.nshared) tasks")
        return get_result(env.meta, root)
    catch ex
        taskexception(env, ex, catch_backtrace())
        rethrow(ex)
    finally
        async_reset(env)
    end
end

function runexecutor(rootpath::String, id::UInt64, brokerid::UInt64, root_t; debug::Bool=false, help_threshold::Int=typemax(Int))
    if genv[] === nothing
        env = genv[] = Sched(rootpath, id, brokerid, :executor, help_threshold; debug=debug)
    else
        env = (genv[])::Sched
    end
    env.debug = debug
    tasklog(env, "starting")
    init(env, root_t)

    try
        root = taskid(root_t)
        task = NoTask

        while !has_result(env.meta, root)
            tasklog(env, "trying to do tasks, remaining ", length(env.reserved))

            # check if we should share some of our reserved tasks out
            if should_share(env, length(env.reserved))
                reserve_to_share(env)
            end

            task = reserve(env)

            # if no tasks in own queue, steal tasks from peers
            if task === NoTask
                tasklog(env, "got notask or lasttask, will steal")
                task = steal(env)

                if task !== NoTask
                    keep(env, task, 0, true)
                    tasklog(env, "stole ", task, ", remaining ", length(env.reserved))
                end
            end

            if task !== NoTask
                # execute task
                if !(complete = has_result(env.meta, task))
                    complete = keep(env, task, 1, true)
                    if !complete
                        if runnable(env, task)
                            complete = exec(env, task)
                        end
                    end
                end
                release(env, task, complete)
                tasklog(env, task, " is complete: ", complete, ", remaining ", length(env.reserved))
            else
                wait_trigger(env.meta)
            end
        end
        tasklog(env, "stole ", env.nstolen, ", shared ", env.nshared, ", completed ", env.nexecuted, " tasks")
    catch ex
        taskexception(env, ex, catch_backtrace())
        rethrow(ex)
    finally
        async_reset(env)
    end
    #tasklog(env, "done")
    (env.nstolen, env.nshared, env.nexecuted)
end

function cleanup(runenv::RunEnv)
    if nothing !== runenv.reset_task
        wait(runenv.reset_task)
        runenv.reset_task = nothing
    end
    delete!(EtcdSchedMeta(runenv.rootpath, 0))
    empty!(runenv.executor_tasks)
    @everywhere DagScheduler.genv[] = nothing
    nothing
end

function rundag(runenv::RunEnv, dag::Thunk)
    dag, _elapsedtime, _bytes, _gctime, _memallocs = @timed begin
        dref_to_fref(dag)
    end
    #info("dag preparation time: $_elapsedtime")

    if nothing !== runenv.reset_task
        wait(runenv.reset_task)
        runenv.reset_task = nothing
    end

    help_threshold = length(runenv.executorids) - 1

    for idx in 1:length(runenv.executorids)
        executorid = runenv.executorids[idx]
        #info("spawning executor $executorid")
        executor_task = @spawnat executorid runexecutor(runenv.rootpath, executorid, runenv.brokerid, dag; debug=runenv.debug, help_threshold=help_threshold)
        push!(runenv.executor_tasks, executor_task)
    end

    #info("spawning broker")
    res = runbroker(runenv.rootpath, UInt64(myid()), runenv.brokerid, dag; debug=runenv.debug)
    runenv.reset_task = @schedule wait_for_executors(runenv)
    res
end
