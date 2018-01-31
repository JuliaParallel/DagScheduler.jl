mutable struct NodeEnv
    brokerid::UInt64
    executorids::Vector{UInt64}
    executor_tasks::Vector{Future}
    last_task_stat::Vector{Tuple{UInt64,Future}}
    reset_task::Union{Task,Void}

    function NodeEnv(brokerid::Integer, executorids::Vector{Int})
        new(brokerid, executorids, Vector{Future}(), Vector{Tuple{UInt64,Future}}(), nothing)
    end
end

mutable struct RunEnv
    rootpath::String
    masterid::UInt64
    nodes::Vector{NodeEnv}
    reset_task::Union{Task,Void}
    debug::Bool

    function RunEnv(; rootpath::String="/dagscheduler", masterid::Int=myid(), executorids::Vector{Int}=workers(), debug::Bool=false)
        nexecutors = length(executorids)
        ((nexecutors > 1) || (nworkers() < nexecutors)) || error("need at least two workers")

        # ensure clean path
        delete_meta(rootpath, masterid)

        @everywhere MemPool.enable_who_has_read[] = false
        @everywhere Dagger.use_shared_array[] = false

        node = NodeEnv(masterid, executorids)

        new(rootpath, masterid, [node], nothing, debug)
    end
end

function wait_for_executors(runenv::RunEnv)
    @sync for node in runenv.nodes
        @async begin
            empty!(node.last_task_stat)
            append!(node.last_task_stat, zip(node.executorids, node.executor_tasks))
            empty!(node.executor_tasks)
            for (pid,task) in node.last_task_stat
                isready(task) || wait(task)
            end
        end
    end
    # TODO: execute delete_meta on brokers for broker env cleanup
    delete_meta(runenv.rootpath, runenv.masterid)
    nothing
end

function print_stats(runenv::RunEnv)
    for node in runenv.nodes
        info("broker $(node.brokerid)")
        map((x)->begin
            pid = x[1]
            result = fetch(x[2])
            if isa(result, Tuple)
                stole, shared, executed = fetch(x[2])
                info("executor $pid stole $stole, shared $shared, executed $executed tasks") 
            else
                info("executor $pid: $result")
            end
        end, node.last_task_stat)
    end
    nothing
end

function delete_meta(rootpath::String, brokerid::Integer)
    broker_rootpath = joinpath(rootpath, string(brokerid))
    M = metastore(NODE_META_IMPL, broker_rootpath, 0)
    M.brokerid = myid()
    delete!(M)
    nothing
end

function cleanup_meta(rootpath::String, brokerid::Integer)
    broker_rootpath = joinpath(rootpath, string(brokerid))
    M = metastore(NODE_META_IMPL, broker_rootpath, 0)
    M.brokerid = myid()
    cleanup(M)
    nothing
end

#------------------------------------------------------------------
# per process scheduler context
#------------------------------------------------------------------
const genv = Ref{Union{Sched,Void}}(nothing)
const upstream_genv = Ref{Union{Sched,Void}}(nothing)

#-------------------------------------------------------------------
# broker provides the initial tasks and coordinates among executors
# by stealing spare tasks from all peers and letting peers steal
#--------------------------------------------------------------------
function runbroker(rootpath::String, id::UInt64, brokerid::UInt64, root_t; upstream_brokerid::UInt64=0, debug::Bool=false, upstream_help_threshold::Int=typemax(Int))
    if genv[] === nothing
        env = genv[] = Sched(NODE_META_IMPL, rootpath, id, brokerid, :broker, typemax(Int); debug=debug)
    else
        env = (genv[])::Sched
    end
    if upstream_brokerid > 0
        if upstream_genv[] === nothing
            upenv = upstream_genv[] = Sched(CLUSTER_META_IMPL, rootpath, id, upstream_brokerid, :executor, upstream_help_threshold; debug=debug)
        else
            upenv = (upstream_genv[])::Sched
        end
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

#-------------------------------------------------------------------
# broker provides the initial tasks and coordinates among executors
# by stealing spare tasks from all peers and letting peers steal
#--------------------------------------------------------------------
function runmaster(rootpath::String, id::UInt64, brokerid::UInt64, root_t; debug::Bool=false)
    if genv[] === nothing
        env = genv[] = Sched(CLUSTER_META_IMPL, rootpath, id, brokerid, :broker, typemax(Int); debug=debug)
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
        env = genv[] = Sched(NODE_META_IMPL, rootpath, id, brokerid, :executor, help_threshold; debug=debug)
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
    delete_meta(runenv.rootpath, runenv.masterid)
    for node in runenv.nodes
        empty!(node.executor_tasks)
    end
    @everywhere DagScheduler.genv[] = nothing
    cleanup_meta(runenv.rootpath, runenv.masterid)
    # TODO: spawn remote tasks on brokers to cleanup meta on brokers
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

    for node in runenv.nodes
        help_threshold = length(node.executorids) - 1

        for idx in 1:length(node.executorids)
            executorid = node.executorids[idx]
            #info("spawning executor $executorid")
            executor_task = @spawnat executorid runexecutor(runenv.rootpath, executorid, runenv.masterid, dag; debug=runenv.debug, help_threshold=help_threshold)
            push!(node.executor_tasks, executor_task)
        end
    end

    #info("spawning master broker")
    res = runmaster(runenv.rootpath, UInt64(myid()), runenv.masterid, dag; debug=runenv.debug)

    runenv.reset_task = @schedule wait_for_executors(runenv)
    res
end
