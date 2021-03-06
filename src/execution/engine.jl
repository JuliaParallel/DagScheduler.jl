#------------------------------------------------------------------
# helper methods for the executor
#------------------------------------------------------------------
@timetrack function wait_for_executors(runenv::RunEnv)
    @sync for node in runenv.nodes
        @async begin
            empty!(node.last_task_stat)
            if node.broker_task !== nothing
                push!(node.last_task_stat, (node.brokerid, node.broker_task))
            end
            append!(node.last_task_stat, zip(node.executorids, node.executor_tasks))
            empty!(node.executor_tasks)
            node.broker_task = nothing
            for (pid,task) in node.last_task_stat
                isready(task) || wait(task)
            end
        end
    end

    delete_meta(runenv.masterid, runenv.nodes, runenv.rootpath)
    remotetrack_end(runenv.remotetrack)
    profile_end(runenv.profile, "master_$(runenv.masterid)")
    nothing
end

function print_stats(runenv::RunEnv)
    info("execution stats:")
    for node in runenv.nodes
        map((x)->begin
            pid = x[1]
            result = fetch(x[2])
            role = (pid === node.brokerid) ? "broker" : "executor"
            if isa(result, Tuple)
                if role == "executor"
                    stole, shared, executed = fetch(x[2])
                    info("  executor $pid stole $stole, shared $shared, executed $executed tasks")
                else
                    stole, shared = fetch(x[2])
                    info("broker $pid stole $stole, shared $shared tasks")
                end
            else
                info("$role $pid: $result")
            end
        end, node.last_task_stat)
    end
    nothing
end

function handle_failure(runenv::RunEnv, env::ExecutionCtx, pid, cause)
    meta = env.meta
    isa(cause, ProcessExitedException) && (cause = :killed)
    info("PID $pid failed with $cause")

    for node in runenv.nodes
        executors = node.executorids
        if (pid == node.brokerid) || (pid in executors)
            # remove node from runenv
            filter!((x)->(x!==node), runenv.nodes)
            tasks_to_reschedule = detach(meta, node.brokerid)
            info("detached brokerid ", node.brokerid, " with ", length(tasks_to_reschedule), " tasks to reschedule")
            cleanupnode = ((cause === :killed) && (pid === node.brokerid)) ? first(executors) : node.brokerid

            # identify a cleanup node and terminate the others
            nodestoterminate = convert(Vector{Int}, copy(executors))
            push!(nodestoterminate, node.brokerid)
            (cause === :killed) && filter!((x)->x!=pid, nodestoterminate)
            filter!((x)->x!=cleanupnode, nodestoterminate)
            info("terminating nodes ", nodestoterminate)
            for tpid in nodestoterminate
                try
                    rmprocs(tpid)
                catch ex
                    warn("error $ex terminating $tpid")
                end
            end

            # cleanup and terminate the last (cleanup) node
            info("cleaning up and terminating last node ", cleanupnode)
            try
                remotecall_wait(cleanup_meta, cleanupnode, META_IMPL[:node], runenv.rootpath, node.brokerid)
                rmprocs(Int(cleanupnode))
            catch ex
                warn("error $ex terminating $cleanupnode")
            end
            isempty(runenv.nodes) && error("PID $pid failed with $cause")

            # reschedule tasks being executed by the terminated node
            for (tid,annotatedtid) in tasks_to_reschedule
                info("rescheduling task $tid annotated as $annotatedtid")
                enqueue(env, annotatedtid, false, true)
            end
            break
        end
    end

    nothing
end

function check_failures(runenv::RunEnv, env::ExecutionCtx)
    nfailures = 0

    # check for process failure
    procsnow = sort!(procs())
    if hash(procsnow) !== runenv.nodehash
        usedprocs = [runenv.masterid]
        for node in runenv.nodes
            push!(usedprocs, node.brokerid)
            append!(usedprocs, node.executorids)
        end
        deadprocs = setdiff(usedprocs, procsnow)

        for pid in deadprocs
            handle_failure(runenv, env, pid, :killed)
            nfailures += 1
        end
    end

    @sync for node in runenv.nodes
        @async begin
            tasklist = []
            if node.broker_task !== nothing
                push!(tasklist, (node.brokerid, node.broker_task))
            end
            append!(tasklist, zip(node.executorids, node.executor_tasks))
            for (pid,task) in tasklist
                try
                    if isready(task)
                        result = fetch(task)
                        if !isa(result, Tuple)
                            handle_failure(runenv, env, pid, result)
                            nfailures += 1
                        end
                    end
                catch ex
                    handle_failure(runenv, env, pid, ex)
                    nfailures += 1
                end
            end
        end
    end
    nfailures
end

function delete_meta(masterid::UInt64, nodes::Vector{NodeEnv}, rootpath::String)
    @sync begin
        @async delete_meta(META_IMPL[:cluster], rootpath, masterid)
        for node in nodes
            if node.brokerid !== masterid
                @async remotecall_wait(delete_meta, node.brokerid, META_IMPL[:node], rootpath, node.brokerid)
            end
        end
    end
    nothing
end

function delete_meta(metaimpl::String, rootpath::String, brokerid::Integer)
    broker_rootpath = joinpath(rootpath, string(brokerid))
    M = metastore(metaimpl, broker_rootpath, 0)
    M.brokerid = myid()
    delete!(M)
    nothing
end

function cleanup_meta(masterid::UInt64, nodes::Vector{NodeEnv}, rootpath::String)
    @sync begin
        @async cleanup_meta(META_IMPL[:cluster], rootpath, masterid)
        for node in nodes
            if node.brokerid !== masterid
                @async remotecall_wait(cleanup_meta, node.brokerid, META_IMPL[:node], rootpath, node.brokerid)
            end
        end
    end
    nothing
end

function cleanup_meta(metaimpl::String, rootpath::String, brokerid::Integer)
    broker_rootpath = joinpath(rootpath, string(brokerid))
    M = metastore(metaimpl, broker_rootpath, 0)
    M.brokerid = brokerid
    DagScheduler.cleanup(M)
    nothing
end

#------------------------------------------------------------------
# per process execution engine context
#------------------------------------------------------------------
const genv = Ref{Union{ExecutionCtx,Void}}(nothing)
const upstream_genv = Ref{Union{ExecutionCtx,Void}}(nothing)
const join_count = Ref(0)
const start_cond = Channel{Void}(1)

#------------------------------------------------------------------
# helper methods for the broker
#------------------------------------------------------------------
function join_cluster()
    join_count[] -= 1
    if join_count[] == 0
        put!(start_cond, nothing)
    end
end

function upstream_result_to_local(env, task, res)
    try
        if !has_result(env.meta, task)
            t = get_executable(env, task)
            set_result(env.meta, task, res; processlocal=false)
        end
    catch ex
        taskexception(env, ex, catch_backtrace())
        rethrow(ex)
    end
    nothing
end

function local_result_to_upstream(upenv, task, res)
    try
        if ((task === taskid(upenv.dag_root)) || was_stolen(upenv, task)) && !has_result(upenv.meta, task)
            t = get_executable(upenv, task)
            set_result(upenv.meta, task, res; processlocal=false)
        end
    catch ex
        taskexception(env, ex, catch_backtrace())
        rethrow(ex)
    end
    nothing
end

function unify_trigger(env::ExecutionCtx, root::TaskIdType, unified_trigger::Channel{Void}, do_trigger::Ref{Bool})
    while !has_result(env.meta, root)
        wait_trigger(env.meta)
        isready(unified_trigger) || put!(unified_trigger, nothing)
    end
    do_trigger[] = false
    put!(unified_trigger, nothing)
    nothing
end

function broker_task_selector(tasks::Vector{UInt64}, brokerid::UInt64, rev::Bool)
    env = (genv[])::ExecutionCtx
    costs = env.costs
    del_annotation = env.meta.del_annotation
    affs = map((tid)->affinity(del_annotation(tid), brokerid, costs), tasks)
    _val, idx = rev ? findmin(affs) : findmax(affs)
    idx
end

function broker_tasks(upenv::ExecutionCtx, env::ExecutionCtx, unified_trigger::Channel{Void}, do_trigger::Ref{Bool})
    try
        costs = env.costs
        hascosts = (costs !== nothing) && !isempty(costs)
        brokerid = env.brokerid
        broker_from_upstream_task_selector = (tasks) -> broker_task_selector(tasks, brokerid, false)  # get task with highest affinity for the broker
        broker_from_downstream_task_selector = (tasks) -> broker_task_selector(tasks, brokerid, true) # get task with least affinity for the broker
        last_upstream = last_downstream = 0.0
        
        while do_trigger[]
            #tasklog(env, "doing broker tasks")
            tnow = time()

            upstat, downstat = upenv.meta.sharemode, env.meta.sharemode
            needed_upstream, needed_downstream = should_share(upenv), should_share(env)
            have_upstream = (upstat.ncreated > 0) ? (upstat.nshared > 0) : true
            have_downstream = (downstat.ncreated > 0) ? (downstat.nshared > 0) : true
            from_upstream = needed_downstream && have_upstream && ((tnow - last_downstream) > 0.5)
            from_downstream = needed_upstream && !needed_downstream && have_downstream && ((tnow - last_upstream) > 0.5)

            tasklog(env, "up,down | shared(", upstat.nshared, ",", downstat.nshared, ") created(", upstat.ncreated, ",", downstat.ncreated, ") del(", upstat.ndeleted, ",", downstat.ndeleted, ") share(", needed_upstream, ",", needed_downstream, ")")

            if from_upstream
                tasklog(env, "bring new upstream tasks into local node")
                # bring new upstream tasks into local node
                task = hascosts ? steal(upenv, broker_from_upstream_task_selector) : steal(upenv)
                tasklog(env, "stole ", (task === NoTask) ? "NoTask" : string(task))
                if task !== NoTask
                    enqueue(env, task, false, true)
                    from_downstream = false
                    last_upstream = tnow
                end
            end

            if from_downstream
                tasklog(env, "export shared node tasks to upstream brokers")
                # export shared node tasks to upstream brokers
                task = hascosts ? steal(env, broker_from_downstream_task_selector) : steal(env)
                tasklog(env, "stole ", (task === NoTask) ? "NoTask" : string(task))
                if task !== NoTask
                    enqueue(upenv, task, false, true)
                    last_downstream = tnow
                end
            end

            tasklog(env, "waiting on trigger")
            # wait for trigger
            take!(unified_trigger)
            while isready(unified_trigger)
                take!(unified_trigger)
            end
        end
    catch ex
        close(unified_trigger)
        taskexception(env, ex, catch_backtrace())
        rethrow(ex)
    end
    nothing
end

#-------------------------------------------------------------------
# broker provides the initial tasks and coordinates among executors
# by stealing spare tasks from all peers and letting peers steal
#--------------------------------------------------------------------
function runbroker(rootpath::String, id::UInt64, upstream_brokerid::UInt64, root_t::Thunk, costs::Vector;
        debug::Bool=false, remotetrack::Bool=false, profile::Bool=false, upstream_help_threshold::Int=typemax(Int), downstream_help_threshold::Int=typemax(Int))
    profile_init(profile, "broker_$(id)")
    remotetrack_init(remotetrack)
    if genv[] === nothing
        env = genv[] = ExecutionCtx(META_IMPL[:node], rootpath, id, id, :broker, downstream_help_threshold; debug=debug)
    else
        env = (genv[])::ExecutionCtx
    end

    if upstream_genv[] === nothing
        upenv = upstream_genv[] = ExecutionCtx(META_IMPL[:cluster], rootpath, id, upstream_brokerid, :executor, upstream_help_threshold; debug=debug)
    else
        upenv = (upstream_genv[])::ExecutionCtx
    end

    env.debug = debug
    env.remotetrack = remotetrack
    upenv.debug = debug
    upenv.remotetrack = remotetrack
    init(env, root_t; result_callback=(task,res)->local_result_to_upstream(upenv,task,res))
    init(upenv, root_t; result_callback=(task,res)->upstream_result_to_local(env,task,res))
    env.costs = upenv.costs = costs
    remotecall_wait(join_cluster, Int(upstream_brokerid))
    tasklog(env, "invoked")

    try
        root = taskid(root_t)
        unified_trigger = Channel{Void}(10)
        do_trigger = Ref(true)

        @sync begin
            # monitor the global and node queues
            @async unify_trigger(upenv, root, unified_trigger, do_trigger)
            @async unify_trigger(env, root, unified_trigger, do_trigger)

            # broker tasks between this node and other nodes
            @async broker_tasks(upenv, env, unified_trigger, do_trigger)
        end

        tasklog(env, "stole ", env.nstolen, " shared ", env.nshared, " tasks")
        #info("broker stole $(env.nstolen), shared $(env.nshared) tasks")

        return (env.nstolen, env.nshared)
    catch ex
        taskexception(env, ex, catch_backtrace())
        rethrow(ex)
    finally
        remotetrack_end(remotetrack)
        profile_end(profile, "broker_$(id)")
        reset(env)
        reset(upenv)
    end
end

#-------------------------------------------------------------------
# broker provides the initial tasks and coordinates among executors
# by stealing spare tasks from all peers and letting peers steal
#--------------------------------------------------------------------
function master_schedule(env, execstages, scheduled, completed)
    tasklog(env, "scheduling ", length(execstages), " execstages entries")
    # filter out fully scheduled stages
    isempty(execstages) || filter!((task)->!(taskid(task) in scheduled), execstages)

    if !isempty(execstages)
        schedulable_depth = Vector{Int}()
        schedulable_tids = Vector{TaskIdType}()
        for task in execstages
            # filter out child stages that are completed
            filter!(x->(isa(x, Thunk) && !(taskid(x) in completed)), task)
            # pick child stages that have no inputs or all inputs ready (indicated by the same condition)
            walk_dag(task, false) do x,d
                if isa(x, Thunk)
                    tid = taskid(x)
                    if !(tid in scheduled) && isempty(x.inputs)
                        push!(schedulable_depth, d)
                        push!(schedulable_tids, tid)
                    end
                end
                nothing
            end
        end
        tasklog(env, "found ", length(schedulable_tids), " schedulable tasks")
        # schedule for execution, in decreasing order of node depth
        sp = sortperm(schedulable_depth; rev=true)
        for idx in 1:length(sp)
            tid = schedulable_tids[sp[idx]]
            if !(tid in scheduled)
                keep(env, tid, 0, false)
                push!(scheduled, tid)
            end
        end
    end
    nothing
end

function runmaster(runenv::RunEnv, root_t::Thunk, execstages, costs::Vector; debug::Bool=false, remotetrack::Bool=false)
    rootpath = runenv.rootpath
    id = UInt64(myid())
    brokerid = runenv.masterid
    if genv[] === nothing
        env = genv[] = ExecutionCtx(META_IMPL[:cluster], rootpath, id, brokerid, :master, typemax(Int); debug=debug)
    else
        env = (genv[])::ExecutionCtx
    end
    env.debug = debug
    env.remotetrack = remotetrack
    env.costs = costs

    completed = Vector{TaskIdType}()
    scheduled = Vector{TaskIdType}()

    init(env, root_t; result_callback=(task,res)->push!(completed,task))
    tasklog(env, "invoked")

    remotecall_wait(join_cluster, Int(id))
    started = false
    @schedule while !started && !isready(start_cond)
        check_failures(runenv, env)
        sleep(0.01)
    end
    take!(start_cond)
    started = true
    tasklog(env, "all brokers and executors joined cluster")

    try
        root = taskid(root_t)
        tasklog(env, "started with ", root)

        last_ncompleted = 0
        while !has_result(env.meta, root)
            # while critical path items exist, process them first
            if isempty(scheduled) || (length(completed) > last_ncompleted)
                last_ncompleted = length(completed)
                master_schedule(env, execstages, scheduled, completed)
            end
            wait_trigger(env.meta)
            check_failures(runenv, env)
        end

        tasklog(env, "stole ", env.nstolen, " shared ", env.nshared, " tasks")
        #info("broker stole $(env.nstolen), shared $(env.nshared) tasks")
        return get_result(env.meta, root)
    catch ex
        taskexception(env, ex, catch_backtrace())
        rethrow(ex)
    finally
        reset(env)
    end
end

function runexecutor(rootpath::String, id::UInt64, brokerid::UInt64, masterid::UInt64, root_t::Thunk;
        debug::Bool=false, remotetrack::Bool=false, profile::Bool=false, help_threshold::Int=typemax(Int))
    profile_init(profile, "executor_$(brokerid)_$(id)")
    remotetrack_init(remotetrack)
    if genv[] === nothing
        env = genv[] = ExecutionCtx(META_IMPL[:node], rootpath, id, brokerid, :executor, help_threshold; debug=debug)
    else
        env = (genv[])::ExecutionCtx
    end
    env.debug = debug
    env.remotetrack = remotetrack
    tasklog(env, "starting")
    init(env, root_t)
    remotecall_wait(join_cluster, Int(masterid))

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
        return (env.nstolen, env.nshared, env.nexecuted)
    catch ex
        taskexception(env, ex, catch_backtrace())
        rethrow(ex)
    finally
        remotetrack_end(remotetrack)
        profile_end(profile, "executor_$(brokerid)_$(id)")
        reset(env)
    end
end

function cleanup(runenv::RunEnv)
    if nothing !== runenv.reset_task
        wait(runenv.reset_task)
        runenv.reset_task = nothing
    end
    delete_meta(runenv.masterid, runenv.nodes, runenv.rootpath)
    for node in runenv.nodes
        empty!(node.executor_tasks)
    end
    @everywhere DagScheduler.genv[] = nothing
    @everywhere DagScheduler.upstream_genv[] = nothing
    cleanup_meta(runenv.masterid, runenv.nodes, runenv.rootpath)
    nothing
end

function rundag(runenv::RunEnv, dag::Thunk)
    profile_init(runenv.profile, "master_$(runenv.masterid)")
    remotetrack_init(runenv.remotetrack)
    #=
    dag, _elapsedtime, _bytes, _gctime, _memallocs = @timed begin
        dref_to_fref(dag)
    end
    =#
    #info("dag preparation time: $_elapsedtime")

    # determine critical path
    execstages, costs = schedule(runenv, dag)

    if nothing !== runenv.reset_task
        wait(runenv.reset_task)
        runenv.reset_task = nothing
    end

    upstream_help_threshold = length(runenv.nodes) - 1
    executor_help_threshold = sum(length(node.executorids) for node in runenv.nodes)
    join_count[] = 1 # start with 1 for the master
    while isready(start_cond)
        take!(start_cond)
    end

    for node in runenv.nodes
        downstream_help_threshold = length(node.executorids) - 1

        for idx in 1:length(node.executorids)
            executorid = node.executorids[idx]
            #info("spawning executor $executorid")
            join_count[] += 1
            executor_task = @spawnat executorid runexecutor(runenv.rootpath, executorid, node.brokerid, runenv.masterid, dag; debug=runenv.debug, remotetrack=runenv.remotetrack, profile=runenv.profile, help_threshold=executor_help_threshold)
            push!(node.executor_tasks, executor_task)
        end

        if node.brokerid !== runenv.masterid
            #info("spawning broker $(node.brokerid)")
            join_count[] += 1
            node.broker_task = @spawnat node.brokerid runbroker(runenv.rootpath, node.brokerid, runenv.masterid, dag, costs;
                debug=runenv.debug, remotetrack=runenv.remotetrack, profile=runenv.profile,
                upstream_help_threshold=upstream_help_threshold,
                downstream_help_threshold=downstream_help_threshold)
        end
    end

    #info("spawning master broker")
    res = runmaster(runenv, dag, execstages, costs; debug=runenv.debug, remotetrack=runenv.remotetrack)

    runenv.reset_task = @schedule wait_for_executors(runenv)
    res
end
