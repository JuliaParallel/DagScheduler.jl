mutable struct ExecutionCtx
    id::UInt64                                  # component id
    brokerid::UInt64                            # broker id
    rootpath::String                            # root path identifying the run
    role::Symbol                                # :executor, :broker or :master
    meta::ExecutorMeta                          # shared metadata store
    reserved::Vector{TaskIdType}                # tasks reserved for this component
    stolen::Set{TaskIdType}                     # tasks that this component has stolen from others
    expanded::Set{TaskIdType}                   # tasks that have been expanded (not new tasks)
    nshared::Int                                # cumulative number of tasks shared
    nexecuted::Int                              # cumulative number of tasks executed
    nstolen::Int                                # cumulative number of tasks stolen
    dag_root::Union{Thunk,Void}                 # the root of dag being processed
    dependents::Dict{Thunk,Set{Thunk}}          # dependents of each node in the dag being processed
    reset_task::Union{Task,Void}                # async task to reset the scheduler env after a run
    taskidmap::Dict{TaskIdType,Thunk}           # for quick lookup
    costs::Union{Vector,Void}                   # optional task costs per broker
    debug::Bool                                 # switch on debug logging
    remotetrack::Bool                           # switch on remote tracking

    function ExecutionCtx(metastore_impl::String, rootpath::String, id::UInt64, brokerid::UInt64, role::Symbol, help_threshold::Int; costs=nothing, debug::Bool=false, remotetrack::Bool=false)
        broker_rootpath = joinpath(rootpath, string(brokerid))
        new(id, brokerid, rootpath, role,
            metastore(metastore_impl, broker_rootpath, help_threshold),
            Vector{TaskIdType}(),
            Set{TaskIdType}(),
            Set{TaskIdType}(),
            0, 0, 0, nothing, Dict{Thunk,Set{Thunk}}(),
            nothing, Dict{TaskIdType,Thunk}(), costs, debug, remotetrack)
    end
end

# enqueue can enqueue either to the reserved or shared section
# enqueuing to shared section is done under lock
function enqueue(env::ExecutionCtx, task::TaskIdType, isreserved::Bool, allow_dup::Bool=false)
    if isreserved
        enqueue(env.reserved, task)
    else
        env.nshared += 1
        share(env, task, allow_dup)
    end
end

const FLG_TASK_EXPANDED = TaskIdType(1 << 30) # we use the lower bits for task id and few higher bits for task state flags

function task_annotation(env::ExecutionCtx, task::TaskIdType, addmode::Bool)
    if task !== NoTask
        if addmode
            if task in env.expanded
                task |= FLG_TASK_EXPANDED
            end
        else
            if (task & FLG_TASK_EXPANDED) == FLG_TASK_EXPANDED
                task &= TaskIdType(~FLG_TASK_EXPANDED)
                push!(env.expanded, task)
            end
        end
    end
    task
end

function share(env::ExecutionCtx, task::TaskIdType, allow_dup::Bool=false)
    share_task(env.meta, task, allow_dup)
    task
end
function enqueue(reserved::Vector{TaskIdType}, task::TaskIdType)
    if task in reserved
        if task !== reserved[end]
            push!(reserved, splice!(reserved, findlast(reserved, task)))
        end
    else
        push!(reserved, task)
    end
    task
end

function dequeue(env::ExecutionCtx, task::TaskIdType)
    idx = findlast(env.reserved, task)
    (idx > 0) && splice!(env.reserved, idx)
    nothing
end

should_share(env::ExecutionCtx) = should_share(env.meta)
should_share(env::ExecutionCtx, nreserved::Int) = should_share(env.meta, nreserved)

function async_reset(env::ExecutionCtx)
    env.reset_task = @schedule reset(env)
    nothing
end

function reset(env::ExecutionCtx)
    reset(env.meta)
    empty!(env.reserved)
    empty!(env.stolen)
    empty!(env.expanded)
    empty!(env.dependents)
    empty!(env.taskidmap)
    env.nshared = 0
    env.nstolen = 0
    env.nexecuted = 0
    env.dag_root = nothing
    nothing
end

function init(env::ExecutionCtx, task::Thunk; result_callback=nothing)
    if env.reset_task !== nothing
        try
            wait(env.reset_task)
        end
        env.reset_task = nothing
    end
    env.dag_root = task
    Dagger.dependents(task, env.dependents)
    walk_dag(task, false) do x,d
        isa(x, Thunk) && (env.taskidmap[x.id] = x)
        nothing
    end

    init(env.meta, Int(env.brokerid);
        add_annotation=(id)->task_annotation(env, id, true),
        del_annotation=(id)->task_annotation(env, id, false),
        result_callback=result_callback)
    nothing
end

get_executable(env::ExecutionCtx, task::TaskIdType) = env.taskidmap[task]

keep(env::ExecutionCtx, executable::Thunk, depth::Int=1, isreserved::Bool=true) = keep(env, taskid(executable), depth, isreserved, executable)
function keep(env::ExecutionCtx, task::TaskIdType, depth::Int=1, isreserved::Bool=true, executable::Any=nothing)
    #tasklog(env, "in keep for task ", task, " depth ", depth)
    has_result(env.meta, task) && (return true)

    tasklog(env, "enqueue task ", task, " isreserved ", isreserved, " depth ", depth)
    enqueue(env, task, isreserved)
    depth -= 1
    if depth >=0
        (executable == nothing) && (executable = get_executable(env, task))
        if istask(executable) && !(task in env.expanded)
            # reserve at least one task
            reservedforself = false
            for input in inputs(executable)
                if istask(input)
                    #tasklog(env, "will keep dependency input for executable ", task)
                    nocrossdeps = (length(env.dependents[input]) < 2)
                    isthisreserved = (isreserved && nocrossdeps) ? (!reservedforself || !should_share(env)) : false
                    keep(env, input, depth, isthisreserved)
                    reservedforself = reservedforself || isthisreserved
                end
            end
            push!(env.expanded, task)
        end
    end
    false
end

#=
function stolen_task_input_watchlist(env::ExecutionCtx, task::TaskIdType)
    dependents = Vector{TaskIdType}()

    if task in env.expanded
        executable = get_executable(env, task)
        for input in inputs(executable)
            if istask(input)
                inptask = taskid(input)
                has_result(env.meta, inptask; recheck=true) || push!(dependents, inptask)
            end
        end
    end

    dependents
end
=#
function steal(env::ExecutionCtx, selector=default_task_selector)
    task = steal_task(env.meta, selector)
    if task !== NoTask
        push!(env.stolen, task)
        env.nstolen += 1
    end
    task
end

was_stolen(env::ExecutionCtx, task::TaskIdType) = task in env.stolen

inputs_available(env::ExecutionCtx, task::TaskIdType) = inputs_available(env, get_executable(env, task))
function inputs_available(env::ExecutionCtx, executable::Thunk)
    for inp in inputs(executable)
        if istask(inp)
            has_result(env.meta, taskid(inp)) || (return false)
        end
    end
    true
end

function runnable(env::ExecutionCtx, task::TaskIdType)
    has_result(env.meta, task) && return true
    t = get_executable(env, task)
    istask(t) ? inputs_available(env, t) : true
end

# select a task from the reserved queue and mark it as being executed
# returns NoTask if no runnable task is found
function reserve(env::ExecutionCtx)
    data = env.reserved
    #tasklog(env, "reserving from ", join(map(x->string(x.id), data), ", "))
    L = length(data)

    # find an unexpanded task
    restask = NoTask
    for idx in L:-1:1
        task = data[idx]
        if !(task in env.expanded)
            restask = task
            break
        end
    end

    # else find a runnable task
    if restask === NoTask
        for idx in L:-1:1
            task = data[idx]
            if runnable(env, task)
                restask = task
                break
            end
        end
        (restask === NoTask) || tasklog(env, "found a runnable task ", restask)
    else
        tasklog(env, "found an unexpanded task ", restask)
    end

    tasklog(env, "reserved ", (restask !== NoTask) ? string(restask) : "notask")
    restask
end

function release(env::ExecutionCtx, task::TaskIdType, complete::Bool)
    complete && dequeue(env, task)
end

function reserve_to_share(env::ExecutionCtx)
    data = env.reserved
    #tasklog(env, "reserving from ", join(map(x->string(x.id), data), ", "))
    L = length(data)

    task_to_move = NoTask
    # find an unexpanded task that's also not stolen
    for idx in L:-1:1
        task = data[idx]
        if !(task in env.expanded) && !(task in env.stolen)
            task_to_move = task
            break
        end
    end

    # else export first task that's not stolen
    if task_to_move === NoTask
        for idx in L:-1:1
            (data[idx] in env.stolen) && continue
            task_to_move = data[idx]
            t = get_executable(env, task_to_move)
            if istask(t)
                # export all inputs
                for inp in t.inputs
                    if istask(inp)
                        export_local_result(env.meta, taskid(inp), inp, UInt64(length(env.dependents[inp])))
                    end
                end
            end
            break
        end
    end

    if task_to_move !== NoTask
        dequeue(env, task_to_move)
        enqueue(env, task_to_move, false)
        tasklog(env, "moved ", string(task_to_move), " from reserved to shared, expanded: ", (task_to_move in env.expanded), " stolen: ", (task_to_move in env.stolen))
    end
    nothing
end

_collect(env::ExecutionCtx, x::Chunk, c::Bool) = c ? collect(x) : x
function _collect(env::ExecutionCtx, x::Thunk, c::Bool=true)
    res = get_result(env.meta, taskid(x))
    (isa(res, Chunk) && c) ? collect(res) : res
end
_collect(env::ExecutionCtx, x, _c::Bool) = x
function exec(env::ExecutionCtx, task::TaskIdType)
    has_result(env.meta, task) && (return true)

    # run the task
    t = get_executable(env, task)
    if istask(t)
        res = t.f(map(x->_collect(env,x,!t.meta), inputs(t))...)
    else
        res = t
    end

    if istask(t) && !t.get_result && !t.meta
        # dagger automatically sets persist and cache flags on dags it generated based on size
        res = Dagger.tochunk(res, persist = t.persist, cache = t.persist ? true : t.cache)
    end

    # export (if other processes need it) or keep in memory (for use in-process) the result
    # TODO: handle vector of chunks specially?
    if was_stolen(env, task)
        if isa(res, Vector{Chunk})
            res = chunktodisk(res)
        end
        if !isa(res, Chunk)
            res = Dagger.tochunk(res)
        end
        if isa(res, Chunk) && isa(res.handle, DRef)
            res = chunktodisk(res)
        end
        set_result(env.meta, task, res; refcount=UInt64(length(env.dependents[t])), processlocal=false)
    else
        set_result(env.meta, task, res)
    end

    #=
    # clean up task inputs, we don't need them anymore
    if istask(t)
        for (inp, ires) in zip(t.inputs, map(x->_collect(env,x,false), inputs(t)))
            (istask(inp) && isa(ires, Chunk) && !ires.persist) || continue
            refcount = (length(env.dependents[inp]) > 1) ? decr_result_ref(env.meta, taskid(inp)) : UInt64(0)
            (refcount == 0) && try pooldelete(ires.handle) end
        end
    end
    =#
    env.nexecuted += 1
    true
end
