struct Sched
    id::UInt64                                  # component id
    name::String                                # component name
    role::Symbol                                # :executor or :broker
    pinger::RemoteChannel                       # signalling channel between peers
    meta::SchedulerNodeMetadata                 # node level shared metadata store
    reserved::Vector{TaskIdType}                # tasks reserved for this component
    shared::SharedCircularDeque{TaskIdType}     # tasks put out by this component for sharing
    stolen::Set{TaskIdType}                     # tasks that this component has stolen from others
    expanded::Set{TaskIdType}                   # tasks that have been expanded (not new tasks)
    help_threshold::Int                         # threshold for putting out tasks for sharing
    nshared::Ref{Int}                           # cumulative number of tasks shared
    debug::Bool                                 # switch on debug logging

    function Sched(name::String, role::Symbol, pinger::RemoteChannel, metastore::String, help_threshold::Int; share_limit::Int=1024, debug::Bool=false)
        new(hash(name), name, role, pinger,
            SchedulerNodeMetadata(metastore), Vector{TaskIdType}(), SharedCircularDeque{TaskIdType}(name, share_limit; create=false),
            Set{TaskIdType}(), Set{TaskIdType}(),
            help_threshold, Ref(0), debug)
    end
end

struct SchedPeer
    id::UInt64                                  # peer componet id
    name::String                                # peer name
    shared::SharedCircularDeque{TaskIdType}     # tasks put out by peer for sharing

    function SchedPeer(name::String; share_limit::Int=1024)
        new(hash(name), name, SharedCircularDeque{TaskIdType}(name, share_limit; create=false))
    end
end

# enqueue can enqueue either to the reserved or shared section
# enqueuing to shared section is done under lock
function enqueue(stack::Sched, task::TaskIdType, isreserved::Bool)
    if isreserved
        enqueue(stack.reserved, task)
    else
        stack.nshared[] += 1
        enqueue(stack.shared, task)
    end
end
function enqueue(shared::SharedCircularDeque{TaskIdType}, task::TaskIdType)
    withlock(shared.lck) do
        (task in shared) || push!(shared, task)
    end
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

function dequeue(stack::Sched, task::TaskIdType)
    idx = findlast(stack.reserved, task)
    (idx > 0) && splice!(stack.reserved, idx)
    nothing
end

function should_share(stack::Sched)
    withlock(stack.shared.lck) do
        return (length(stack.shared) < stack.help_threshold)
    end
end

has_shared(stack::Union{Sched,SchedPeer}) = !isempty(stack.shared)

function has_shared(stack::Union{Sched,SchedPeer}, howmuch::Int)
    withlock(stack.shared.lck) do
        length(stack.shared) >= howmuch
    end
end

function keep(env::Sched, executable::Any, depth::Int=1, isreserved::Bool=true)
    task = taskid(executable)
    if isreserved
        set_executable(env.meta, task, executable)
    else
        cond_set_executable(env.meta, task, executable, (existing)->false, false)
    end
    keep(env, task, depth, isreserved, executable)
end

function keep(env::Sched, task::TaskIdType, depth::Int=1, isreserved::Bool=true, executable::Any=nothing)
    #tasklog(env, "in keep for task $(task) depth $depth")
    has_result(env.meta, task) && (return true)

    canreserve = isreserved || cond_set_executor(env.meta, task, env.id, (existing)->((existing == 0) || (existing == env.id)), false)

    if canreserve
        #tasklog(env, "enqueue task $(task) depth $depth")
        enqueue(env, task, isreserved)
        !isreserved && (env.role === :executor) && ping(env)
        depth -= 1
        if depth >=0
            (executable == nothing) && (executable = get_executable(env.meta, task))
            if istask(executable) && !(task in env.expanded)
                # reserve at least one task
                reservedforself = false
                for input in inputs(executable)
                    if istask(input)
                        #tasklog(env, "will keep dependency input for executable $(task)")
                        keep(env, input, depth, isreserved ? (!reservedforself || !should_share(env)) : false)
                        reservedforself = true
                    end
                end
                push!(env.expanded, task)
            end
        end
    #else
    #    tasklog(env, "not enqueing as task is owned by other executor $depth")
    end
    false
end

function steal(env::Sched, from::SchedPeer)
    withlock(from.shared.lck) do
        has_shared(from) || (return NoTask)
        task = shift!(from.shared)
        cond_set_executor(env.meta, task, env.id, (existing)->((existing == 0) || (existing == from.id)), false)
        push!(env.stolen, task)
        return task
    end
end

was_stolen(env::Sched, task::TaskIdType) = task in env.stolen

inputs_available(env::Sched, task::TaskIdType) = inputs_available(env, get_executable(env.meta, task))
function inputs_available(env::Sched, executable::Thunk)
    for inp in inputs(executable)
        if istask(inp)
            has_result(env.meta, taskid(inp)) || (return false)
        end
    end
    true
end

function runnable(env::Sched, task::TaskIdType)
    has_result(env.meta, task) && return true
    t = get_executable(env.meta, task)
    istask(t) ? inputs_available(env, t) : true
end

# select a task from the reserved queue and mark it as being executed
# returns NoTask if no runnable task is found
function reserve(env::Sched)
    data = env.reserved
    #tasklog(env, "reserving from ", join(map(x->string(x.id), data), ", "))
    L = length(data)

    restask = NoTask

    # find a runnable task
    for idx in L:-1:1
        task = data[idx]
        if runnable(env, task)
            restask = task
            break
        end
    end

    # else get the top task
    (restask === NoTask) && (L > 0) && (restask = data[L])

    # mark the executor
    #(restask !== NoTask) && cond_set_executor(env.meta, restask, env.id, (existing)->((existing == 0) || (existing == env.id)), false)
    tasklog(env, (restask !== NoTask) ? "reserved $(restask)" : "reserved notask")
    restask
end

function release(env::Sched, task::TaskIdType, complete::Bool)
    if complete
        dequeue(env, task)
    #else
    #    # if task is suspended, dequeue and put it up for stealing
    end
end

_collect(env, x::Chunk) = collect(x)
function _collect(env, x::Union{Thunk,Function})
    res = get_result(env.meta, taskid(x))
    isa(res, Chunk) ? collect(res) : res
end
_collect(env, x) = x
function exec(env::Sched, task::TaskIdType)
    has_result(env.meta, task) && (return true)

    # run the task
    t = get_executable(env.meta, task)
    if istask(t)
        res = t.f(map(x->_collect(env,x), inputs(t))...)
    elseif isa(t, Function)
        res = t()
    else
        res = t
    end

    # collect the result
    if isa(res, Chunk)
        res = collect(res)
    end
    if isa(res, SharedArray)
        res = convert(Array, res)
    end

    # export (if other processes need it) or keep in memory (for use in-process) the result
    if was_stolen(env, task)
        if !isa(res, Chunk) && istask(t) && !t.get_result
            # TODO: maybe should do this only if the size is beyond a certain threshold
            res = Dagger.tochunk(res)
        end
        if isa(res, Chunk) && isa(res.handle, DRef)
            res = chunktodisk(res)
        end
        export_result(env.meta, task, res)
        del_executable(env.meta, task)
    else
        set_result(env.meta, task, res)
        _procdel(env.meta, NodeMetaKey(task,M_EXECUTABLE))
    end

    # clean up task inputs, we don't need them anymore
    if istask(t)
        for inp in t.inputs
            if isa(inp, Chunk)
                pooldelete(inp.handle)
            #elseif istask(inp)
            #    _procdel(env.meta, NodeMetaKey(task,M_RESULT))
            end
        end
    end

    true
end

