struct Sched
    id::UInt64
    name::String
    meta::SchedulerNodeMetadata
    reserved::Vector{TaskIdType}
    shared::SharedCircularDeque{TaskIdType}
    help_threshold::Int
    debug::Bool

    function Sched(name::String, metastore::String, help_threshold::Int; share_limit::Int=1024, debug::Bool=false)
        new(hash(name), name, SchedulerNodeMetadata(metastore), Vector{TaskIdType}(), SharedCircularDeque{TaskIdType}(name, share_limit; create=false), help_threshold, debug)
    end
end

struct SchedPeer
    id::UInt64
    name::String
    shared::SharedCircularDeque{TaskIdType}
    function SchedPeer(name::String; share_limit::Int=1024)
        new(hash(name), name, SharedCircularDeque{TaskIdType}(name, share_limit; create=false))
    end
end

# enqueue can enqueue either to the reserved or shared section
# enqueuing to shared section is done under lock
enqueue(stack::Sched, task::TaskIdType, isreserved::Bool) = enqueue(isreserved ? stack.reserved : stack.shared, task)
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
    cond_set_executable(env.meta, task, executable, (existing)->false, false)
    keep(env, task, depth, isreserved, executable)
end

function keep(env::Sched, task::TaskIdType, depth::Int=1, isreserved::Bool=true, executable::Any=nothing)
    #tasklog(env, "in keep for task $(task) depth $depth")
    has_result(env.meta, task) && (return true)

    if cond_set_executor(env.meta, task, env.id, (existing)->((existing == 0) || (existing == env.id)), false)
        #tasklog(env, "enqueue task $(task) depth $depth")
        enqueue(env, task, isreserved)
        depth -= 1
        if depth >=0
            (executable == nothing) && (executable = get_executable(env.meta, task))
            if istask(executable)
                for input in inputs(executable)
                    #tasklog(env, "will keep dependency input for executable $(task)")
                    keep(env, input, depth, isreserved && !should_share(env))
                end
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
        return task
    end
end

inputs_available(env::Sched, task::TaskIdType) = inputs_available(env, get_executable(env.meta, task))
function inputs_available(env::Sched, executable::Thunk)
    for inp in inputs(executable)
        has_result(env.meta, taskid(inp)) || (return false)
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
    (restask !== NoTask) && cond_set_executor(env.meta, restask, env.id, (existing)->((existing == 0) || (existing == env.id)), false)
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
_collect(env, x::Union{Thunk,Function}) = get_result(env.meta, taskid(x))
_collect(env, x) = x
function exec(env::Sched, task::TaskIdType)
    has_result(env.meta, task) && (return true)

    t = get_executable(env.meta, task)
    if istask(t)
        #res = t.f(map(x->_collect(result(env.results,x)), inputs(t))...)
        #inps = map(x->_collect(env,x), inputs(t))
        #try
        #res = t.f(inps...)
        #(res == nothing) && error("got nothing output")
        #catch ex
        #    info("I had: $inps")
        #    info("For: $(inputs(t))")
        #    info("To function $t")
        #    rethrow(ex)
        #end
        res = t.f(map(x->_collect(env,x), inputs(t))...)
    elseif isa(t, Function)
        res = t()
    else
        res = t
    end
    if isa(res, Chunk)
        res = collect(res)
    end
    if isa(res, SharedArray)
        res = convert(Array, res)
    end
    set_result(env.meta, task, res)
    true
end

