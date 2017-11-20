const TaskIdType = UInt64

const NoTask = TaskIdType(0)
taskid(id::TaskIdType) = id
taskid(th::Thunk) = TaskIdType(th.id)
taskid(ch::Chunk) = TaskIdType(hash(collect(ch)))
taskid(executable) = TaskIdType(hash(executable))

function tasklog(env, msg...)
    env.debug && info(env.name, " : ", env.id, " : ", msg...)
end

function taskexception(env, ex, bt)
    xret = CapturedException(ex, bt)
    tasklog(env, "exception $xret")
    @show xret
    xret
end

function collect_chunks(dag)
    if isa(dag, Chunk)
        return collect(dag)
    elseif isa(dag, Thunk)
       dag.inputs = map(collect_chunks, dag.inputs)
       return dag
    else
       return dag
    end
end
