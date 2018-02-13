const TaskIdType = UInt64

const NoTask = TaskIdType(0)
taskid(id::TaskIdType) = id
taskid(th::Thunk) = TaskIdType(th.id)
#taskid(ch::Chunk) = TaskIdType(hash(ch))
#taskid(executable) = TaskIdType(hash(executable))

function tasklog(env, msg...)
    env.debug && info(env.role, " : ", env.id, " : ", env.brokerid, " : ", msg...)
end

function taskexception(env, ex, bt)
    xret = CapturedException(ex, bt)
    tasklog(env, "exception ", xret)
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

function get_drefs(dag, bucket::Vector{Chunk}=Vector{Chunk}())
   if isa(dag, Chunk)
       if isa(dag.handle, DRef)
           push!(bucket, dag)
       end
   elseif isa(dag, Thunk)
       map(x->get_drefs(x, bucket), dag.inputs)
   end
   bucket
end

#=
get_frefs(dag) = map(chunktodisk, get_drefs(dag))
=#

chunktodisk(chunk) = Chunk(chunk.chunktype, chunk.domain, movetodisk(chunk.handle), chunk.persist)

function walk_dag(fn, dag_node, update::Bool, depth::Int=1)
    if isa(dag_node, Thunk)
        if update
            dag_node.inputs = map(x->walk_dag(fn, x, update, depth+1), dag_node.inputs)
        else
            map(x->walk_dag(fn, x, update, depth+1), dag_node.inputs)
        end
    end
    fn(dag_node, depth)
end

function filter!(fn, dag_node::Thunk)
    for inp in dag_node.inputs
        filter!(fn, inp)
    end
    dag_node.inputs = tuple(filter!(fn, collect(dag_node.inputs))...)
    dag_node
end

persist_chunks!(dag) = walk_dag(dag, true) do node,depth
    if isa(node, Chunk)
        node.persist = true
    end
    node
end

dref_to_fref(dag) = dref_to_fref!(deepcopy(dag))
dref_to_fref!(dag) = walk_dag(dag, true) do node,depth
    if isa(node, Chunk) && isa(node.handle, DRef)
        chunktodisk(node)
    else
        node
    end
end

execution_stages(dag_node) = execution_stages!(deepcopy(dag_node))
function execution_stages!(dag_node, deps=Dagger.dependents(dag_node), root=dag_node)
    newinps = Set{Thunk}()
    for inp in dag_node.inputs
        if istask(inp)
            for cinp in execution_stages!(inp, deps, root)
                push!(newinps, cinp)
            end
        end
    end
    dag_node.inputs = tuple(newinps...)
    ((dag_node == root) || (length(deps[dag_node]) > 1)) && (return [dag_node])
    [dag_node.inputs...]
end
