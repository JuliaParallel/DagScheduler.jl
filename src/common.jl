#------------------------------------
# Common Types
#------------------------------------
const TaskIdType = UInt64

const NoTask = TaskIdType(0)
taskid(id::TaskIdType) = id
taskid(th::Thunk) = TaskIdType(th.id)
#taskid(ch::Chunk) = TaskIdType(hash(ch))
#taskid(executable) = TaskIdType(hash(executable))

mutable struct NodeEnv
    brokerid::UInt64
    broker_task::Union{Future,Void}
    executorids::Vector{UInt64}
    executor_tasks::Vector{Future}
    last_task_stat::Vector{Tuple{UInt64,Future}}

    function NodeEnv(brokerid::Integer, executorids::Vector{Int})
        new(brokerid, nothing, executorids, Vector{Future}(), Vector{Tuple{UInt64,Future}}())
    end
end

mutable struct RunEnv
    rootpath::String
    masterid::UInt64
    nodes::Vector{NodeEnv}
    reset_task::Union{Task,Void}
    debug::Bool

    function RunEnv(; rootpath::String="/dagscheduler", masterid::Int=myid(), nodes::Vector{NodeEnv}=[NodeEnv(masterid,workers())], debug::Bool=false)
        nexecutors = 0
        for node in nodes
            nw = length(node.executorids)
            (nw > 1) || error("need at least two workers on each node")
            nexecutors += nw
        end
        (nexecutors > 1) || error("need at least two workers")

        # ensure clean paths
        delete_meta(UInt64(masterid), nodes, rootpath)

        @everywhere MemPool.enable_who_has_read[] = false
        @everywhere Dagger.use_shared_array[] = false

        new(rootpath, masterid, nodes, nothing, debug)
    end
end

struct ComputeCost
    cpu::Float64        # cost of computing
    net::Float64        # cost of transfering data
end

fval(c::ComputeCost) = (c.cpu + c.net)

function +(c1::ComputeCost, c2::ComputeCost)
    ComputeCost(c1.cpu + c2.cpu, c1.net + c2.net)
end
+(c1::ComputeCost) = c1
isless(c1::ComputeCost, c2::ComputeCost) = fval(c1) < fval(c2)
/(c1::ComputeCost, c2::ComputeCost) = fval(c1) / fval(c2)

# here is where the network cost can be adjusted to account for the computation that this Thunk does
# if it is a reduction step, apply the reduction before summing the input costs
cost_function(t::Thunk) = sum

# consider splitting node for scheduling only if there is no clear association with any node
const SPLIT_COST_RATIO = 1.5
# consider splitting node for scheduling only if the compute cost is substantial
const SPLIT_CAPACITY_RATIO = 0.5

mean_node_capacity(runenv::RunEnv) = mean([length(node.executorids)*1 for node in runenv.nodes])

#------------------------------------
# Logging utilities
#------------------------------------
function tasklog(env, msg...)
    env.debug && info(env.role, " : ", env.id, " : ", env.brokerid, " : ", msg...)
end

function taskexception(env, ex, bt)
    xret = CapturedException(ex, bt)
    tasklog(env, "exception ", xret)
    @show xret
    xret
end

#------------------------------------
# DAG Utilities
#------------------------------------
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

function walk_dag(fn, dag_node, update::Bool, stop_nodes=TaskIdType[], depth::Int=1)
    if isa(dag_node, Thunk) && !(taskid(dag_node) in stop_nodes)
        if update
            dag_node.inputs = map(x->walk_dag(fn, x, update, stop_nodes, depth+1), dag_node.inputs)
        else
            map(x->walk_dag(fn, x, update, stop_nodes, depth+1), dag_node.inputs)
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

function depth(dag_node)
    let maxd = 1
        f = (n,d)->begin
            maxd = max(maxd, d)
        end
        walk_dag(f, dag_node, false)
    end
end

function find_node(dag_node, id::TaskIdType)
    let node = nothing
        f = (n,d)->begin
            if istask(n) && (taskid(n) === id)
                node = n
            end
            node
        end
        walk_dag(f, dag_node, false)
    end
end

ischild(dag_node, id::TaskIdType) = (nothing !== find_node(dag_node, id))

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
