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
    host::IPAddr
    broker_task::Union{Future,Void}
    executorids::Vector{UInt64}
    executor_tasks::Vector{Future}
    last_task_stat::Vector{Tuple{UInt64,Future}}

    function NodeEnv(brokerid::Integer, host::IPAddr, executorids::Vector{Int})
        new(brokerid, host, nothing, executorids, Vector{Future}(), Vector{Tuple{UInt64,Future}}())
    end
end

mutable struct RunEnv
    rootpath::String
    masterid::UInt64
    nodes::Vector{NodeEnv}
    reset_task::Union{Task,Void}
    debug::Bool
    profile::Bool
    remotetrack::Bool

    function RunEnv(; rootpath::String="/D", masterid::Int=myid(), nodes::Vector{NodeEnv}=setup_nodes(masterid), debug::Bool=false, profile::Bool=false, remotetrack::Bool=false)
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

        new(rootpath, masterid, nodes, nothing, debug, profile, remotetrack)
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
    env.debug && info(now(), " ", env.role, " : ", env.id, " : ", env.brokerid, " : ", msg...)
    env.remotetrack && logmsg(Symbol("t"*string(object_id(current_task()))), now(), " ", env.role, " : ", env.id, " : ", env.brokerid, " : ", msg...)
    nothing
end

function taskexception(env, ex, bt)
    xret = CapturedException(ex, bt)
    tasklog(env, "exception ", xret)
    @show xret
    xret
end

function profile_init(prof::Bool, myid::String)
    if prof
        Profile.init(10^7, 0.01)
        Profile.start_timer()
    end
    nothing
end

function profile_end(prof::Bool, myid::String)
    if prof
        Profile.stop_timer()
        open("/tmp/$(myid).profile", "w") do f
            Profile.print(IOContext(f, :displaysize => (256, 1024)))
        end
        Profile.clear()
    end
    nothing
end

function remotetrack_init(remotetrack::Bool)
    remotetrack && start_sender()
    nothing
end

function remotetrack_end(remotetrack::Bool)
    remotetrack && stop_sender()
    nothing
end

#------------------------------------
# Topology utilities
#------------------------------------
function worker_distribution()
    d = Dict{IPv4,Vector{Int}}()
    for w in Base.Distributed.PGRP.workers
        wip = IPv4(isa(w, Base.Distributed.Worker) ? get(w.config.bind_addr) : w.bind_addr)
        if wip in keys(d)
            push!(d[wip], w.id)
        else
            d[wip] = [w.id]
        end
    end

    # disambiguate localhost
    loc = ip"127.0.0.1"
    if loc in keys(d)
        realip = remotecall_fetch(getipaddr, first(d[loc]))
        if realip in keys(d)
            append!(d[realip], d[loc])
        else
            d[realip] = d[loc]
        end
        delete!(d, loc)
    end
    d
end

function setup_nodes(master=1)
    wd = worker_distribution()
    frefservers = Dict{IPv4,Vector{Int}}()

    if length(wd) == 1
        # only one node. make master the broker and use shm meta_store
        @everywhere begin
            DagScheduler.META_IMPL[:node] = "DagScheduler.ShmemMeta.ShmemExecutorMeta"
            DagScheduler.META_IMPL[:cluster] = "DagScheduler.ShmemMeta.ShmemExecutorMeta"
        end
        executors = first(values(wd))
        splice!(executors, findfirst(executors, master))
        nodes = [NodeEnv(master, first(keys(wd)), executors)]
        frefservers[first(keys(wd))] = [master]
    else
        # multiple nodes. select one broker on each node (minimum pid on each group is the broker)
        @everywhere begin
            DagScheduler.META_IMPL[:node] = "DagScheduler.ShmemMeta.ShmemExecutorMeta"
            DagScheduler.META_IMPL[:cluster] = "DagScheduler.SimpleMeta.SimpleExecutorMeta"
        end
        nodes = NodeEnv[]
        for (ip,wrkrs) in wd
            sort!(filter!(w->w!=master, wrkrs))
            if !isempty(wrkrs)
                brokerid = shift!(wrkrs)
                push!(nodes, NodeEnv(brokerid, ip, wrkrs))
                frefservers[ip] = [brokerid]
            else
                frefservers[ip] = [master]
            end
        end
    end

    @sync for w in procs()
        @async remotecall_wait(w) do
            MemPool.enable_random_fref_serve[] = false
            empty!(MemPool.wrkrips)
            merge!(MemPool.wrkrips, frefservers)
        end
    end

    nodes
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

chunktodisk(chunks::Vector{Chunk}) = map(chunktodisk, chunks)
chunktodisk(chunk::Chunk) = isa(chunk.handle, DRef) ? Chunk(chunk.chunktype, chunk.domain, movetodisk(chunk.handle), chunk.persist) : chunk

function walk_dag(fn, dag_node, update::Bool, stop_nodes=TaskIdType[], depth::Int=1)
    if isa(dag_node, Thunk) && !(taskid(dag_node) in stop_nodes)
        if update
            dag_node.inputs = map(x->walk_dag(fn, x, update, stop_nodes, depth+1), dag_node.inputs)
        else
            for inp in dag_node.inputs
                walk_dag(fn, inp, update, stop_nodes, depth+1)
            end
        end
    end
    fn(dag_node, depth)
end

function filter!(fn, dag_node::Thunk)
    for inp in dag_node.inputs
        isa(inp, Thunk) && filter!(fn, inp)
    end
    newinputs = filter!(fn, collect(dag_node.inputs))
    if length(newinputs) < length(dag_node.inputs)
        dag_node.inputs = tuple(newinputs...)
    end
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
find_node(id::TaskIdType) = Dagger._thunk_dict[id]

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
