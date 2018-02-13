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

# Execution stages based on dependency
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

cost_for_nodes(runenv::RunEnv, root_t::Thunk) = [node.brokerid => cost_for_node(node, root_t) for node in runenv.nodes]
function cost_for_node(node::NodeEnv, root_t::Thunk)
    nc = Dict{TaskIdType,ComputeCost}()
    compute_cost(root_t, [node.brokerid, node.executorids...], nc)
    nc
end

function extend_stages_by_affinity(runenv::RunEnv, stages::Vector{Thunk}, root_t::Thunk)
    if length(runenv.nodes) > 1
        costs = cost_for_nodes(runenv, root_t)
        nodecap = mean_node_capacity(runenv)

        for stage in stages
            #add_stages_by_compute_cost(stage, root_t, costs, nodecap)

            walk_dag(stage, false) do n,d
                add_stages_by_compute_cost(n, root_t, costs, nodecap)
            end
        end
    end
    nothing
end

compute_cost(dag_node, processorids::Vector{UInt64}, costs::Dict{TaskIdType,ComputeCost}) = ComputeCost(0.0, 0.0)
function compute_cost(dag_node::Thunk, processorids::Vector{UInt64}, costs::Dict{TaskIdType,ComputeCost})
    inps = dag_node.inputs
    costs[DagScheduler.taskid(dag_node)] = isempty(inps) ? ComputeCost(1.0, 0.0) : cost_function(dag_node)(map((inp)->compute_cost(inp, processorids, costs), inps))
end
function compute_cost(dag_node::Chunk, processorids::Vector{UInt64}, costs::Dict{TaskIdType,ComputeCost})
    # network transfer cost (arbitrarily set as 1 per 1MB - needs to be parameterized)
    netcost = 0.0

    if isa(dag_node.handle, DRef) && !(dag_node.handle.owner in processorids)
        netcost = ceil(Int, dag_node.handle.size/10^6)
    # elseif isa(dag_node.handle, FRef) # TODO: once FRef encodes location
    end

    # cpu cost arbitrarily set to 1 for all functions - needs to be provided in configuration/function annotation
    ComputeCost(1.0, netcost)
end

function schedule_target(id::TaskIdType, costs::Vector{Pair{UInt64,Dict{TaskIdType,ComputeCost}}})
    target = UInt64(0)
    target_cost = ComputeCost(0.0, 0.0)
    for (brokerid,brokercosts) in costs
        brokercost = get(brokercosts, id, target_cost)
        if (target === UInt64(0)) || (brokercost < target_cost)
            target = brokerid
            target_cost = brokercost
        end
    end

    cost_comparison = Vector{Pair{UInt64,Float64}}()
    total_costs = Vector{Pair{UInt64,Float64}}()
    for (brokerid,brokercosts) in costs
        push!(total_costs, brokerid => fval(get(brokercosts, id, ComputeCost(0.0, 0.0))))
        push!(cost_comparison, brokerid => (get(brokercosts, id, target_cost) / target_cost))
    end
    sort!(total_costs, lt=(x,y)->x[2]<y[2])
    sort!(cost_comparison, lt=(x,y)->x[2]<y[2])

    total_costs, cost_comparison
end

function optimum_affinity(tid::TaskIdType, costs::Vector{Pair{UInt64,Dict{TaskIdType,ComputeCost}}}, avg_node_capacity::Float64)
    total_costs, cost_comparison = schedule_target(tid, costs)
    insignificant_cost(total_costs, avg_node_capacity) || strong_affinity(cost_comparison)
end

function strong_affinity(cost_comparison)
    (length(cost_comparison) < 2) ||                                    # affinity with just one node
    (cost_comparison[2][2] >= SPLIT_COST_RATIO)                         # cost on one node substantially less than others
end

function insignificant_cost(total_costs, avg_node_capacity)
    (total_costs[1][2] <= (SPLIT_CAPACITY_RATIO * avg_node_capacity))   # total cost is not significant enough to split
end

function add_stages_by_compute_cost(staged_node::Thunk, full_dag::Thunk, costs::Vector{Pair{UInt64,Dict{TaskIdType,ComputeCost}}}, avg_node_capacity::Float64)
    sn_tc, sn_cc = schedule_target(taskid(staged_node), costs)
    (insignificant_cost(sn_tc, avg_node_capacity) || strong_affinity(sn_cc)) && return

    # if no clear affinity, search till we find a node with one
    stop_nodes = Vector{TaskIdType}()
    for inp in staged_node.inputs
        istask(inp) && push!(stop_nodes, taskid(inp))
    end

    let chld=Set{Thunk}(), start_node=find_node(full_dag, taskid(staged_node)), deps=Dagger.dependents(full_dag)
        walk_dag(start_node, false, stop_nodes) do n,d
            istask(n) || return

            total_costs, cost_comparison = schedule_target(taskid(n), costs)
            ((total_costs[1][2]/sn_tc[1][2] <= 0.5) ||  insignificant_cost(total_costs, avg_node_capacity) || !strong_affinity(cost_comparison)) && return

            # find children of this node if there are any
            nchld = filter(c->ischild(n, taskid(c)), chld)
            setdiff!(chld, nchld)

            # keep children only if substantial cost difference or multi-dependency node
            filter!(nchld) do c
                (length(deps[c]) > 1) && (return true)
                c_tc, c_cc = schedule_target(taskid(c), costs)
                total_costs[1][2]/c_tc[1][2] <= 0.5
            end

            # create a new staged node (with its children) and insert
            staged_n = deepcopy(n)
            staged_n.inputs = tuple(nchld...)
            push!(chld, staged_n)
        end

        # insert new split points at staged node
        if !isempty(chld)
            #println("found nodes!! $(chld)")
            for inp in staged_node.inputs
                push!(chld, inp)
            end
            staged_node.inputs = tuple(chld...)
        end
    end
end
