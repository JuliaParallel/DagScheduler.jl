module DefaultScheduler

import ..DagScheduler: Dagger, RunEnv, NodeEnv, Thunk, Chunk, DRef, FileRef, ComputeCost, TaskIdType, istask,
        cost_function, taskid, walk_dag, fval, SPLIT_CAPACITY_RATIO, SPLIT_COST_RATIO,
        find_node

#-------------------------------------
# Execution stages based on dependency
#-------------------------------------
function execution_stages(dag_node, deps=Dagger.dependents(dag_node), root=dag_node)
    newinps = Set{Thunk}()
    for inp in dag_node.inputs
        if istask(inp)
            for cinp in execution_stages(inp, deps, root)
                push!(newinps, cinp)
            end
        end
    end
    ((dag_node == root) || (length(deps[dag_node]) > 1)) ? [copy_thunk(dag_node, newinps)] : [newinps...]
end

#----------------------------------------------
# Execution stages based on costs and affinity
#----------------------------------------------
cost_for_nodes(runenv::RunEnv, root_t::Thunk) = [node.brokerid => cost_for_node(node, root_t) for node in runenv.nodes]
function cost_for_node(node::NodeEnv, root_t::Thunk)
    nc = Dict{TaskIdType,ComputeCost}()
    compute_cost(root_t, [node.brokerid, node.executorids...], node.host, nc)
    nc
end

function extend_stages_by_affinity(runenv::RunEnv, stages::Vector{Thunk}, root_t::Thunk, costs::Vector{Pair{UInt64,Dict{TaskIdType,ComputeCost}}}, nodecap::Float64)
    for stage in stages
        #add_stages_by_compute_cost(stage, root_t, costs, nodecap)

        walk_dag(stage, false) do n,d
            add_stages_by_compute_cost(n, root_t, costs, nodecap)
        end
    end
    nothing
end

compute_cost(dag_node, processorids::Vector{UInt64}, host::IPAddr, costs::Dict{TaskIdType,ComputeCost}) = ComputeCost(0.0, 0.0)
function compute_cost(dag_node::Thunk, processorids::Vector{UInt64}, host::IPAddr, costs::Dict{TaskIdType,ComputeCost})
    inps = dag_node.inputs
    costs[taskid(dag_node)] = isempty(inps) ? ComputeCost(1.0, 0.0) : cost_function(dag_node)(map((inp)->compute_cost(inp, processorids, host, costs), inps))
end
function compute_cost(dag_node::Chunk, processorids::Vector{UInt64}, host::IPAddr, costs::Dict{TaskIdType,ComputeCost})
    # network transfer cost (arbitrarily set as 1 per 1MB - needs to be parameterized)
    netcost = 0.0

    if (isa(dag_node.handle, DRef) && !(dag_node.handle.owner in processorids)) || (isa(dag_node.handle, FileRef) && !(dag_node.handle.host == host))
        netcost = ceil(Int, dag_node.handle.size/10^6)
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
            staged_n = copy_thunk(n, nchld)
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

copy_thunk(dag_node, newinps) = Dagger.Thunk(dag_node.f, newinps...;
            id = dag_node.id,
            get_result = dag_node.get_result,
            meta = dag_node.meta,
            persist = dag_node.persist,
            cache = dag_node.cache,
            cache_ref = dag_node.cache_ref,
            affinity = dag_node.affinity)

end # module DefaultScheduler

function schedule(runenv::RunEnv, root_t::Thunk)
    execstages = DefaultScheduler.execution_stages(root_t)

    if length(runenv.nodes) > 1
        costs = DefaultScheduler.cost_for_nodes(runenv, root_t)
        nodecap = mean_node_capacity(runenv)
        DefaultScheduler.extend_stages_by_affinity(runenv, execstages, root_t, costs, nodecap)
    else
        costs = []
    end

    execstages, costs
end

function affinity(tid::TaskIdType, brokerid::UInt64, costs::Vector{Pair{UInt64,Dict{TaskIdType,ComputeCost}}})
    tc, cc = DefaultScheduler.schedule_target(tid, costs)

    if !isempty(cc)
        for (b,c) in cc
            (b === brokerid) && (return c)
        end
    end

    1.0
end
