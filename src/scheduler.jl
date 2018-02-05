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
