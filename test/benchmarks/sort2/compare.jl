function runcompare()
    println("Running sort comparison with distributed results")
    println("Node Meta: ", ENV["NODE_META_IMPL"])
    println("Cluster Meta: ", ENV["CLUSTER_META_IMPL"])
    J = joinpath(JULIA_HOME, "julia")
    D = dirname(@__FILE__)
    F = map(x->joinpath(D,x), ("dagger.jl", "dagscheduler.jl", "dagscheduler_1node.jl", "dagscheduler_2node.jl"))

    for f in F
        command = `$J $f`
        println("- $(basename(f))")
        run(command)
    end
end

ENV["NODE_META_IMPL"] = "DagScheduler.ShmemMeta.ShmemExecutorMeta"
ENV["CLUSTER_META_IMPL"] = "DagScheduler.SimpleMeta.SimpleExecutorMeta"
runcompare()

ENV["NODE_META_IMPL"] = "DagScheduler.ShmemMeta.ShmemExecutorMeta"
ENV["CLUSTER_META_IMPL"] = "DagScheduler.FdbMeta.FdbExecutorMeta"
runcompare()
