const opts = Base.JLOptions()
const inline_flag = opts.can_inline == 1 ? `` : `--inline=no`
const cov_flag = (opts.code_coverage == 1) ? `--code-coverage=user` :
                 (opts.code_coverage == 2) ? `--code-coverage=all` :
                 ``

function run_test(script)
    srvrscript = joinpath(dirname(@__FILE__), script)
    srvrcmd = `$(joinpath(JULIA_HOME, "julia")) $cov_flag $inline_flag $script`
    println("Running tests from ", script, "\n", "="^60)
    ret = run(srvrcmd)
    println("Finished ", script, "\n", "="^60)
    nothing
end

ENV["NODE_META_IMPL"] = "DagScheduler.ShmemMeta.ShmemExecutorMeta"
ENV["CLUSTER_META_IMPL"] = "DagScheduler.SimpleMeta.SimpleExecutorMeta"
println("Running with ShmemMeta and SimpleMeta")
println("===================================")
run(`ipcs -a`)
println("===================================")
run_test("runtests_master_only.jl")
run_test("runtests_1node.jl")
run_test("runtests_2node.jl")
println("===================================")
run(`ipcs -a`)

ENV["NODE_META_IMPL"] = "DagScheduler.ShmemMeta.ShmemExecutorMeta"
ENV["CLUSTER_META_IMPL"] = "DagScheduler.FdbMeta.FdbExecutorMeta"
println("Running with ShmemMeta and FdbMeta")
println("===================================")
run(`ipcs -a`)
println("===================================")
run_test("runtests_1node.jl")
run_test("runtests_2node.jl")
println("===================================")
run(`ipcs -a`)

ENV["NODE_META_IMPL"] = "DagScheduler.FdbMeta.FdbExecutorMeta"
ENV["CLUSTER_META_IMPL"] = "DagScheduler.FdbMeta.FdbExecutorMeta"
println("Running with FdbMeta")
println("===================================")
run_test("runtests_fdb_queue.jl")
run_test("runtests_fdb_dict.jl")
run_test("runtests_master_only.jl")
