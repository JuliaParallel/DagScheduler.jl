addprocs(5)

include("../../daggen.jl")
using DagScheduler
using BenchmarkTools

isdir(".mempool") && rm(".mempool"; recursive=true)
@everywhere begin
    DagScheduler.META_IMPL[:node] = "DagScheduler.ShmemMeta.ShmemExecutorMeta"
    DagScheduler.META_IMPL[:cluster] = "DagScheduler.ShmemMeta.ShmemExecutorMeta"
end

node1 = NodeEnv(1, getipaddr(), [2,3,4,5,6])
runenv = RunEnv(; nodes=[node1])

const L = 6^4
const dag2 = gen_straight_dag(ones(Int, L));

result = collect(rundag(runenv, dag2));
#@time result = rundag(runenv, dag2);
@btime collect(rundag(runenv, dag2))

cleanup(runenv)
