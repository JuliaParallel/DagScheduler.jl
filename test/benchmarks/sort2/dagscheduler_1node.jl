addprocs(5)

include("../../daggen.jl")
using DagScheduler
using BenchmarkTools

isdir(".mempool") && rm(".mempool"; recursive=true)
@everywhere begin
    DagScheduler.META_IMPL[:node] = "DagScheduler.ShmemMeta.ShmemExecutorMeta"
    DagScheduler.META_IMPL[:cluster] = "DagScheduler.SimpleMeta.SimpleExecutorMeta"
end

node1 = NodeEnv(2, getipaddr(), [3,4,5,6])
runenv = RunEnv(; nodes=[node1])

const L = 10^6
const dag2 = DagScheduler.dref_to_fref!(DagScheduler.persist_chunks!(gen_sort_dag(L, 40, 4, 40)));

result = collect(rundag(runenv, dag2));
#@time result = rundag(runenv, dag2);
@btime collect(rundag(runenv, dag2));

cleanup(runenv)
