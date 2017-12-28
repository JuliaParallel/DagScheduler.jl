addprocs(5)

include("../../daggen.jl")
using DagScheduler
using BenchmarkTools

isdir(".mempool") && rm(".mempool"; recursive=true)
runenv = RunEnv()

const L = 10^6
const dag2 = DagScheduler.dref_to_fref(DagScheduler.persist_chunks!(gen_sort_dag(L, 40, 4, 1)));

result = collect(rundag(runenv, dag2));
#@time result = rundag(runenv, dag2);
@btime collect(rundag(runenv, dag2));

cleanup(runenv)
