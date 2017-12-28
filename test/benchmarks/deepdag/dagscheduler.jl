addprocs(5)

include("../../daggen.jl")
using DagScheduler
using BenchmarkTools

isdir(".mempool") && rm(".mempool"; recursive=true)
runenv = RunEnv();

const L = 6^4
const dag2 = gen_straight_dag(ones(Int, L));

result = collect(rundag(runenv, dag2));
#@time result = rundag(runenv, dag2);
@btime collect(rundag(runenv, dag2))

cleanup(runenv)
