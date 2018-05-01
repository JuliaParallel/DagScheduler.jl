addprocs(5)

include("../../daggen.jl")
using DagScheduler
using BenchmarkTools

isdir(".mempool") && rm(".mempool"; recursive=true)
@everywhere begin
    DagScheduler.META_IMPL[:map_num_entries] = 1024*5
    DagScheduler.META_IMPL[:map_entry_sz] = 256
end

runenv = DagScheduler.Plugin.setrunenv(RunEnv())

const L = 6^4
const dag2 = gen_straight_dag(ones(Int, L));

result = collect(rundag(runenv, dag2));
#@time result = rundag(runenv, dag2);
@btime collect(rundag(runenv, dag2))

DagScheduler.cleanup(runenv)
