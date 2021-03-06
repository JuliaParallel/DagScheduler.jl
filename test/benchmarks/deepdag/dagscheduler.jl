addprocs(5)

include("../../daggen.jl")
using DagScheduler
using BenchmarkTools

isdir(".mempool") && rm(".mempool"; recursive=true)
@everywhere begin
    DagScheduler.META_IMPL[:node] = ENV["NODE_META_IMPL"]
    DagScheduler.META_IMPL[:cluster] = ENV["NODE_META_IMPL"]
    DagScheduler.META_IMPL[:map_num_entries] = 1024*5
    DagScheduler.META_IMPL[:map_entry_sz] = 256
end

node1 = NodeEnv(1, getipaddr(), [2,3,4,5,6])
runenv = DagScheduler.Plugin.setrunenv(RunEnv(; nodes=[node1]))

const L = 6^4
const dag2 = gen_straight_dag(ones(Int, L));

result = collect(rundag(runenv, dag2));
#@time result = rundag(runenv, dag2);
@btime collect(rundag(runenv, dag2))

DagScheduler.cleanup(runenv)
