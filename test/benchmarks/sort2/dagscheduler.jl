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

const L = 10^6
const dag2 = DagScheduler.dref_to_fref!(DagScheduler.persist_chunks!(gen_sort_dag(L, 40, 4, 40)));

result = collect(rundag(runenv, dag2));
#@time result = rundag(runenv, dag2);
@btime collect(rundag(runenv, dag2));

DagScheduler.cleanup(runenv)
