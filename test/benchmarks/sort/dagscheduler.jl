addprocs(5)

include("../../daggen.jl")
using DagScheduler
using BenchmarkTools

isdir(".mempool") && rm(".mempool"; recursive=true)

const L = 10^6
const dag2 = DagScheduler.dref_to_fref(DagScheduler.persist_chunks!(gen_sort_dag(L, 40, 4)));

result = rundag(dag2, nexecutors=nworkers(), debug=false);
@time result = rundag(dag2, nexecutors=nworkers(), debug=false);

@btime rundag(dag2, nexecutors=nworkers(), debug=false);
