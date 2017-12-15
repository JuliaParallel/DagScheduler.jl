addprocs(5)

include("../../daggen.jl")
using DagScheduler
using BenchmarkTools

isdir(".mempool") && rm(".mempool"; recursive=true)

const L = 6^4
const dag2 = gen_straight_dag(ones(Int, L));

result = rundag(dag2, nexecutors=nworkers(), debug=false);
@time result = rundag(dag2, nexecutors=nworkers(), debug=false);

@btime rundag(dag2, nexecutors=nworkers(), debug=false);
