include("daggen.jl")
using Base.Test

isdir(".mempool") && rm(".mempool"; recursive=true)

# need to generate dags before addprocs, because Dagger tries to distribute DRefs on its own otherwise
const dag1 = gen_straight_dag(ones(Int, 6^4))
const dag2 = gen_sort_dag()

addprocs(5)
using DagScheduler

result = rundag(dag1, nexecutors=nworkers(), slowdown=false, debug=false)
info("result = ", result)
@test result == 1

#=
const dag3 = gen_cross_dag()
result = rundag(dag3, nexecutors=nworkers(), slowdown=false, debug=false)
info("result = ", result)
@test result == 84
=#

result = rundag(dag2, nexecutors=nworkers(), slowdown=false, debug=false)
info("result = ", result)
@test issorted(result)

isdir(".mempool") && rm(".mempool"; recursive=true)
