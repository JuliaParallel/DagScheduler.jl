include("daggen.jl")

addprocs(5)

using DagScheduler
using Base.Test

const dag = gendeepdag(T)
result = rundag(dag, nexecutors=nworkers(), slowdown=false, debug=false)
info("result = ", result)
@test result == 84
