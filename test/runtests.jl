addprocs(5)

include("daggen.jl")
using DagScheduler
using Base.Test

isdir(".mempool") && rm(".mempool"; recursive=true)

@testset "deep dag" begin
    info("Testing deep dag...")
    dag1 = gen_straight_dag(ones(Int, 6^4))
    result = rundag(dag1, nexecutors=nworkers(), debug=false)
    info("result = ", result)
    @test result == 1

    #=
    const dag3 = gen_cross_dag()
    result = rundag(dag3, nexecutors=nworkers(), debug=false)
    info("result = ", result)
    @test result == 84
    =#
end

@testset "shallow dag - sorting" begin
    info("Testing shallow dag - sorting...")

    for L in (10^6, 10^7)
        dag2 = gen_sort_dag(L, 40, 4)
        result = rundag(dag2, nexecutors=nworkers(), debug=false)
        info("result = ", typeof(result), ", length: ", length(result), ", sorted: ", issorted(result))
        @test isa(result, Array{Float64,1})
        @test issorted(result)
        @test length(result) == L
        @everywhere MemPool.cleanup()
    end
end

isdir(".mempool") && rm(".mempool"; recursive=true)
