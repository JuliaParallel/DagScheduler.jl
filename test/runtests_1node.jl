addprocs(5)

include("daggen.jl")
using DagScheduler
using Base.Test

isdir(".mempool") && rm(".mempool"; recursive=true)
@everywhere begin
    DagScheduler.META_IMPL[:node] = "DagScheduler.ShmemMeta.ShmemExecutorMeta"
    DagScheduler.META_IMPL[:cluster] = "DagScheduler.SimpleMeta.SimpleExecutorMeta"
end

node1 = NodeEnv(2, getipaddr(), [3,4,5,6])
runenv = RunEnv(; nodes=[node1])

@testset "deep dag" begin
    info("Testing deep dag...")
    dag1 = gen_straight_dag(ones(Int, 6^4))
    result = collect(rundag(runenv, dag1))
    info("result = ", result)
    @test result == 1
    DagScheduler.print_stats(runenv)

    info("Testing cross connected dag...")
    dag3 = gen_cross_dag()
    result = collect(rundag(runenv, dag3))
    info("result = ", result)
    @test result == 84
    DagScheduler.print_stats(runenv)
end

@testset "sorting" begin
    info("Testing sorting...")

    for L in (10^6, 10^7)
        dag2 = gen_sort_dag(L, 40, 4, 1)
        result = collect(rundag(runenv, dag2))
        info("result = ", typeof(result), ", length: ", length(result), ", sorted: ", issorted(result))
        @test isa(result, Array{Float64,1})
        @test issorted(result)
        @test length(result) == L
        @everywhere MemPool.cleanup()
        DagScheduler.print_stats(runenv)

        # for cross dag
        dag4 = gen_sort_dag(L, 40, 4, 40)
        DagScheduler.dref_to_fref!(dag4)
        result = collect(rundag(runenv, dag4))
        info("result = ", typeof(result), ", length: ", length(result))
        fullresult = collect(Dagger.treereduce(delayed(vcat), result))
        @test isa(fullresult, Array{Float64,1})
        @test issorted(fullresult)
        @test length(fullresult) == L
        @everywhere MemPool.cleanup()
        DagScheduler.print_stats(runenv)
    end
end

@testset "meta" begin
    info("Testing meta annotation...")
    x = [delayed(rand)(10) for i=1:10]
    y = delayed((c...) -> [c...]; meta=true)(x...)
    result = collect(rundag(runenv, y))
    @test isa(result, Vector{<:Dagger.Chunk})
    @test length(result) == 10
    @everywhere MemPool.cleanup()
    DagScheduler.print_stats(runenv)
end

cleanup(runenv)
isdir(".mempool") && rm(".mempool"; recursive=true)
