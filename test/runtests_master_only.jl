addprocs(5)

include("daggen.jl")
using DagScheduler
using Base.Test

isdir(".mempool") && rm(".mempool"; recursive=true)
@everywhere begin
    DagScheduler.META_IMPL[:node] = ENV["NODE_META_IMPL"]
    DagScheduler.META_IMPL[:cluster] = ENV["NODE_META_IMPL"]
    DagScheduler.META_IMPL[:map_num_entries] = 1024*100
    DagScheduler.META_IMPL[:map_entry_sz] = 1512
end
node1 = NodeEnv(1, getipaddr(), [2,3,4,5,6])
runenv = DagScheduler.Plugin.setrunenv(RunEnv(; nodes=[node1]))
RC = (DagScheduler.META_IMPL[:cluster] == "DagScheduler.FdbMeta.FdbExecutorMeta") ? DagScheduler.FdbRC : DagScheduler.SimpleRC

@testset "deep dag" begin
    @everywhere begin
        RC = (DagScheduler.META_IMPL[:cluster] == "DagScheduler.FdbMeta.FdbExecutorMeta") ? DagScheduler.FdbRC : DagScheduler.SimpleRC
        DagScheduler.RefCounter[] = RC
    end

    info("Testing deep dag...")
    dag1 = gen_straight_dag(ones(Int, 6^4))
    result = collect(rundag(runenv, dag1))
    info("result = ", result)
    @test result == 1
    DagScheduler.print_stats(runenv)
    @test isempty(RC.nz_refcounts())

    info("Testing cross connected dag...")
    dag3 = gen_cross_dag()
    result = collect(rundag(runenv, dag3))
    info("result = ", result)
    @test result == 84
    DagScheduler.print_stats(runenv)
    @test isempty(RC.nz_refcounts())
end

@testset "sorting" begin
    @everywhere begin
        DagScheduler.RefCounter[] = DagScheduler.NoRC
    end

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
    @everywhere begin
        RC = (DagScheduler.META_IMPL[:cluster] == "DagScheduler.FdbMeta.FdbExecutorMeta") ? DagScheduler.FdbRC : DagScheduler.SimpleRC
        DagScheduler.RefCounter[] = RC
    end

    info("Testing meta annotation...")
    x = [delayed(rand)(10) for i=1:10]
    y = delayed((c...) -> [c...]; meta=true)(x...)
    result = collect(rundag(runenv, y))
    @test isa(result, Vector{<:Dagger.Chunk})
    @test length(result) == 10
    @everywhere MemPool.cleanup()
    DagScheduler.print_stats(runenv)
    @test isempty(RC.nz_refcounts())
end

DagScheduler.cleanup(runenv)
isdir(".mempool") && rm(".mempool"; recursive=true)

node1 = NodeEnv(1, getipaddr(), [2,4,6])
runenv = RunEnv(; nodes=[node1])

@testset "selectedworkers" begin
    @everywhere begin
        RC = (DagScheduler.META_IMPL[:cluster] == "DagScheduler.FdbMeta.FdbExecutorMeta") ? DagScheduler.FdbRC : DagScheduler.SimpleRC
        DagScheduler.RefCounter[] = RC
    end

    x = [delayed(rand)(10) for i=1:10]
    y = delayed((c...) -> [c...]; meta=true)(x...)
    result = collect(rundag(runenv, y))
    @test isa(result, Vector{<:Dagger.Chunk})
    @test length(result) == 10
    @everywhere MemPool.cleanup()
    DagScheduler.print_stats(runenv)
    @test isempty(RC.nz_refcounts())
end

DagScheduler.cleanup(runenv)
isdir(".mempool") && rm(".mempool"; recursive=true)
