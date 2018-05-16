using DagScheduler
using FoundationDB
using Base.Test

start_client()
atexit() do
    stop_client()
end
yield()

@testset "fdb dict" begin
    open(FDBCluster()) do cluster
        open(FDBDatabase(cluster)) do db
            ncallbacks = 0
            rs = DagScheduler.FdbMeta.FdbResultStore(db, "A"; callback=(tid,result)->(ncallbacks+=1))

            DagScheduler.FdbMeta.clear(rs)
            DagScheduler.FdbMeta.init(rs)
            DagScheduler.FdbMeta.start_processing_events(rs)

            @test isempty(rs.dict)

            test_tids = DagScheduler.TaskIdType[1, 2]
            test_results = [rand(UInt8, 5), rand(UInt8, 5)]

            DagScheduler.FdbMeta.publish_result(rs, test_tids[1], test_results[1])
            sleep(0.5)
            @test length(rs.dict) == 1
            @test rs.dict[test_tids[1]] == test_results[1]

            DagScheduler.FdbMeta.publish_result(rs, test_tids[2], test_results[2])
            sleep(0.5)
            @test length(rs.dict) == 2
            @test rs.dict[test_tids[2]] == test_results[2]

            DagScheduler.FdbMeta.stop_processing_events(rs)
            @test rs.eventprocessor == nothing
            @test ncallbacks == 2
        end
    end
end # testset fdb queue
