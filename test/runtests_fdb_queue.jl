using DagScheduler
using FoundationDB
using Base.Test

start_client()
yield()

@testset "fdb queue" begin
    open(FDBCluster()) do cluster
        open(FDBDatabase(cluster)) do db
            sharemode = DagScheduler.ShareMode(1)
            ts = DagScheduler.FdbMeta.FdbTaskStore(db, "A", sharemode)

            DagScheduler.FdbMeta.clear(ts)
            DagScheduler.FdbMeta.init(ts)
            DagScheduler.FdbMeta.start_processing_events(ts)

            @test isempty(ts.taskids)
            @test isempty(ts.taskprops)

            test_tids = DagScheduler.TaskIdType[1, 2]
            test_annotated_tids = DagScheduler.TaskIdType[11, 12]

            DagScheduler.FdbMeta.new_task(ts, test_tids[1], test_annotated_tids[1])
            sleep(0.5)
            @test length(ts.taskids) == 1
            @test length(ts.taskprops) == 1
            @test ts.taskids[1] == test_tids[1]
            key, tid_annotated, reservation = ts.taskprops[test_tids[1]]
            @test tid_annotated == test_annotated_tids[1]
            @test reservation == 0
            @test sharemode.ncreated == 1
            @test sharemode.ndeleted == 0
            @test sharemode.nshared == 1

            DagScheduler.FdbMeta.reserve_task(ts, test_tids[1], 3)
            sleep(0.5)
            @test length(ts.taskids) == 0
            @test length(ts.taskprops) == 1
            key, tid_annotated, reservation = ts.taskprops[test_tids[1]]
            @test tid_annotated == test_annotated_tids[1]
            @test reservation == 3
            @test sharemode.ncreated == 1
            @test sharemode.ndeleted == 1
            @test sharemode.nshared == 0

            DagScheduler.FdbMeta.new_task(ts, test_tids[2], test_annotated_tids[2])
            sleep(0.5)
            @test length(ts.taskids) == 1
            @test length(ts.taskprops) == 2
            @test ts.taskids[1] == test_tids[2]
            key, tid_annotated, reservation = ts.taskprops[test_tids[2]]
            @test tid_annotated == test_annotated_tids[2]
            @test reservation == 0
            @test sharemode.ncreated == 2
            @test sharemode.ndeleted == 1
            @test sharemode.nshared == 1

            DagScheduler.FdbMeta.unreserve_task(ts, test_tids[1])
            sleep(0.5)
            @test length(ts.taskids) == 2
            @test length(ts.taskprops) == 2
            @test ts.taskids[1] == test_tids[2]
            @test ts.taskids[2] == test_tids[1]
            key, tid_annotated, reservation = ts.taskprops[test_tids[1]]
            @test tid_annotated == test_annotated_tids[1]
            @test reservation == 0
            @test sharemode.ncreated == 3
            @test sharemode.ndeleted == 1
            @test sharemode.nshared == 2

            DagScheduler.FdbMeta.reserve_task(ts, test_tids[1], 4)
            DagScheduler.FdbMeta.reserve_task(ts, test_tids[2], 4)
            sleep(0.5)
            @test length(ts.taskids) == 0
            @test length(ts.taskprops) == 2
            key, tid_annotated, reservation = ts.taskprops[test_tids[1]]
            @test tid_annotated == test_annotated_tids[1]
            @test reservation == 4
            key, tid_annotated, reservation = ts.taskprops[test_tids[2]]
            @test tid_annotated == test_annotated_tids[2]
            @test reservation == 4
            @test sharemode.ncreated == 3
            @test sharemode.ndeleted == 3
            @test sharemode.nshared == 0

            DagScheduler.FdbMeta.finish_task(ts, test_tids[1])
            DagScheduler.FdbMeta.finish_task(ts, test_tids[2])
            sleep(0.5)
            DagScheduler.FdbMeta.prune_finished_tasks(ts)
            @test isempty(ts.taskids)
            @test isempty(ts.taskprops)
            @test sharemode.ncreated == 3
            @test sharemode.ndeleted == 3
            @test sharemode.nshared == 0

            DagScheduler.FdbMeta.stop_processing_events(ts)
            @test ts.eventprocessor == nothing
        end
    end
end # testset fdb queue

stop_client()
