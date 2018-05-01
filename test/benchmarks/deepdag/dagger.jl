addprocs(5)

include("../../daggen.jl")
using DagScheduler
using BenchmarkTools

isdir(".mempool") && rm(".mempool"; recursive=true)
@everywhere begin
    function resetscheduler()
        Dagger.PLUGIN_CONFIGS[:scheduler] = "Dagger.Sch"
    end
    Dagger.use_shared_array[] = true
    resetscheduler()
end

const L = 6^4
const dag2 = gen_straight_dag(ones(Int, L));

function clean_compute(dag2)
    result = compute(dag2)
    gc() # gc required to prevent memory usage rising exponentially
    @everywhere gc()
    collect(result) # to make it similar to what dagscheduer does today
end

result = clean_compute(dag2);
#@time result = clean_compute(dag2);
@btime clean_compute(dag2);
