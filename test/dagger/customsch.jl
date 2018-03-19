module CustomSch

using DagScheduler
using Dagger

DagScheduler.META_IMPL[:node] = "DagScheduler.ShmemMeta.ShmemExecutorMeta"
DagScheduler.META_IMPL[:cluster] = "DagScheduler.ShmemMeta.ShmemExecutorMeta"

_drunenv = nothing
function compute_dag(ctx, d::Dagger.Thunk)
    global _drunenv
    if _drunenv === nothing
        isdir(".mempool") && rm(".mempool"; recursive=true)
        node1 = DagScheduler.NodeEnv(1, getipaddr(), [2,3])
        _drunenv = DagScheduler.RunEnv(; nodes=[node1])
    end
    DagScheduler.rundag(_drunenv, d)
end

function cleanup(ctx)
    global _drunenv
    (_drunenv === nothing) || DagScheduler.cleanup(_drunenv)
    _drunenv = nothing
end

Dagger.PLUGIN_CONFIGS[:scheduler] = "CustomSch"

end # module CustomSch
