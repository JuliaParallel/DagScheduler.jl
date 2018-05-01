module Plugin

import ..DagScheduler
using Dagger

_drunenv = nothing
function compute_dag(ctx, d::Dagger.Thunk)
    global _drunenv
    if _drunenv === nothing
        isdir(".mempool") && rm(".mempool"; recursive=true)
        _drunenv = DagScheduler.RunEnv()
    end
    DagScheduler.rundag(_drunenv, d)
end

function cleanup(ctx)
    global _drunenv
    (_drunenv === nothing) || DagScheduler.cleanup(_drunenv)
    _drunenv = nothing
end

function __init__()
    Dagger.PLUGIN_CONFIGS[:scheduler] = "DagScheduler.Plugin"
end

function setrunenv(runenv)
    global _drunenv
    _drunenv = runenv
end

end # module Plugin
