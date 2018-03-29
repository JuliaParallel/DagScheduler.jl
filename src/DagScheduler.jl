__precompile__(true)
module DagScheduler

using Dagger
using MemPool
using RemoteMonitor

import Dagger: istask, inputs, Chunk, cleanup
import Base: delete!, filter!, +, /, isless


export runmaster, runbroker, runexecutor, rundag, RunEnv, NodeEnv, cleanup

const META_IMPL = Dict(
    # :node => "DagScheduler.SimpleMeta.SimpleExecutorMeta",
    :node => "DagScheduler.ShmemMeta.ShmemExecutorMeta",
    # :node => "DagScheduler.EtcdMeta.EtcdExecutorMeta",
    :cluster => "DagScheduler.SimpleMeta.SimpleExecutorMeta",
    # :cluster => "DagScheduler.ShmemMeta.ShmemExecutorMeta",
    # :cluster => "DagScheduler.EtcdMeta.EtcdExecutorMeta",
    :_ => "_"
)

include("common.jl")
include("bcast_channel.jl")
include("meta_stores/meta_store.jl")
include("scheduling/scheduler.jl")
include("execution/queue.jl")
include("execution/engine.jl")

end # module
