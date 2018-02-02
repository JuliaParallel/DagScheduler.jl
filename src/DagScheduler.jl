__precompile__(true)
module DagScheduler

using Dagger
using MemPool

import Dagger: istask, inputs, Chunk
import Base: delete!

export runmaster, runbroker, runexecutor, rundag, RunEnv, NodeEnv, cleanup

const META_IMPL = Dict(
    # :node => "DagScheduler.SimpleMeta.SimpleSchedMeta",
    :node => "DagScheduler.ShmemMeta.ShmemSchedMeta",
    # :node => "DagScheduler.EtcdMeta.EtcdSchedMeta",
    :cluster => "DagScheduler.SimpleMeta.SimpleSchedMeta",
    # :cluster => "DagScheduler.ShmemMeta.ShmemSchedMeta",
    # :cluster => "DagScheduler.EtcdMeta.EtcdSchedMeta",
    :_ => "_"
)

include("common.jl")
include("bcast_channel.jl")
include("meta_store.jl")
include("task_queue.jl")
include("tasks.jl")

include("simple_meta_store.jl")

end # module
