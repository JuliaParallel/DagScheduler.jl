__precompile__(true)
module DagScheduler

using Dagger
using MemPool

import Dagger: istask, inputs, Chunk
import Base: delete!

export runbroker, runexecutor, rundag, RunEnv, cleanup

#const META_IMPL = "DagScheduler.SimpleMeta.SimpleSchedMeta"
const META_IMPL = "DagScheduler.ShmemMeta.ShmemSchedMeta"
#const META_IMPL = "DagScheduler.EtcdMeta.EtcdSchedMeta"

include("common.jl")
include("bcast_channel.jl")
include("meta_store.jl")
include("task_queue.jl")
include("tasks.jl")

include("simple_meta_store.jl")

end # module
