__precompile__(true)
module DagScheduler

using Dagger
using MemPool

import Dagger: istask, inputs, Chunk
import Base: delete!

export runmaster, runbroker, runexecutor, rundag, RunEnv, cleanup

#const NODE_META_IMPL = "DagScheduler.SimpleMeta.SimpleSchedMeta"
const NODE_META_IMPL = "DagScheduler.ShmemMeta.ShmemSchedMeta"
#const NODE_META_IMPL = "DagScheduler.EtcdMeta.EtcdSchedMeta"

#const CLUSTER_META_IMPL = "DagScheduler.SimpleMeta.SimpleSchedMeta"
const CLUSTER_META_IMPL = "DagScheduler.ShmemMeta.ShmemSchedMeta"
#const CLUSTER_META_IMPL = "DagScheduler.EtcdMeta.EtcdSchedMeta"

include("common.jl")
include("bcast_channel.jl")
include("meta_store.jl")
include("task_queue.jl")
include("tasks.jl")

include("simple_meta_store.jl")

end # module
