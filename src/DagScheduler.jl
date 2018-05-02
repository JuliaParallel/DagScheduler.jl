__precompile__(true)
module DagScheduler

using Dagger
using MemPool
using RemoteMonitor

import Dagger: istask, inputs, Chunk, cleanup
import Base: delete!, filter!, +, /, isless, show


export rundag, RunEnv, NodeEnv, cleanup, reset

const META_IMPL = Dict(
    # configurable metadata implementations to use
    :node => "DagScheduler.ShmemMeta.ShmemExecutorMeta",
    :cluster => "DagScheduler.SimpleMeta.SimpleExecutorMeta",

    # configuration of shmem metadata parameters
    :map_num_entries => 1024*5,         # max number of results to store in shared dict
    :map_entry_sz => 256,               # max size of each result stored in shared dict
    :done_tasks_sz => 1024*100,         # size of shm, limits the max number of nodes in dag (roughly > (total_dag_nodes / nphyiscal nodes))
    :shared_tasks_sz => 1024*100,       # size of shm, limits the max number of nodes in dag (roughly > (total_dag_nodes / nphyiscal nodes))
    :_ => "_"
)

include("common.jl")
include("bcast_channel.jl")
include("meta_stores/meta_store.jl")
include("scheduling/scheduler.jl")
include("execution/queue.jl")
include("execution/engine.jl")
include("plugin.jl")

end # module
