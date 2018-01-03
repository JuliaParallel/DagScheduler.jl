__precompile__(true)
module DagScheduler

using Semaphores
using SharedDataStructures
using Dagger
using MemPool
using Etcd

import Dagger: istask, inputs, Chunk
import LMDB: MDBValue, close
import Base: delete!

export runbroker, runexecutor, rundag, RunEnv, cleanup

include("common.jl")
include("meta_store.jl")
include("etcd_meta_store.jl")
include("task_queue.jl")
include("tasks.jl")

end # module
