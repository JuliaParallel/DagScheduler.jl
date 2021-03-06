# scheduler metadata store
# -------------------------
# Used to share metadata among processes in a node (result and refcount).
# It would be possible to switch between different implementations of ExecutorMeta.

abstract type ExecutorMeta end


"""
ShareMode holds sharing statistics and determines share-first/help-first mode.
"""
mutable struct ShareMode
    nshared::Int
    ncreated::Int
    ndeleted::Int
    sharesnapshot::Tuple{Int,Int}
    sharethreshold::Int
    shouldshare::Bool

    function ShareMode(sharethreshold::Int)
        new(0, 0, 0, (0,0), sharethreshold, true)
    end
end

function take_share_snapshot(sm::ShareMode)
    t1 = sm.sharesnapshot
    t2 = (sm.ncreated, sm.ndeleted)
    sm.sharesnapshot = t2
    incr = t2 .- t1
    incr[1] - incr[2] # residual tasks shared in the interval (>0 => stop sharing more)
end

function should_share(sm::ShareMode)
    if sm.nshared == 0
        (sm.sharethreshold > 0)
    elseif sm.nshared <= sm.sharethreshold
        incr = take_share_snapshot(sm)
        sm.shouldshare = (incr == 0) ? sm.shouldshare : (incr < 0)
    else
        false
    end
end

function should_share(sm::ShareMode, nreserved::Int)
    (nreserved > sm.sharethreshold) && (sm.nshared == 0)
end

function reset(sm::ShareMode)
    sm.nshared = sm.ncreated = sm.ndeleted = 0
    sm.sharesnapshot = (0, 0)
    sm.shouldshare = true
    nothing
end

struct Packed
    res
end

#meta_deser(v) = meta_deser(String(v))
#meta_deser(v::String) = meta_deser(base64decode(v))
meta_deser(v::Vector{UInt8}) = deserialize(IOBuffer(v))
function meta_ser(x)
    iob = IOBuffer()
    serialize(iob, x)
    #base64encode(take!(iob))
    take!(iob)
end

meta_pack(v::Chunk) = isa(v.handle, DRef) ? chunktodisk(v) : v
meta_pack(v::Vector{Chunk}) = Packed(chunktodisk(Dagger.tochunk(chunktodisk(v))))
meta_pack(v) = Packed(chunktodisk(Dagger.tochunk(v)))
meta_unpack(v) = v
meta_unpack(v::Packed) = collect(v.res)

function repurpose_result_to_export(t::Thunk, val)
    if !isa(val, Chunk)
        val = Dagger.tochunk(val, persist = t.persist, cache = t.persist ? true : t.cache)
    end
    if isa(val.handle, DRef)
        val = chunktodisk(val)
    end
    val
end

function brokercall(fn, M, args...)
    result = remotecall_fetch(fn, M.brokerid, args...)
    if isa(result, Exception)
        @show result
        throw(result)
    end
    result
end

default_task_selector(x) = 1

resultroot{T<:ExecutorMeta}(M::T) = joinpath(M.path, "R")
resultpath{T<:ExecutorMeta}(M::T, id::TaskIdType) = joinpath(resultroot(M), string(id))
sharepath{T<:ExecutorMeta}(M::T, id::TaskIdType) = joinpath(M.path, "S", string(id))
taskpath{T<:ExecutorMeta}(M::T) = joinpath(M.path, "B", string(M.brokerid))

should_share{T<:ExecutorMeta}(M::T) = should_share(M.sharemode)
should_share{T<:ExecutorMeta}(M::T, nreserved::Int) = should_share(M.sharemode, nreserved)

init{T<:ExecutorMeta}(M::T, brokerid::Int; add_annotation=identity, del_annotation=identity, result_callback=nothing) = error("method not implemented for $T")
wait_trigger{T<:ExecutorMeta}(M::T; timeoutsec::Int=5) = error("method not implemented for $T")
delete!{T<:ExecutorMeta}(M::T) = error("method not implemented for $T")
reset{T<:ExecutorMeta}(M::T) = error("method not implemented for $T")
cleanup{T<:ExecutorMeta}(M::T) = error("method not implemented for $T")
detach{T<:ExecutorMeta}(M::T, pid) = error("method not implemented for $T")
share_task{T<:ExecutorMeta}(M::T, id::TaskIdType, allow_dup::Bool) = error("method not implemented for $T")
steal_task{T<:ExecutorMeta}(M::T, selector=default_task_selector) = error("method not implemented for $T")
set_result{T<:ExecutorMeta}(M::T, id::TaskIdType, val; refcount::Int=0, processlocal::Bool=true) = error("method not implemented for $T")
get_result{T<:ExecutorMeta}(M::T, id::TaskIdType) = error("method not implemented for $T")
has_result{T<:ExecutorMeta}(M::T, id::TaskIdType) = error("method not implemented for $T")
decr_result_ref{T<:ExecutorMeta}(M::T, id::TaskIdType) = error("method not implemented for $T")
export_local_result{T<:ExecutorMeta}(M::T, id::TaskIdType, executable) = error("method not implemented for $T")

function get_type(s::String)
    T = Main
    for t in split(s, ".")
        T = eval(T, Symbol(t))
    end
    T
end

metastore(name::String, args...) = (get_type(name))(args...)

# include the meta implementations

# ShmemMeta - uses shared memory as metadata store
module ShmemMeta

using Semaphores
using SharedDataStructures

import ..DagScheduler
import ..DagScheduler: TaskIdType, ExecutorMeta, ShareMode, NoTask, BcastChannel,
        take_share_snapshot, should_share, reset, cleanup, detach, meta_deser, meta_ser, meta_unpack, meta_pack, resultroot, resultpath, sharepath, taskpath,
        init, delete!, wait_trigger, share_task, steal_task, set_result, get_result, has_result, decr_result_ref,
        export_local_result, repurpose_result_to_export, register, deregister, put!, brokercall, default_task_selector, @timetrack, statetrack, logmsg

export ShmemExecutorMeta

const pinger = BcastChannel{Void}()

include("shmem_meta_store.jl")

end # module ShmemMeta

# SimpleMeta - uses Julia messaging and remotecalls
module SimpleMeta

using Base.Threads

import ..DagScheduler
import ..DagScheduler: TaskIdType, ExecutorMeta, ShareMode, NoTask, BcastChannel,
        take_share_snapshot, should_share, reset, cleanup, detach, meta_deser, meta_ser, meta_unpack, meta_pack, resultroot, resultpath, sharepath, taskpath,
        init, delete!, wait_trigger, share_task, steal_task, set_result, get_result, has_result, decr_result_ref,
        export_local_result, repurpose_result_to_export, register, deregister, put!, brokercall, default_task_selector, @timetrack, statetrack, logmsg

export SimpleExecutorMeta

const Results = BcastChannel{Tuple{String,Vector{UInt8}}}

const META = Dict{String,String}()
const TASKS = Vector{TaskIdType}()
const RESULTS = Results()
const PID_RR_MAP = Dict{Int,RemoteChannel}()
const PID_TASK_MAP = Dict{Int, Dict{TaskIdType,TaskIdType}}()
const taskmutex = Channel{Bool}(1)

include("simple_meta_store.jl")

function __init__()
    put!(taskmutex, true)
    nothing
end

function withtaskmutex(f)
    l = take!(taskmutex)
    try
        return f()
    finally
        put!(taskmutex, l)
    end
end

end # module SimpleMeta

# FdbMeta - uses foundationdb
module FdbMeta

using FoundationDB

import ..DagScheduler
import ..DagScheduler: TaskIdType, ExecutorMeta, ShareMode, NoTask, BcastChannel,
        take_share_snapshot, should_share, reset, cleanup, detach, meta_deser, meta_ser, meta_unpack, meta_pack, resultroot, resultpath, sharepath, taskpath,
        init, delete!, wait_trigger, share_task, steal_task, set_result, get_result, has_result, decr_result_ref,
        export_local_result, repurpose_result_to_export, register, deregister, put!, brokercall, default_task_selector, @timetrack, statetrack, logmsg

export FdbExecutorMeta

include("fdb_queue.jl")
include("fdb_dict.jl")
include("fdb_meta_store.jl")

const fdb_cluster = Ref{Union{Void,FDBCluster}}(nothing)
const fdb_db = Ref{Union{Void,FDBDatabase}}(nothing)

function __init__()
    try
        start_client()

        fdb_cluster[] = open(FDBCluster())
        fdb_db[] = open(FDBDatabase(fdb_cluster[]))

        atexit() do
            close(fdb_db[])
            close(fdb_cluster[])
            stop_client()
        end
    catch ex
        info("FdbMeta can not be used. FoundationDB could not be initialized.")
    end
    nothing
end

end # module FdbMeta
