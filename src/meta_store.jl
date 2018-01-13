# scheduler metadata store
# -------------------------
# Used to share metadata among processes in a node (result and refcount).
# It would be possible to switch between different implementations of SchedMeta.

abstract type SchedMeta end


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
        true
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

meta_deser(v) = meta_deser(string(v))
meta_deser(v::String) = meta_deser(base64decode(v))
meta_deser(v::Vector{UInt8}) = deserialize(IOBuffer(v))
function meta_ser(x)
    iob = IOBuffer()
    serialize(iob, x)
    base64encode(take!(iob))
end

function repurpose_result_to_export(t::Thunk, val)
    if !isa(val, Chunk)
        val = Dagger.tochunk(val, persist = t.persist, cache = t.persist ? true : t.cache)
    end
    if isa(val.handle, DRef)
        val = chunktodisk(val)
    end
    val
end

resultroot{T<:SchedMeta}(M::T) = joinpath(M.path, "result")
resultpath{T<:SchedMeta}(M::T, id::TaskIdType) = joinpath(resultroot(M), string(id))
sharepath{T<:SchedMeta}(M::T, id::TaskIdType) = joinpath(M.path, "shared", string(id))
taskpath{T<:SchedMeta}(M::T, brokerid::String) = joinpath(M.path, "broker", brokerid)

should_share{T<:SchedMeta}(M::T) = should_share(M.sharemode)
should_share{T<:SchedMeta}(M::T, nreserved::Int) = should_share(M.sharemode, nreserved)

init{T<:SchedMeta}(M::T, brokerid::String) = error("method not implemented for $T")
wait_trigger{T<:SchedMeta}(M::T; timeoutsec::Int=5) = error("method not implemented for $T")
delete!{T<:SchedMeta}(M::T) = error("method not implemented for $T")
reset{T<:SchedMeta}(M::T) = error("method not implemented for $T")
share_task{T<:SchedMeta}(M::T, brokerid::String, id::TaskIdType; annotation=identity) = error("method not implemented for $T")
steal_task{T<:SchedMeta}(M::T, brokerid::String; annotation=identity) = error("method not implemented for $T")
set_result{T<:SchedMeta}(M::T, id::TaskIdType, val; refcount::UInt64=UInt64(1), processlocal::Bool=true) = error("method not implemented for $T")
get_result{T<:SchedMeta}(M::T, id::TaskIdType) = error("method not implemented for $T")
has_result{T<:SchedMeta}(M::T, id::TaskIdType) = error("method not implemented for $T")
decr_result_ref{T<:SchedMeta}(M::T, id::TaskIdType) = error("method not implemented for $T")
export_local_result{T<:SchedMeta}(M::T, id::TaskIdType, executable, refcount::UInt64) = error("method not implemented for $T")

function get_type(s::String)
    T = Main
    for t in split(s, ".")
        T = eval(T, Symbol(t))
    end
    T
end

metastore(name::String, args...) = (get_type(name))(args...)

# include the meta implementations

# EtcdMeta - uses Etcd as centralized metadata store
#module EtcdMeta
#
#using Etcd
#
#include("etcd_meta_store.jl")
#
#end

# SimpleMeta - uses Julia messaging and remotecalls
module SimpleMeta

using Base.Threads

import ..DagScheduler
import ..DagScheduler: TaskIdType, SchedMeta, ShareMode, NoTask,
        take_share_snapshot, should_share, reset, meta_deser, meta_ser, resultroot, resultpath, sharepath, taskpath,
        init, delete!, wait_trigger, share_task, steal_task, set_result, get_result, has_result, decr_result_ref,
        export_local_result, repurpose_result_to_export

export SimpleSchedMeta

include("bcast_channel.jl")

const Results = BcastChannel{Tuple{String,String}}

const META = Dict{String,String}()
const TASKS = Ref(Channel{TaskIdType}(1024))
const RESULTS = Results()
const taskmutex = Ref(Mutex())

include("simple_meta_store.jl")

function __init__()
    TASKS[] = Channel{TaskIdType}(1024)
    taskmutex[] = Mutex()
    nothing
end

end # module SimpleMeta
