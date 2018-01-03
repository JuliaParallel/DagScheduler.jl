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
    #false
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
