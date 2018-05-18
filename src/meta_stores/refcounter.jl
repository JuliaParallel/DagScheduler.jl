# result reference counter
# -------------------------
# Used to keep track if a result should be kept around because it is needed by another node in the DAG.
# If not, then results can be cleaned to free up resources.

"""
Simple reference counting done by the scheduler master
"""
module SimpleRC

import ..DagScheduler
import ..DagScheduler: TaskIdType

const REFCOUNT = Dict{TaskIdType,Int}()

refcount(id::TaskIdType, cnt::Int) = remotecall_fetch(_refcount, 1, id, cnt)
clean_refcount() = remotecall_fetch(_clean_refcount, 1)
nz_refcounts() = remotecall_fetch(_nz_refcounts, 1)

function _refcount(id::TaskIdType, cnt::Int)
    ref = REFCOUNT[id] = (get!(REFCOUNT, id, 0) + cnt)
    if ref == 0
        delete!(REFCOUNT, id)
    end
    ref
end

function _clean_refcount()
    empty!(REFCOUNT)
    nothing
end

function _nz_refcounts()
    filter((n,v)->(v > 0), REFCOUNT)
end

end # SimpleRC

"""
No reference counting done by the scheduler
"""
module NoRC

import ..DagScheduler
import ..DagScheduler: TaskIdType

refcount(id::TaskIdType, cnt::Int) = 2
clean_refcount() = nothing
nz_refcounts() = Dict{TaskIdType,Int}()

end # NoRC

"""
Maintain reference counts on foundation db
"""
module FdbRC

import ..DagScheduler
import ..DagScheduler: TaskIdType
using FoundationDB

const RefCountRoot = UInt8[0]
const fdb_db = Ref{Union{Void,FDBDatabase}}(nothing)

function set_fdb_db()
    fdb_db[] = DagScheduler.FdbMeta.fdb_db[]
    nothing
end

function refcount(id::TaskIdType, cnt::Int)
    key = vcat(RefCountRoot, reinterpret(UInt8, [id]))
    open(FDBTransaction(fdb_db[])) do tran
        atomic_add(tran, key, cnt)
    end
    ref = open(FDBTransaction(fdb_db[])) do tran
        ref = atomic_integer(Int, getval(tran, key))
        if ref == 0
            clearkey(tran, key)
        end
    end
    ref
end

function clean_refcount()
    start_key = vcat(RefCountRoot, UInt8[0])
    end_key = vcat(RefCountRoot, UInt8[0xff])
    open(FDBTransaction(fdb_db[])) do tran
        clearkeyrange(tran, start_key, end_key)
    end
    nothing
end

function nz_refcounts()
    start_key = vcat(RefCountRoot, UInt8[0])
    end_key = vcat(RefCountRoot, UInt8[0xff])
    kvs = open(FDBTransaction(fdb_db[])) do tran
        kvs, more = getrange(tran, keysel(FDBKeySel.first_greater_than, start_key), keysel(FDBKeySel.first_greater_than, end_key))
        kvs
    end
    res = Dict{TaskIdType,Int}()
    if kvs !== nothing
        for kv in kvs
            key,val = kv
            id = reinterpret(TaskIdType, key[(end-sizeof(TaskIdType)+1):end])[1]
            cnt = atomic_integer(Int, val)
            res[id] = cnt
        end
    end
    res
end

end # FdbRC

const RefCounter = Ref{Module}(NoRC)
