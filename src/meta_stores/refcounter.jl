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

const RefCounter = Ref{Module}(NoRC)
