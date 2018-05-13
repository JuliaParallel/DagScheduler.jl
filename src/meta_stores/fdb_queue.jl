# task store keeps a list of tasks in an ordered queue
# ordering is by versionstamp (which is also the key in its entirity)
# value stores the taskid,assigned pid (0 if free)
# 
# creation:
# - the subspace is cleared
# - keys for qevent, ncreated and ndeleted counts are created with atomic zero addition
#
# addition:
# - create a new key with versionstamp and set executor to zero
# - atomic increment ncreated
# - atomic increment qevent
#
# toggle reservation:
# - if this is reservation
#   - for the current key
#   - ensure key is present
#   - determine start and end key range
#   - add a read and write conflict for the range
#   - delete the key
# - create a new key with versionstamp and set value to pid on reservation, or empty otherwise
# - atomic increment ndeleted on reservation, or ncreated otherwise
#
# deletion (on task completion):
# - delete the current key
#
# on event (update in-memory state):
# - queue elements are maintained in a vector
# - queue element properties (current key, pid reserved by) are maintained in a dict
# - get all key values from last versionstamp till the last key
# - for every key-value
#   - if this is a reservation
#     - remove taskid from vector
#     - update current key and pid reserved by in properties
#   - if this is a new task
#     - push taskid into vector
#     - update current key in properties and set pid reserved by to zero

struct SubSpaces
    path::String
    queue::Vector{UInt8}
    uniq::Vector{UInt8}
    event::Vector{UInt8}
    ncreated::Vector{UInt8}
    ndeleted::Vector{UInt8}

    function SubSpaces(path::String)
        queue = convert(Vector{UInt8}, joinpath(path, "q") * "/")
        uniq = convert(Vector{UInt8}, joinpath(path, "u") * "/")
        event = convert(Vector{UInt8}, joinpath(path, "qe"))
        ncreated = convert(Vector{UInt8}, joinpath(path, "qnc"))
        ndeleted = convert(Vector{UInt8}, joinpath(path, "qnd"))
        new(path, queue, uniq, event, ncreated, ndeleted)
    end
end
new_key(sp::SubSpaces) = prep_atomic_key!(copy(sp.queue), length(sp.queue)+1)

mutable struct FdbTaskStore
    db::FDBDatabase
    subspaces::SubSpaces
    sharemode::ShareMode
    taskids::Vector{TaskIdType}
    taskprops::Dict{TaskIdType,Tuple}
    watchhandle::Union{Void,FDBFuture}
    versionstamp::Vector{UInt8}
    eventprocessor::Union{Void,Task}
    callback::Union{Void,Function}
    run::Bool

    function FdbTaskStore(db::FDBDatabase, path::String, sharemode::ShareMode; callback=nothing)
        new(db, SubSpaces(path), sharemode, Vector{TaskIdType}(), Dict{TaskIdType,Tuple}(), nothing, UInt8[], nothing, callback, true)
    end
end
set_callback(ts::FdbTaskStore, callback) = (ts.callback = callback)

function clear(ts::FdbTaskStore)
    start_keys = [vcat(ts.subspaces.queue, zeros(UInt8, 10)), vcat(ts.subspaces.uniq, zeros(UInt8, 8))]
    end_keys = [vcat(ts.subspaces.queue, ones(UInt8, 10) * 0xff), vcat(ts.subspaces.uniq, ones(UInt8, 8) * 0xff)]
   end_key = vcat(ts.subspaces.queue, ones(UInt8, 10) * 0xff)

    open(FDBTransaction(ts.db)) do tran
        for (start_key, end_key) in zip(start_keys, end_keys)
            clearkeyrange(tran, start_key, end_key)
        end

        clearkey(tran, ts.subspaces.event)
        clearkey(tran, ts.subspaces.ncreated)
        clearkey(tran, ts.subspaces.ndeleted)
    end
    nothing
end

function init(ts::FdbTaskStore)
    open(FDBTransaction(ts.db)) do tran
        atomic_add(tran, ts.subspaces.event, 0)
        atomic_add(tran, ts.subspaces.ncreated, 0)
        atomic_add(tran, ts.subspaces.ndeleted, 0)
    end
    nothing
end

function new_task(ts::FdbTaskStore, tids::Vector{TaskIdType}, annotated_tids::Vector{TaskIdType}; allow_duplicate::Bool=false)
    for (tid,annotated_tid) in zip(tids, annotated_tids)
        new_task(ts, tid, annotated_tid; allow_duplicate=allow_duplicate)
    end
    nothing
end

function new_task(ts::FdbTaskStore, tid::TaskIdType, annotated_tid::TaskIdType; allow_duplicate::Bool=false)
    open(FDBTransaction(ts.db)) do tran
        uniqkey = vcat(ts.subspaces.uniq, reinterpret(UInt8, TaskIdType[tid]))
        # mark task as created, if no duplicates allowed ensure task was not created before
        if allow_duplicate || (getval(tran, uniqkey) === nothing)
            if !allow_duplicate
                # setup conflict and create task
                endkey = vcat(uniqkey, UInt8[0xff])
                conflict(tran, uniqkey, endkey, FDBConflictRangeType.READ)
                conflict(tran, uniqkey, endkey, FDBConflictRangeType.WRITE)
            end
            setval(tran, uniqkey, UInt8[0])

            val = reinterpret(UInt8, TaskIdType[tid, annotated_tid, 0])
            atomic_setval(tran, new_key(ts.subspaces), val, FDBMutationType.SET_VERSIONSTAMPED_KEY)
            atomic_add(tran, ts.subspaces.ncreated, 1)
            atomic_add(tran, ts.subspaces.event, 1)
        end
    end
    nothing
end

function finish_task(ts::FdbTaskStore, tid::TaskIdType)
    if tid in keys(ts.taskprops)
        key, annotated_tid, reservation = ts.taskprops[tid]
        open(FDBTransaction(ts.db)) do tran
            clearkey(tran, key)
        end
    end
    nothing
end

function prune_finished_tasks(ts::FdbTaskStore)
    reserved = filter((k,v)->(v[3] > 0), ts.taskprops)
    open(FDBTransaction(ts.db)) do tran
        for (k,v) in reserved
            key, annotated_tid, reservation = v
            (getval(tran, key) == nothing) || delete!(reserved, k)
        end
    end
    for kv in reserved
        if kv in ts.taskprops
            delete!(ts.taskprops, first(kv))
        end
    end
    nothing
end

function unreserve_task(ts::FdbTaskStore, tid::TaskIdType)
    if tid in keys(ts.taskprops)
        key, annotated_tid, reservation = ts.taskprops[tid]
        open(FDBTransaction(ts.db)) do tran
            @assert getval(tran, key) !== nothing
            clearkey(tran, key)
            val = reinterpret(UInt8, TaskIdType[tid, annotated_tid, 0])
            atomic_setval(tran, new_key(ts.subspaces), val, FDBMutationType.SET_VERSIONSTAMPED_KEY)
            atomic_add(tran, ts.subspaces.ncreated, 1)
            atomic_add(tran, ts.subspaces.event, 1)
        end
    end
    nothing
end

function reserve_task(ts::FdbTaskStore, tid::TaskIdType, reservation::Integer)
    if tid in keys(ts.taskprops)
        key, annotated_tid, old_reservation = ts.taskprops[tid]
        open(FDBTransaction(ts.db)) do tran
            if getval(tran, key) == nothing
                # task is already reserved
                false
            else
                endkey = getkey(tran, keysel(FDBKeySel.first_greater_than, key))
                conflict(tran, key, endkey, FDBConflictRangeType.READ)
                conflict(tran, key, endkey, FDBConflictRangeType.WRITE)
                clearkey(tran, key)

                val = reinterpret(UInt8, TaskIdType[tid, annotated_tid, reservation])
                atomic_setval(tran, new_key(ts.subspaces), val, FDBMutationType.SET_VERSIONSTAMPED_KEY)
                atomic_add(tran, ts.subspaces.ndeleted, 1)
                atomic_add(tran, ts.subspaces.event, 1)
                true
            end
        end
    else
        false
    end
end

function range(ts::FdbTaskStore)
    r1 = isempty(ts.versionstamp) ? vcat(ts.subspaces.queue, UInt8[0]) : ts.versionstamp
    r2 = vcat(ts.subspaces.queue, UInt8[0xff])
    r1, r2
end

function on_event(ts::FdbTaskStore)
    sleep(0.2)
    # process all updates
    ncreated, ndeleted, kvs, more = open(FDBTransaction(ts.db)) do tran
        startkey,endkey = range(ts)
        ncreated = atomic_integer(Int, getval(tran, ts.subspaces.ncreated))
        ndeleted = atomic_integer(Int, getval(tran, ts.subspaces.ndeleted))
        kvs, more = getrange(tran, keysel(FDBKeySel.first_greater_than, startkey), keysel(FDBKeySel.first_greater_than, endkey))
        ncreated, ndeleted, kvs, more
    end
    if (kvs !== nothing) && !isempty(kvs)
        for kv in kvs
            key,_val = kv
            val = reinterpret(TaskIdType, _val)
            tid = val[1]
            annotated_tid = val[2]
            reservation = val[3]

            if reservation > 0
                pos = findfirst(ts.taskids, tid)
                (pos > 0) && splice!(ts.taskids, pos)
            else
                push!(ts.taskids, tid)
            end
            ts.taskprops[tid] = (key, annotated_tid, reservation)
        end

        # remember the last versionstamp processed
        key,_val = kvs[end]
        ts.versionstamp = key
    end

    # update counts
    ts.sharemode.ncreated = ncreated
    ts.sharemode.ndeleted = ndeleted
    ts.sharemode.nshared = ncreated - ndeleted
    (ts.callback === nothing) || ts.callback(kvs)
    nothing
end

function start_watch(ts::FdbTaskStore)
    open(FDBTransaction(ts.db)) do tran
        watchhandle = FDBFuture()
        watchtask = watchkey(tran, ts.subspaces.event; handle=watchhandle)
        ts.watchhandle = watchhandle
        watchtask
    end
end

function process_events(ts::FdbTaskStore)
    while ts.run
        watchtask = start_watch(ts)

        # process events
        # - either because an event triggered watchtask
        # - or those that we might have missed while not watching
        on_event(ts)

        try 
            wait(watchtask)
        catch ex
            (isa(ex, FDBError) && (ex.code == 1101)) || rethrow(ex)
        end
    end
    nothing
end

function start_processing_events(ts::FdbTaskStore)
    if ts.eventprocessor === nothing
        ts.eventprocessor = @schedule process_events(ts)
    end
    nothing
end

function stop_processing_events(ts::FdbTaskStore)
    ts.run = false
    if ts.eventprocessor !== nothing
        if !istaskdone(ts.eventprocessor)
            open(FDBTransaction(ts.db)) do tran
                atomic_add(tran, ts.subspaces.event, 1)
            end
            wait(ts.eventprocessor)
        end
        ts.eventprocessor = nothing
    end
    nothing
end
