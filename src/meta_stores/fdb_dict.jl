# result store keeps results of completed tasks.
# also publishes events when a result is published.
# 
# creation:
# - the subspace is cleared
# - key for event created with atomic zero addition
#
# addition:
# - create a new versionstamped key with taskid and result set as value
# - atomic increment event
#
# deletion (on refcount becoming zero): (not implemented yet)
# - create a new versionstamped key with taskid and result set as nothing
# - atomic increment event
#
# on event (update in-memory state):
# - results are maintained in a dict
# - get all key values from last versionstamp till the last key
# - for every key-value
#   - if this is a new result
#     - add taskid and result to in-memory dict
#     - update current key
#   - if this is a deletion dut to refcount becoming zero
#     - delete taskid from in-memory dict
#     - update current key

mutable struct FdbResultStore
    db::FDBDatabase
    dictpath::Vector{UInt8}
    eventpath::Vector{UInt8}
    dict::Dict{TaskIdType,Vector{UInt8}}
    watchhandle::Union{Void,FDBFuture}
    versionstamp::Vector{UInt8}
    eventprocessor::Union{Void,Task}
    callback::Union{Void,Function}
    run::Bool

    function FdbResultStore(db::FDBDatabase, path::String; callback=nothing)
        dictpath = convert(Vector{UInt8}, joinpath(path, "r") * "/")
        eventpath = convert(Vector{UInt8}, joinpath(path, "re"))
        new(db, dictpath, eventpath, Dict{TaskIdType,Tuple}(), nothing, UInt8[], nothing, callback, true)
    end
end
new_key(rs::FdbResultStore) = prep_atomic_key!(copy(rs.dictpath), length(rs.dictpath)+1)

function clear(rs::FdbResultStore)
    start_key = vcat(rs.dictpath, zeros(UInt8, 10))
    end_key = vcat(rs.dictpath, ones(UInt8, 10) * 0xff)
    open(FDBTransaction(rs.db)) do tran
        clearkeyrange(tran, start_key, end_key)
        clearkey(tran, rs.eventpath)
    end
    nothing
end

function init(rs::FdbResultStore)
    clear(rs)
    open(FDBTransaction(rs.db)) do tran
        atomic_add(tran, rs.eventpath, 0)
    end
    nothing
end

function publish_result(rs::FdbResultStore, tid::TaskIdType, result::Vector{UInt8})
    open(FDBTransaction(rs.db)) do tran
        val = vcat(reinterpret(UInt8, [tid]), result)
        atomic_setval(tran, new_key(rs), val, FDBMutationType.SET_VERSIONSTAMPED_KEY)
        atomic_add(tran, rs.eventpath, 1)
    end
    nothing
end

function range(rs::FdbResultStore)
    r1 = isempty(rs.versionstamp) ? vcat(rs.dictpath, UInt8[0]) : rs.versionstamp
    r2 = vcat(rs.dictpath, UInt8[0xff])
    r1, r2
end

function on_event(rs::FdbResultStore)
    # process all updates
    kvs, more = open(FDBTransaction(rs.db)) do tran
        startkey,endkey = range(rs)
        getrange(tran, keysel(FDBKeySel.first_greater_than, startkey), keysel(FDBKeySel.first_greater_than, endkey))
    end
    if (kvs !== nothing) && !isempty(kvs)
        for kv in kvs
            key,_val = kv
            tid = reinterpret(TaskIdType, _val[1:sizeof(TaskIdType)])[1]
            result = _val[(sizeof(TaskIdType)+1):end]
            rs.dict[tid] = result
            if rs.callback !== nothing
                rs.callback(tid, result)
            end
        end

        # remember the last versionstamp processed
        key,_val = kvs[end]
        rs.versionstamp = key
    end
    nothing
end

function start_watch(rs::FdbResultStore)
    open(FDBTransaction(rs.db)) do tran
        watchhandle = FDBFuture()
        watchtask = watchkey(tran, rs.eventpath; handle=watchhandle)
        rs.watchhandle = watchhandle
        watchtask
    end
end

function process_events(rs::FdbResultStore)
    while rs.run
        watchtask = start_watch(rs)

        # process events
        # - either because an event triggered watchtask
        # - or those that we might have missed while not watching
        on_event(rs)

        try 
            wait(watchtask)
        catch ex
            (isa(ex, FDBError) && (ex.code == 1101)) || rethrow(ex)
        end
    end
    nothing
end

function start_processing_events(rs::FdbResultStore)
    if rs.eventprocessor === nothing
        rs.eventprocessor = @schedule process_events(rs)
    end
    nothing
end

function stop_processing_events(rs::FdbResultStore)
    rs.run = false
    open(FDBTransaction(rs.db)) do tran
        atomic_add(tran, rs.eventpath, 1)
    end
    wait(rs.eventprocessor)
    rs.eventprocessor = nothing
    nothing
end
