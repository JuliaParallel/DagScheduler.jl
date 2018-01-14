# scheduler metadata store
# -------------------------
# used to share metadata among processes in a node (result and refcount)
#
# layered storage
# - executors keep intermediate results in process memory till they need to give back the result of a stolen task
# - results are cached in process memory too
# - also does reference counting and purging

const M_RESULT = UInt8(1)
const M_REFCOUNT = UInt8(2)

struct NodeMetaKey
    id::TaskIdType
    attr::UInt8
end

askey(key::NodeMetaKey) = "$(key.id).$(key.attr)"

struct NodeAttrProp
    set_processor::Function
    get_processor::Function
    get_type::DataType
end

meta_deser(v) = meta_deser(convert(Vector{UInt8}, v))
meta_deser(v::Vector{UInt8}) = deserialize(IOBuffer(v))
function meta_ser(x)
    iob = IOBuffer()
    serialize(iob, x)
    take!(iob)
end

const ATTR_PROPS = [
    NodeAttrProp(meta_ser, meta_deser, Vector{UInt8}),
    NodeAttrProp(identity, identity, UInt64)
]

struct SchedulerNodeMetadata
    dbpath::String
    env::Environment
    proclocal::Dict{String,Any}
    donetasks::SharedCircularDeque{TaskIdType}

    function SchedulerNodeMetadata(path::String; ndbs::Int=1, nreaders::Int=1, mapsz::Int=MAP_SZ)
        isdir(path) || mkdir(path)
        env = LMDB.create()

        # we want it to be sync, since we are using across processes
        isflagset(env[:Flags], Cuint(LMDB.NOSYNC)) && unset!(env, LMDB.NOSYNC)

        env[:Readers] = nreaders
        env[:MapSize] = mapsz
        env[:DBs] = ndbs

        open(env, path)
        donetasks = SharedCircularDeque{TaskIdType}(donetaskspath(path), DONE_TASKS_SZ; create=false)
        new(path, env, Dict{String,Any}(), donetasks)
    end
end

donetaskspath(M::SchedulerNodeMetadata) = donetaskspath(M.path)
donetaskspath(path::String) = joinpath(path, DONE_TASKS)

function close(M::SchedulerNodeMetadata)
    close(M.env)
    empty!(M.donetasks)
    nothing
end

function delete!(M::SchedulerNodeMetadata)
    reset(M; delete=true)
    close(M)
    rm(M.dbpath; recursive=true)
    nothing
end

function reset(M::SchedulerNodeMetadata; delete::Bool=false, dropdb::Bool=true)
    empty!(M.proclocal)
    if dropdb
        txn = start(M.env)
        dbi = open(txn)
        drop(txn, dbi; delete=delete)
        commit(txn)
        close(M.env, dbi)
    end
    nothing
end

function create_metadata_ipc(metastore::String, deques::Vector{SharedCircularDeque{TaskIdType}})
    donetasks = donetaskspath(metastore)
    isdir(metastore) && rm(metastore; recursive=true)
    mkpath(metastore)
    mkpath(donetasks)
    push!(deques, SharedCircularDeque{TaskIdType}(donetasks, DONE_TASKS_SZ; create=true))
    nothing
end

_prochas(M::SchedulerNodeMetadata, key::NodeMetaKey) = askey(key) in keys(M.proclocal)
_procget(M::SchedulerNodeMetadata, key::NodeMetaKey) = get(M.proclocal, askey(key), nothing)
_procdel(M::SchedulerNodeMetadata, key::NodeMetaKey) = (delete!(M.proclocal, askey(key)); nothing)
_procset(M::SchedulerNodeMetadata, key::NodeMetaKey, val) = (M.proclocal[askey(key)] = val; nothing)

function _has(M::SchedulerNodeMetadata, key::NodeMetaKey)
    txn = start(M.env)
    dbi = open(txn)
    try
        get(txn, dbi, askey(key), ATTR_PROPS[key.attr].get_type)
        return true
    catch
        return false
    finally
        commit(txn)
        close(M.env, dbi)
    end
end

_cached_has(M::SchedulerNodeMetadata, key::NodeMetaKey) = _prochas(M, key) || _has(M, key)

function _del(M::SchedulerNodeMetadata, key::NodeMetaKey)
    txn = start(M.env)
    dbi = open(txn)
    try
        delete!(txn, dbi, askey(key), C_NULL)
    catch ex
        rethrow(ex)
    finally
        commit(txn)
        close(M.env, dbi)
    end
    nothing
end

function _cached_del(M::SchedulerNodeMetadata, key::NodeMetaKey)
    _prochas(M, key) && _procdel(M, key)
    _del(M, key)
end

function _get(M::SchedulerNodeMetadata, key::NodeMetaKey)
    txn = start(M.env)
    dbi = open(txn)
    try
        fn = ATTR_PROPS[key.attr].get_processor
        T = ATTR_PROPS[key.attr].get_type
        return fn(get(txn, dbi, askey(key), T))
    catch ex
        return nothing
    finally
        commit(txn)
        close(M.env, dbi)
    end
end

function _cached_get(M::SchedulerNodeMetadata, key::NodeMetaKey)
    val = _procget(M, key)
    (val === nothing) && (val = _get(M, key))
    (val !== nothing) && _procset(M, key, val)
    val
end

function _set(M::SchedulerNodeMetadata, key::NodeMetaKey, val)
    txn = start(M.env)
    dbi = open(txn)
    try
        fn = ATTR_PROPS[key.attr].set_processor
        put!(txn, dbi, askey(key), fn(val))
    catch ex
        rethrow(ex)
    finally
        commit(txn)
        close(M.env, dbi)
    end
    nothing
end

function _cached_set(M::SchedulerNodeMetadata, key::NodeMetaKey, val)
    _procset(M, key, val)
    _set(M, key, val)
end

function decr_resultrefcount(M::SchedulerNodeMetadata, id::TaskIdType)
    txn = start(M.env)
    dbi = open(txn)
    key = NodeMetaKey(id,M_REFCOUNT)
    reskey = NodeMetaKey(id,M_RESULT)
    existing = UInt64(0)
    try
        fn = ATTR_PROPS[key.attr].get_processor
        T = ATTR_PROPS[key.attr].get_type
        existing = fn(get(txn, dbi, askey(key), T))
        (existing > 0) && (existing -= 1)
        if existing > 0
            fn = ATTR_PROPS[key.attr].set_processor
            put!(txn, dbi, askey(key), fn(existing))
        else
            delete!(txn, dbi, askey(key), C_NULL)
            delete!(txn, dbi, askey(reskey), C_NULL)
            _prochas(M, key) && _procdel(M, key)
            _prochas(M, reskey) && _procdel(M, reskey)
        end
    catch ex
        # ignore
    finally
        commit(txn)
        close(M.env, dbi)
    end
    existing
end

function has_result(M::SchedulerNodeMetadata, id::TaskIdType)
    _prochas(M, NodeMetaKey(id,M_RESULT)) && (return true)
    withlock(M.donetasks.lck) do
        return (id in M.donetasks)
    end
end

del_result(M::SchedulerNodeMetadata, id::TaskIdType)        = _cached_del(M, NodeMetaKey(id,M_RESULT))
get_result(M::SchedulerNodeMetadata, id::TaskIdType)        = _cached_get(M, NodeMetaKey(id,M_RESULT))
set_result(M::SchedulerNodeMetadata, id::TaskIdType, val)   = _procset(M, NodeMetaKey(id,M_RESULT), val)

function export_result(M::SchedulerNodeMetadata, id::TaskIdType, val, refcount::UInt64)
    key = NodeMetaKey(id,M_RESULT)
    refkey = NodeMetaKey(id,M_REFCOUNT)
    _procset(M, key, val)
    txn = start(M.env)
    dbi = open(txn)
    try
        fn = ATTR_PROPS[key.attr].set_processor
        put!(txn, dbi, askey(key), fn(val))
        reffn = ATTR_PROPS[refkey.attr].set_processor
        put!(txn, dbi, askey(refkey), reffn(refcount))
    catch ex
        rethrow(ex)
    finally
        commit(txn)
        close(M.env, dbi)
    end
    withlock(M.donetasks.lck) do
        push!(M.donetasks, id)
    end
    nothing
end

function export_local_result(M::SchedulerNodeMetadata, id::TaskIdType, t::Thunk, refcount::UInt64)
    key = NodeMetaKey(id,M_RESULT)

    _prochas(M, key) || return
    isalreadyexported = withlock(M.donetasks.lck) do
        id in M.donetasks
    end
    isalreadyexported && return

    val = _procget(M, key)
    if !isa(val, Chunk)
        val = Dagger.tochunk(val, persist = t.persist, cache = t.persist ? true : t.cache)
    end
    if isa(val.handle, DRef)
        val = chunktodisk(val)
    end

    export_result(M, id, val, refcount)
end