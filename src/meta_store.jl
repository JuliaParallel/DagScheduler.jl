# scheduler metadata store
# -------------------------
# used to share metadata among processes in a node
# - task executable
# - task executor
# - result if execution is complete
const M_EXECUTABLE = UInt8(1)
const M_EXECUTOR = UInt8(2)
const M_RESULT = UInt8(3)

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
    NodeAttrProp(identity, identity, UInt64),
    NodeAttrProp(meta_ser, meta_deser, Vector{UInt8})
]

struct SchedulerNodeMetadata
    dbpath::String
    env::Environment
    cache::Dict{UInt64,Any}

    function SchedulerNodeMetadata(path::String; ndbs::Int=1, nreaders::Int=1, mapsz::Int=1000^3)
        isdir(path) || mkdir(path)
        env = LMDB.create()

        # we want it to be sync, since we are using across processes
        isflagset(env[:Flags], Cuint(LMDB.NOSYNC)) && unset!(env, LMDB.NOSYNC)

        env[:Readers] = nreaders
        env[:MapSize] = mapsz
        env[:DBs] = ndbs

        open(env, path)
        new(path, env, Dict{UInt64,Any}())
    end
end

close(M::SchedulerNodeMetadata) = close(M.env)
function delete!(M::SchedulerNodeMetadata)
    txn = start(M.env)
    dbi = open(txn)
    drop(txn, dbi; delete=true)
    commit(txn)
    close(M.env, dbi)
    close(M)
    rm(M.dbpath; recursive=true)
    nothing
end

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

function _cond_set(M::SchedulerNodeMetadata, key::NodeMetaKey, val, cond::Function, update_only::Bool)
    txn = start(M.env)
    dbi = open(txn)
    updated = false
    try
        fn = ATTR_PROPS[key.attr].get_processor
        T = ATTR_PROPS[key.attr].get_type
        existing = fn(get(txn, dbi, askey(key), T))

        if cond(existing)
            if existing != val
                fn = ATTR_PROPS[key.attr].set_processor
                put!(txn, dbi, askey(key), fn(val))
            end
            updated = true
        end
        commit(txn)
        close(M.env, dbi)
        return updated
    catch ex
        try
            fn = ATTR_PROPS[key.attr].set_processor
            put!(txn, dbi, askey(key), fn(val))
            updated = true
        finally
            commit(txn)
            close(M.env, dbi)
        end
    end

    updated
end

has_executable(M::SchedulerNodeMetadata, id::TaskIdType)                = _has(M, NodeMetaKey(id,M_EXECUTABLE))
has_result(M::SchedulerNodeMetadata, id::TaskIdType)                    = _has(M, NodeMetaKey(id,M_RESULT))
has_executor(M::SchedulerNodeMetadata, id::TaskIdType)                  = _has(M, NodeMetaKey(id,M_EXECUTOR))

del_executable(M::SchedulerNodeMetadata, id::TaskIdType)                = _del(M, NodeMetaKey(id,M_EXECUTABLE))
del_result(M::SchedulerNodeMetadata, id::TaskIdType)                    = _del(M, NodeMetaKey(id,M_RESULT))
del_executor(M::SchedulerNodeMetadata, id::TaskIdType)                  = _del(M, NodeMetaKey(id,M_EXECUTOR))

get_executable(M::SchedulerNodeMetadata, id::TaskIdType)                = (id in keys(M.cache)) ? M.cache[id] : _get(M, NodeMetaKey(id,M_EXECUTABLE))
get_result(M::SchedulerNodeMetadata, id::TaskIdType)                    = _get(M, NodeMetaKey(id,M_RESULT))
get_executor(M::SchedulerNodeMetadata, id::TaskIdType)                  = _get(M, NodeMetaKey(id,M_EXECUTOR))::Union{UInt64,Void}

set_executable(M::SchedulerNodeMetadata, id::TaskIdType, val)           = _set(M, NodeMetaKey(id,M_EXECUTABLE), val)
set_result(M::SchedulerNodeMetadata, id::TaskIdType, val)               = _set(M, NodeMetaKey(id,M_RESULT), val)
set_executor(M::SchedulerNodeMetadata, id::TaskIdType, val::UInt64)     = _set(M, NodeMetaKey(id,M_EXECUTOR), val)

cond_set_executable(M::SchedulerNodeMetadata, id::TaskIdType, val, cond::Function, update_only::Bool)   = _cond_set(M, NodeMetaKey(id,M_EXECUTABLE), val, cond, update_only)
cond_set_result(M::SchedulerNodeMetadata, id::TaskIdType, val, cond::Function, update_only::Bool)       = _cond_set(M, NodeMetaKey(id,M_RESULT), val, cond, update_only)
cond_set_executor(M::SchedulerNodeMetadata, id::TaskIdType, val, cond::Function, update_only::Bool)     = _cond_set(M, NodeMetaKey(id,M_EXECUTOR), val, cond, update_only)
