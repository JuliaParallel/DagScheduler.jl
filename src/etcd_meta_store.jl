# scheduler metadata store

"""
Metadata store using etcd.
"""
mutable struct EtcdSchedMeta <: SchedMeta
    path::String
    server::String
    port::UInt
    cli::Etcd.Client
    start_index::Int
    proclocal::Dict{String,Any}
    watches::Dict{String,Task}
    tasklist::Vector{Pair{String,TaskIdType}}
    sharedtasks::Set{TaskIdType}
    donetasks::Set{TaskIdType}
    sharemode::ShareMode
    trigger::Channel{Void}
    add_annotation::Function
    del_annotation::Function

    function EtcdSchedMeta(path::String, sharethreshold::Int)
        server = "127.0.0.1"
        port = UInt(2379)
        # TODO: take a list of failover servers, reconnect on disconnection
        cli = Etcd.connect(server, Int(port), "v2")
        start_index = _determine_start_index(cli, path)

        new(path, server, port, cli, start_index,
            Dict{String,Any}(),
            Dict{String,Task}(),
            Vector{Pair{String,TaskIdType}}(),
            Set{TaskIdType}(),
            Set{TaskIdType}(),
            ShareMode(sharethreshold),
            Channel{Void}(1024),
            identity, identity)
    end
end

function Base.show(io::IO, M::EtcdSchedMeta)
    print(io, "EtcdSchedMeta(", M.path, ")")
end

function init(M::EtcdSchedMeta, brokerid::String; add_annotation=identity, del_annotation=identity)
    M.start_index = _determine_start_index(M.cli, M.path)
    M.add_annotation = add_annotation
    M.del_annotation = del_annotation
    _track_results(M)
    _track_shared_tasks(M, brokerid)
    nothing
end

function _determine_start_index(cli::Etcd.Client, path::String)
    try
        res = createdir(cli, path)
        res["node"]["modifiedIndex"]
    catch ex
        (isa(ex, EtcdError) && (ex.resp["errorCode"] == 105)) || rethrow(ex)
        res = get(cli, path)
        res["node"]["modifiedIndex"]
    end
end

function wait_trigger(M::EtcdSchedMeta; timeoutsec::Int=5)
    if !isready(M.trigger)
        @schedule begin
            i1 = M.start_index
            sleep(timeoutsec)
            (M.start_index == i1) && !isready(M.trigger) && put!(M.trigger, nothing)
        end
    end
    take!(M.trigger)
end

function delete!(M::EtcdSchedMeta)
    reset(M)
    try
        Etcd.deletedir(M.cli, M.path, recursive=true)
    end
    nothing
end

function reset(M::EtcdSchedMeta)
    for (key,task) in M.watches
        timedwait(()->istaskdone(task), 5.)
        if !istaskdone(task)
            try
                schedule(task, InterruptException(); error=true)
                wait(task)
            end
        end
    end
    reset(M.sharemode)
    empty!(M.proclocal)
    empty!(M.watches)
    empty!(M.tasklist)
    empty!(M.donetasks)
    empty!(M.sharedtasks)
    M.add_annotation = identity
    M.del_annotation = identity
    while isready(M.trigger)
        take!(M.trigger)
    end
    nothing
end

function share_task(M::EtcdSchedMeta, brokerid::String, id::TaskIdType)
    s = sharepath(M, id)
    last_index = M.start_index
    try
        Etcd.create(M.cli, s, "")
        annotated = M.add_annotation(id)
        k = taskpath(M, brokerid)
        set(M.cli, k, string(annotated); ordered=true)
    catch ex
        (isa(ex, EtcdError) && (ex.resp["errorCode"] == 105)) || rethrow(ex)
    end

    nothing
end

function _get_shared_tasks(M::EtcdSchedMeta, brokerid::String)
    k = taskpath(M, brokerid)
    last_index = M.start_index
    res = Vector{Pair{String,TaskIdType}}()

    try
        resp = get(M.cli, k)
        last_index = get(resp, "modifiedIndex", get(resp, "createdIndex", M.start_index))

        if "node" in keys(resp)
            last_index = max(last_index, get(resp, "modifiedIndex", get(resp, "createdIndex", M.start_index)))
            if "nodes" in keys(resp["node"])
                for node in resp["node"]["nodes"]
                    push!(res, node["key"] => parse(TaskIdType, node["value"]))
                    last_index = max(last_index, get(node, "modifiedIndex", get(node, "createdIndex", M.start_index)))
                end
            end
        end
    catch ex
        (isa(ex, EtcdError) && (ex.resp["errorCode"] == 100)) || rethrow(ex)
    end
    res, last_index
end

function _remove_from_tasklist(tasklist::Vector{Pair{String,TaskIdType}}, tpath::String)
    filter!((nv)->(nv[1] != tpath), tasklist)
    nothing
end

function steal_task(M::EtcdSchedMeta, brokerid::String, tasklist::Vector{Pair{String,TaskIdType}}=M.tasklist)
    while !isempty(tasklist)
        tpath, taskid = tasklist[1]
        try
            resp = delete(M.cli, tpath)
            last_index = resp["node"]["modifiedIndex"]
            taskid = M.del_annotation(taskid)
            _remove_from_tasklist(tasklist, tpath)
            return taskid
        catch ex
            if isa(ex, EtcdError) && (ex.resp["errorCode"] == 100)
                _remove_from_tasklist(tasklist, tpath)
            else
                rethrow(ex)
            end
        end
    end
    NoTask
end

function _watchloop_end_cond(M::EtcdSchedMeta, resp)
    (resp["action"] == "delete") && (resp["node"]["key"] == M.path)
end

function _wait_shared_task(f::Function, M::EtcdSchedMeta, brokerid::String; wait_index::Int=(M.start_index+1))
    _purge_completed_watches(M)
    k = taskpath(M, brokerid)

    M.watches[k] = watchloop(M.cli, k, (resp)->_watchloop_end_cond(M, resp); recursive=true, wait_index=wait_index) do resp
        respnode = resp["node"]
        action = resp["action"]
        key = respnode["key"]
        val = parse(TaskIdType, get(respnode, "value", "0"))
        f(action, key, val)
    end
    nothing
end

function _track_results(M::EtcdSchedMeta; wait_index::Int=(M.start_index+1))
    _purge_completed_watches(M)
    k = resultroot(M)

    M.watches[k] = watchloop(M.cli, k, (resp)->_watchloop_end_cond(M, resp); recursive=true, wait_index=wait_index) do resp
        action = resp["action"]
        if action == "set" && ("node" in keys(resp)) && ("value" in keys(resp["node"]))
            val, refcount = meta_deser(resp["node"]["value"])
            key = resp["node"]["key"]
            id = parse(TaskIdType, basename(key))
            set_result(M, id, val)
            isready(M.trigger) || put!(M.trigger, nothing)
        end
    end
    nothing
end

function _create_delete_shared_task(M::EtcdSchedMeta, action::String, key::String, val::TaskIdType)
    if key != M.path
        if action == "create"
            taskid = M.del_annotation(val)
            if !(taskid in M.sharedtasks)
                elem = key=>val
                if !(elem in M.tasklist)
                    push!(M.tasklist, key=>val)
                    M.sharemode.ncreated += 1
                    M.sharemode.nshared += 1
                end
                push!(M.sharedtasks, taskid)
            end
        elseif action == "delete"
            _remove_from_tasklist(M.tasklist, key)
            M.sharemode.ndeleted += 1
            M.sharemode.nshared -= 1
        end
    end
    isready(M.trigger) || put!(M.trigger, nothing)
    nothing
end

function _track_shared_tasks(M::EtcdSchedMeta, brokerid::String)
    # get the contents one time
    tasks, last_index = _get_shared_tasks(M, brokerid)
    for (key,val) in tasks
        _create_delete_shared_task(M, "create", key, val)
    end

    # start watch from the last index
    _wait_shared_task(M, brokerid; wait_index=(last_index+1)) do action, key, val
        _create_delete_shared_task(M, action, key, val)
    end
end

function set_result(M::EtcdSchedMeta, id::TaskIdType, val; refcount::UInt64=UInt64(1), processlocal::Bool=true)
    k = resultpath(M, id)
    M.proclocal[k] = val
    processlocal || set(M.cli, k, meta_ser((val,refcount)))
    push!(M.donetasks, id)
    nothing
end

function get_result(M::EtcdSchedMeta, id::TaskIdType)
    k = resultpath(M, id)
    if k in keys(M.proclocal)
        M.proclocal[k]
    else
        resp = get(M.cli, k)
        val, refcount = meta_deser(resp["node"]["value"])
        M.proclocal[k] = val
        val
    end
end

function has_result(M::EtcdSchedMeta, id::TaskIdType) #; recheck::Bool=false)
    id in M.donetasks
end

function decr_result_ref(M::EtcdSchedMeta, id::TaskIdType)
    k = resultpath(M, id)
    while true
        resp = get(M.cli, k)
        val, refcount = meta_deser(resp["node"]["value"])
        refcount -= 1

        if refcount == 0
            delete(M.cli, k)
            return
        else
            try
                prev_index = resp["node"]["modifiedIndex"]
                cas(M.cli, k, meta_ser((val, refcount)); prev_index=prev_index)
                return
            catch ex
                (isa(ex, EtcdError) && (ex.resp["errorCode"] == 101)) || rethrow(ex)
            end
        end
    end
end

function _purge_completed_watches(M::EtcdSchedMeta)
    #=
    for (n,v) in M.watches
        if istaskdone(v)
            if v.exception === nothing
                println(n, " watcher task is done")
            else
                println(n, " watcher task is done with error ", v.exception)
            end
        end
    end
    =#
    filter!((n,v)->!istaskdone(v), M.watches)
end

function export_local_result(M::EtcdSchedMeta, id::TaskIdType, executable, refcount::UInt64)
    k = resultpath(M, id)
    (k in keys(M.proclocal)) || return
    exists(M.cli, k) && return

    val = repurpose_result_to_export(executable, M.proclocal[k])
    set(M.cli, k, meta_ser((val,refcount)))
    nothing
end
