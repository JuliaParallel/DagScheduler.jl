struct BcastChannel{T}
    recepients::Set{RemoteChannel{Channel{T}}}

    function BcastChannel{T}() where T
        new(Set{RemoteChannel{Channel{T}}}())
    end
end

function register{T}(bcast::BcastChannel{T})
    remote = RemoteChannel(()->Channel{T}(1024))
    push!(bcast.recepients, remote)
    remote
end

function deregister{T}(bcast::BcastChannel{T}, remote::RemoteChannel{Channel{T}})
    pop!(bcast.recepients, remote)
    nothing
end

function Base.put!{T}(bcast::BcastChannel{T}, val::T)
    @sync for recepient in bcast.recepients
        @async put!(recepient, val)
    end
    nothing
end

function Base.put!{T}(bcast::BcastChannel{T}, val::T, cond)
    @sync for recepient in bcast.recepients
        cond(recepient, val) && @async put!(recepient, val)
    end
    nothing
end

function deregister{T}(bcast::BcastChannel{T})
    empty!(bcast.recepients)
    nothing
end
