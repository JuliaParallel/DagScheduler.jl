using Dagger
Dagger.use_shared_array[] = false

f = i -> (i+1)
g = (i, j) -> (i + j)
h = (i, j, k) -> floor(Int, mean([i,j,k]))

const T = Any[Thunk(()->10), Thunk(()->10), Thunk(()->10)]

function gendag(U,V,W)
    fd = delayed(f)(U)
    fg = delayed(g)(fd, V)
    fh = delayed(h)(fd, fg, W)
    fh
end

function gendag(T)
    f1 = gendag(T[end-2:end]...)
    f2 = gendag(T[end-2:end]...)
    f3 = gendag(T[end-2:end]...)
    push!(T, f1)
    push!(T, f2)
    push!(T, f3)
    gendag(f1, f2, f3)
end

function pushthunk(t, a = Any[])
   isa(t, Thunk) && map(x->pushthunk(x, a), t.inputs)
   (t in a) || push!(a, t)
   a
end

function gen_cross_dag()
    x = gendag(T)
    for idx in 1:5
        x = gendag(T)
    end
    x
end

function gen_straight_dag(inp)
    nextinp = []
    if length(inp) == 6
        push!(nextinp, delayed(f)(inp[1]))
        push!(nextinp, delayed(g)(inp[2], inp[3]))
        push!(nextinp, delayed(h)(inp[4], inp[5], inp[6]))
        return delayed(h)(nextinp...)
    else
        for idx in 1:6:length(inp)
            f1 = delayed(f)(inp[idx])
            f2 = delayed(g)(inp[idx+1], inp[idx+2])
            f3 = delayed(h)(inp[idx+3], inp[idx+4], inp[idx+5])
            push!(nextinp, delayed(h)(f1, f2, f3))
        end
        return gen_straight_dag(nextinp)
    end
end

#const deepdag = gen_straight_dag(T)
#const ordereddag = pushthunk(deepdag)

import Dagger: dsort_chunks

function gen_sort_dag(totalsz=10^6, nchunks=40, batchsize=4)
    cs = compute(rand(Blocks(ceil(Int,totalsz/nchunks)), totalsz)).chunks
    lt = Base.isless
    by = identity
    rev = false
    ord = Base.Sort.ord(lt,by,rev,Dagger.default_ord)

    dsort_chunks(cs, 1, batchsize=batchsize, merge=(x,y)->Dagger.merge_sorted(ord, x, y))[1]
end
