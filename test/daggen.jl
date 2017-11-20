using Dagger

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

function gendeepdag(T)
    x = gendag(T)
    for idx in 1:5
        x = gendag(T)
    end
    x
end

#const deepdag = gendeepdag(T)
#const ordereddag = pushthunk(deepdag)

import Dagger: dsort_chunks

function gen_sort_dag()
    cs = compute(rand(Blocks(10), 1000)).chunks
       lt=Base.isless
       by=identity
       rev=false
    ord = Base.Sort.ord(lt,by,rev,Dagger.default_ord)

    dsort_chunks(cs, 1, batchsize=4, merge=(x,y)->Dagger.merge_sorted(ord, x, y))[1]
end
