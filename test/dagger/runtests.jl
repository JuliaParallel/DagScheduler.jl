addprocs(2)

using Base.Test
using Dagger

@everywhere include("customsch.jl")

include("domain.jl")
include("array.jl")
Dagger.cleanup()
#include("cache.jl")
