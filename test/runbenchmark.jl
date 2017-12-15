include("benchmarks/sort/compare.jl")
isdir(".mempool") && rm(".mempool"; recursive=true)
include("benchmarks/deepdag/compare.jl")
isdir(".mempool") && rm(".mempool"; recursive=true)
