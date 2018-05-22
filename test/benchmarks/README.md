Some benchmark results:

Environment:
```
julia> versioninfo()
Julia Version 0.6.2
Commit d386e40c17 (2017-12-13 18:08 UTC)
Platform Info:
  OS: Linux (x86_64-pc-linux-gnu)
  CPU: Intel(R) Core(TM) i7-4700MQ CPU @ 2.40GHz
  WORD_SIZE: 64
  BLAS: libopenblas (USE64BITINT DYNAMIC_ARCH NO_AFFINITY Haswell)
  LAPACK: libopenblas64_
  LIBM: libopenlibm
  LLVM: libLLVM-3.9.1 (ORCJIT, haswell)

julia> Base.Sys.cpu_info()
8-element Array{Base.Sys.CPUinfo,1}:
 Intel(R) Core(TM) i7-4700MQ CPU @ 2.40GHz: 
        speed         user         nice          sys         idle          irq
     2394 MHz     149596 s        102 s      32642 s    1570358 s          0 s
 Intel(R) Core(TM) i7-4700MQ CPU @ 2.40GHz: 
        speed         user         nice          sys         idle          irq
     2394 MHz     115010 s        162 s      24770 s     328574 s          0 s
 Intel(R) Core(TM) i7-4700MQ CPU @ 2.40GHz: 
        speed         user         nice          sys         idle          irq
     2394 MHz     151723 s         99 s      31186 s     310437 s          0 s
 Intel(R) Core(TM) i7-4700MQ CPU @ 2.40GHz: 
        speed         user         nice          sys         idle          irq
     2394 MHz     115159 s        171 s      19987 s     333978 s          0 s
 Intel(R) Core(TM) i7-4700MQ CPU @ 2.40GHz: 
        speed         user         nice          sys         idle          irq
     2394 MHz     172781 s        101 s      43952 s     311635 s          0 s
 Intel(R) Core(TM) i7-4700MQ CPU @ 2.40GHz: 
        speed         user         nice          sys         idle          irq
     2394 MHz     119378 s        152 s      21120 s     331955 s          0 s
 Intel(R) Core(TM) i7-4700MQ CPU @ 2.40GHz: 
        speed         user         nice          sys         idle          irq
     2394 MHz     151358 s        109 s      31853 s     313440 s          0 s
 Intel(R) Core(TM) i7-4700MQ CPU @ 2.40GHz: 
        speed         user         nice          sys         idle          irq
     2394 MHz     114616 s        161 s      25374 s     332725 s          0 s

julia> Base.Sys.total_memory()
0x00000003e5055000
```

```
Running sort comparison
Node Meta: DagScheduler.ShmemMeta.ShmemExecutorMeta
Cluster Meta: DagScheduler.SimpleMeta.SimpleExecutorMeta
- dagger.jl
  345.036 ms (31319 allocations: 1.57 MiB)
- dagscheduler.jl
  45.869 ms (15915 allocations: 1.05 MiB)
- dagscheduler_2node.jl
  54.092 ms (16783 allocations: 845.92 KiB)
Running sort comparison with distributed results
Node Meta: DagScheduler.ShmemMeta.ShmemExecutorMeta
Cluster Meta: DagScheduler.SimpleMeta.SimpleExecutorMeta
- dagger.jl
  2.459 s (1357843 allocations: 73.66 MiB)
- dagscheduler.jl
  378.567 ms (323169 allocations: 18.09 MiB)
- dagscheduler_1node.jl
  548.678 ms (280794 allocations: 15.60 MiB)
- dagscheduler_2node.jl
  623.153 ms (541577 allocations: 28.58 MiB)
Running deep dag comparison
Node Meta: DagScheduler.ShmemMeta.ShmemExecutorMeta
Cluster Meta: DagScheduler.SimpleMeta.SimpleExecutorMeta
- dagger.jl
  711.763 ms (2328546 allocations: 270.96 MiB)
- dagscheduler.jl
  222.438 ms (228670 allocations: 16.63 MiB)
- dagscheduler_2node.jl
  229.976 ms (125307 allocations: 5.84 MiB)
```
