include("daggen.jl")

using GraphViz
using Dagger
import Dagger: Thunk, Chunk, istask, dependents

# generate the dag that was executed here
const dag = gen_sort_dag(10^6, 40, 4, 40)
const execution_order = [
# fill in the execution order from actual run output
]
const colors = ["black", "red", "green", "blue", "cyan", "orange", "purple", "brown", "yellow"]

function label_attribs(t::Thunk)
    order = 1
    node = 1
    for idx in 1:length(execution_order)
        p = execution_order[idx]
        if p.first == t.id
            order = idx
            node = p.second
            break
        end
    end
    grayscale = 50 + floor(Int, order*49/length(execution_order))
    color = colors[node]

     """ color="$color" style="filled" fillcolor="gray$grayscale" penwidth="5" """
end

function node_label(io, t::Thunk, c)
    if isa(t.f, Function)
        println(io, t.id, " [label=\"", t.f, " - ", t.id, "\"", label_attribs(t), "]")
    else
        println(io, t.id, " [label=\"fn - ", t.id, "\"", label.attribs(t), "]")
    end
    c
end

function node_label(io, t,c)
    l = replace(string(t), "\"", "")
    println(io, dec(hash(t)), " [label=\"$l\"]")
end

global _part_labels = Dict()

function node_label(io, t::Chunk, c)
    _part_labels[t]="part_$c"
    c+1
end

function node_id(t::Thunk)
    t.id
end

function node_id(t)
    dec(hash(t))
end

function node_id(t::Chunk)
    _part_labels[t]
end

function write_dag(io, t)
    !istask(t) && return
    deps = dependents(t)
    c=1
    for k in keys(deps)
        c = node_label(io, k, c)
    end
    for (k, v) in deps
        for dep in v
            if isa(k, Union{Chunk, Thunk})
                println(io, "$(node_id(k)) -> $(node_id(dep))")
            end
        end
    end
end

function viz(t::Thunk)
    io = IOBuffer()
    write_dag(io, t)
    """digraph {
        graph [layout=dot, rankdir=TB];
        $(String(take!(io)))
    }"""
end

graph = viz(dag)
dotfile = joinpath(pwd(), "viz.dot")
htmlfile = joinpath(pwd(), "viz.html")
svgfile = joinpath(pwd(), "viz.svg")
pngfile = joinpath(pwd(), "viz.png")
open(dotfile, "w") do io
    println(io, graph)
end
open(htmlfile, "w") do io
    iob = IOBuffer()
    show(iob, MIME"image/svg+xml"(), Graph(graph))
    svgstr = String(take!(iob))
    svgstart = searchindex(svgstr, "<g id=\"graph0\"")
    svgend = searchindex(svgstr, "</svg>") - 1
    svgstr = svgstr[svgstart:svgend]

    print(io, """<html>
    <head>
        <script src="https://cdnjs.cloudflare.com/ajax/libs/svg.js/2.6.4/svg.js">
        </script>
        <script>
            var G;
            function showgraph() {
                var draw = SVG('dag').size('100%', '100%').viewbox(0,0,800,1000);
                G = draw.svg(`""")
    print(io, strip(svgstr))
    print(io, """`)
            };
            function onrescale() {
                alert(G);
                var g = SVG.select('graph')
            };
        </script>
    </head>
    <body onload="showgraph()">
    <input id="horzscale" type="range" min="-100" max="5" style="width: 800px" onchange="onrescale();"><br>
    <input id="vertscale" type="range" min="-10" max="1" style="width: 800px" onchange="onrescale();"><br>
    <div id="dag"></div>
    </body></html>""")
end
open(svgfile, "w") do io
    show(io, MIME"image/svg+xml"(), Graph(graph))
end
#=
open(pngfile, "w") do io
    show(io, MIME"image/png"(), Graph(graph))
end
=#
#dotbin = joinpath(Pkg.dir("GraphViz"), "deps", "usr", "bin", "dot")
#run(`$dotbin $dotfile -Tsvg -o $pngfile`)
