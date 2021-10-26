using DataFrames, Random, BenchmarkTools, OnlineStats, Dates



if length(ARGS) != 4
    n = 1_000_000
    max_chunksize = 0
    unique_values = Int32(1_000)
    ncolumns = 4
else
    n = tryparse(Int, ARGS[1])
    max_chunksize = tryparse(Int, ARGS[2])
    unique_values = tryparse(Int32, ARGS[3])
    ncolumns = tryparse(Int, ARGS[4])
end

tablesize = sizeof(Int32) * ncolumns * n / 1_000_000
println("tablesize $tablesize MB")

rng = MersenneTwister(1111)
d = DataFrame((;[Symbol("a$i") => abs.(rand(rng, Int32, n)) .% unique_values for i in 1:ncolumns]...))

filename = "dataframes_bench" * string(round(Int, Dates.datetime2unix(now()))) * ".csv"
file = open(filename, "w")
println("saving results to $filename")
write(file, "tech,type,n,chunksize,unique_vals,ncolumns,time,gctime,memory,allocs\n")

run_bench = (f, arg) -> begin
    @benchmark $f($arg) samples=2 evals=1 gcsample=true
end

w_test = (type, f, arg) -> begin
    b = run_bench(f, arg)
    m = minimum(b)
    s = "dataframesjl,$type,$n,$max_chunksize,$unique_values,$ncolumns,$(m.time),$(m.gctime),$(m.memory),$(m.allocs)\n"
    write(file, s)
    flush(file)
    println("done $type")
end


fmap = (d) -> begin
    select(d, :a1 => (x) -> x.+1)
end
w_test("increment_map", fmap, d)

ffilter = (d) -> begin
    f = filter(row -> row.a1 < unique_values ÷ 2, d)
end
w_test("filter_half", ffilter, d)


fredall = (d) -> begin
    combine(d, propertynames(d) .=> var)
end
w_test("reduce_var_all", fredall, d)


fredsingle = (d) -> begin
    combine(d, :a1 => var)
end
w_test("reduce_var_single", fredsingle, d)



groupby_reduce_mean_all = (d) -> begin
    combine(groupby(d, :a1), propertynames(d) .=> mean)
end
w_test("groupby_reduce_mean_all", groupby_reduce_mean_all, d)



groupby_single_col = (d) -> begin
    groupby(d, :a1)
end
w_test("groupby_single_col", groupby_single_col, d)


################
# grouped prep
g = groupby(d, :a1)
d = nothing
GC.gc();GC.gc();
################


grouped_reduce_mean_singlecol = (g) -> begin
    r = combine(g, :a2 => mean)
end
w_test("grouped_reduce_mean_singlecol", grouped_reduce_mean_singlecol, g)


grouped_reduce_mean_allcols = (g) -> begin
    combine(g, names(g) .=> mean)
end
w_test("grouped_reduce_mean_allcols", grouped_reduce_mean_allcols, g)


close(file)