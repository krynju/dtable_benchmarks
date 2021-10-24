using Dagger, Random, BenchmarkTools, OnlineStats, Dates



if length(ARGS) != 4
    n = 400_000_000
    max_chunksize = 1_000_000
    unique_values = 10_00
    ncolumns = 4
else
    n = tryparse(Int, ARGS[1])
    max_chunksize = tryparse(Int, ARGS[2])
    unique_values = tryparse(Int, ARGS[3])
    ncolumns = tryparse(Int, ARGS[4])
end

tablesize = sizeof(Int32) * ncolumns * n / 1_000_000
println("tablesize $tablesize MB")
# data = (;[Symbol("a$i") => abs.(rand(rng,Int64, n)).%10_00 for i in 1:10]...)
# f = () -> while true sleep(0.2); println(length(Dagger.Sch.EAGER_STATE.x.running)) end
Dagger.@spawn 10+10
#@async f()
# create

rng = MersenneTwister(1111)
d = DTable((;[Symbol("a$i") => abs.(rand(rng, Int32, n)) .% unique_values for i in 1:ncolumns]...), max_chunksize)

filename = "dtable_bench" * string(round(Int, Dates.datetime2unix(now()))) * ".csv"
file = open(filename, "w")
println("saving results to $filename")
write(file, "tech,type,n,chunksize,unique_vals,ncolumns,time,gctime,memory,allocs\n")

run_bench = (f, arg) -> begin
    @benchmark $f($arg) samples=2 evals=1 gcsample=true
end

w_test = (type, f, arg) -> begin
    b = run_bench(f, arg)
    m = minimum(b)
    s = "dtable,$type,$n,$max_chunksize,$unique_values,$ncolumns,$(m.time),$(m.gctime),$(m.memory),$(m.allocs)\n"
    write(file, s)
    flush(file)
    println("done $type")
end


fmap = (d) -> begin
    m = map(row -> (r = row.a1 + 1,), d)
    wait.(m.chunks)
end
w_test("increment_map", fmap, d)

ffilter = (d) -> begin
    f = filter(row -> row.a1 < unique_values ÷ 2, d)
    wait.(f.chunks)
end
w_test("filter_half", ffilter, d)


fredall = (d) -> begin
    r = reduce(fit!, d, init=Variance())
    fetch(r)
end
w_test("reduce_var_all", fredall, d)


fredsingle = (d) -> begin
    r = reduce(fit!, d, cols=[:a1], init=Variance())
    fetch(r)
end
w_test("reduce_var_single", fredsingle, d)



groupby_reduce_mean_all = (d) -> begin
    _g = Dagger.groupby(d, :a1)
    r = reduce(fit!, _g, init=Mean())
    fetch(r)
end
w_test("groupby_reduce_mean_all", groupby_reduce_mean_all, d)



groupby_single_col = (d) -> begin
    g = Dagger.groupby(d, :a1)
    (x -> x isa Dagger.EagerThunk && wait(x)).(g.dtable.chunks)
end
w_test("groupby_single_col", groupby_single_col, d)


################
# grouped prep
d = nothing
rng = MersenneTwister(1111)
g = Dagger.groupby(DTable((;[Symbol("a$i") => abs.(rand(rng, Int32, n)) .% unique_values for i in 1:ncolumns]...), max_chunksize), :a1)

GC.gc();GC.gc();
################


grouped_reduce_mean_singlecol = (g) -> begin
    r = reduce(fit!, g, cols=[:a2], init=Mean())
    fetch(r)
end
w_test("grouped_reduce_mean_singlecol", grouped_reduce_mean_singlecol, g)


grouped_reduce_mean_allcols = (g) -> begin
    r = reduce(fit!, g, init=Mean())
    fetch(r)
end
w_test("grouped_reduce_mean_allcols", grouped_reduce_mean_allcols, g)


close(file)