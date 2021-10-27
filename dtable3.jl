using Dagger, Random, BenchmarkTools, OnlineStats, Dates



if length(ARGS) != 4
    n = 400_000_000
    max_chunksize = 1_000_000
    unique_values = Int32(10_00)
    ncolumns = 4
else
    n = tryparse(Int, ARGS[1])
    max_chunksize = tryparse(Int, ARGS[2])
    unique_values = tryparse(Int32, ARGS[3])
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
data = (;[Symbol("a$i") => abs.(rand(rng, Int32, n)) .% unique_values for i in 1:ncolumns]...)
d = DTable(data, max_chunksize)
data = nothing

filename = "dtable_bench" * string(round(Int, Dates.datetime2unix(now()))) * ".csv"
file = open(filename, "w")
println("saving results to $filename")
write(file, "tech,type,n,chunksize,unique_vals,ncolumns,time,gctime,memory,allocs\n")

_gc = () -> begin
    for i in 1:10
        Dagger.@spawn 10+10
    end
    for i in 1:4
        GC.gc()
    end
end

run_bench = (f, arg) -> begin
    @benchmark $f($arg) samples=2 evals=1 gcsample=true
end

w_test = (type, f, arg) -> begin
    b = run_bench(f, arg)
    m = minimum(b)
    s = "dtable,$type,$n,$max_chunksize,$unique_values,$ncolumns,$(m.time),$(m.gctime),$(m.memory),$(m.allocs)\n"
    write(file, s)
    flush(file)
    _gc()
    println("done $type")
end

groupby_single_col = (d) -> begin
    g = Dagger.groupby(d, :a1)
    (x -> x isa Dagger.EagerThunk && wait(x)).(g.dtable.chunks)
end
w_test("groupby_single_col", groupby_single_col, d)




close(file)