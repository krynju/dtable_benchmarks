@everywhere using Random, BenchmarkTools, OnlineStats, Dates

if length(ARGS) != 4
    n = 1_000_000
    max_chunksize = 1_000_000
    unique_values = Int32(1000)
    ncolumns = 4
else
    n = tryparse(Int, ARGS[1])
    max_chunksize = tryparse(Int, ARGS[2])
    unique_values = tryparse(Int32, ARGS[3])
    ncolumns = tryparse(Int, ARGS[4])
end

tablesize = sizeof(Int32) * ncolumns * n / 1_000_000
println("tablesize $tablesize MB")


filename = filename_prefix * string(round(Int, Dates.datetime2unix(now()))) * ".csv"
file = open(filename, "w")
println("saving results to $filename")
write(file, "tech,type,n,chunksize,unique_vals,ncolumns,time,gctime,memory,allocs\n")


run_bench = (f, arg, s) -> begin
    @benchmark $f($arg) samples=s evals=1 gcsample=true
end

_gc = () -> begin
    for i in 1:4
        GC.gc()
    end
end

w_test = (type, f, arg; s=2) -> begin
    b = run_bench(f, arg, s)
    m = minimum(b)
    s = "dtable,$type,$n,$max_chunksize,$unique_values,$ncolumns,$(m.time),$(m.gctime),$(m.memory),$(m.allocs)\n"
    write(file, s)
    flush(file)
    println("done $type")
    _gc()
    b
end

rng = MersenneTwister(1111)
data = (;[Symbol("a$i") => abs.(rand(rng, Int32, n)) .% unique_values for i in 1:ncolumns]...)
