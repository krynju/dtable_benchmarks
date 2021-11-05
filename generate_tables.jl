using CSV, DataFrames, Statistics

d = CSV.read(joinpath.("summary_benches", readdir("summary_benches")), DataFrame)


dsk = d[d.tech .== "dask",:]
dtb = d[d.tech .== "dtable", :]
dtf = d[d.tech .== "dataframesjl", :]

cmp = leftjoin(dtb, dsk, on=[:type, :n, :chunksize, :unique_vals],makeunique=true)
cmp = leftjoin(cmp, dtf, on=[:type, :n, :chunksize, :unique_vals],makeunique=true)
dropmissing!(cmp)

g = groupby(cmp, :type)

fff = (a,b) -> begin 
if b >= a 
    -b/a 
else a/b end
end

c = combine(g,
    [:time, :time_1] => ((t1,t2) -> mean(fff.(t2,t1))) => :avg_times_faster_than_dask,
    [:time, :time_2] => ((t1,t2) -> mean(fff.(t2,t1))) => :avg_times_faster_than_dataframes,
)

c.type = replace.(string.(c.type), "_" => "")
c = select(c, :type, [2,3] .=> (x-> round.(x; digits=1)).=> names(c)[2:3])

using Latexify
clipboard(latexify(c))