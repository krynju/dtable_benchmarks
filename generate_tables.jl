using CSV, DataFrames, Statistics

d = CSV.read(joinpath.("summary_benches", readdir("summary_benches")), DataFrame)


dsk = d[d.tech .== "dask",:]
dtb = d[d.tech .== "dtable", :]
dtf = d[d.tech .== "dataframesjl", :]

cmp = leftjoin(dtb, dsk, on=[:type, :n, :chunksize, :unique_vals],makeunique=true)
cmp = leftjoin(cmp, dtf, on=[:type, :n, :chunksize, :unique_vals],makeunique=true)
dropmissing!(cmp)

g = groupby(cmp, :type)

c = combine(g,
    [:time, :time_1] => ((t1,t2) -> mean(t2./t1)) => :avg_times_faster_than_dask,
    [:time, :time_2] => ((t1,t2) -> mean(t2./t1)) => :avg_times_faster_than_dataframes,
)

c.type = replace.(string.(c.type), "_" => "")
c = select(c, :type, [2,3] .=> (x-> round.(x; digits=3)).=> names(c)[2:3])

using Latexify
clipboard(latexify(c))