using CSV, DataFrames, Statistics

d = CSV.read(joinpath.("summaries", readdir("summaries")), DataFrame)


dsk = d[d.tech .== "dask",:]
dtb = d[d.tech .== "dtable", :]
dtf = d[d.tech .== "dataframesjl", :]

cmp = leftjoin(dtb, dsk, on=[:type, :n, :chunksize, :unique_vals],makeunique=true)
cmp = leftjoin(cmp, dtf, on=[:type, :n, :chunksize, :unique_vals],makeunique=true)
# dropmissing!(cmp)

g = groupby(cmp, :type)

fff = (a,b) -> begin 
    any(ismissing.([a,b])) && return missing
    -(b-a)/b*100
end

c = combine(g,
    [:time, :time_1] => ((t1,t2) -> mean(skipmissing(fff.(t2,t1)))) => :avg_times_faster_than_dask,
    [:time, :time_2] => ((t1,t2) -> mean(skipmissing(fff.(t2,t1)))) => :avg_times_faster_than_dataframes,
)

c.type = replace.(string.(c.type), "_" => "")
c = select(c, :type, [2,3] .=> (x-> round.(x; digits=1)).=> names(c)[2:3])

using Latexify
clipboard(latexify(c))

