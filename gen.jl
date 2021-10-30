using CSV, DataFrames, Tables, TableOperations, Plots

d = vcat(DataFrame.(CSV.File.("benches//" .* readdir("benches/")))...)

allchunksizes = combine(d, :chunksize => unique => :chunksize)
filter!(x -> x.chunksize != 0, allchunksizes)

dataframes_only = d[d.tech .== "dataframesjl", :]
a = repeat(dataframes_only, nrow(allchunksizes))
for (i, chunksize) in enumerate(allchunksizes.chunksize)
    a[((i-1)*nrow(dataframes_only)+1):i*nrow(dataframes_only), :chunksize] .= chunksize
end

d = vcat(d[d.tech .!= "dataframesjl", :], a)

# d = DataFrame(t)
sort!(d, :time)
d1 = combine(groupby(d, 1:6), first)
sort!(d1, [:tech, :n])

d2 = groupby(d1, [2,4,5,6])

mkpath("plots")
mkpath("summary_benches")
for k in keys(d2)
    println(k)
    name= "$(k.type)_chunksize$(string(k.chunksize))_uniquevals$(string(k.unique_vals))"

    p = d2[k]
    x = unique(p.n)
    techs = sort(combine(p, :tech => unique).tech_unique)
    ys = [p[p.tech .== t, [:n, :time]] for t in techs]
    @async CSV.write("summary_benches/"*name*".csv", p)
    plot()
    for (i, t) in enumerate(techs)
        s = ys[i]
        plot!(
            s.n,
            s.time./1e9,
            xscale=:log10,
            yscale=:log10,
            xticks=Int.([1e6, 1e7, 1e8, 1e9]),
            title=name,
            label=t,
            xlabel="n - single column length [Int32]",
            ylabel="time [s]",
            legend=:topleft,
            dpi=600
            )
    end
    savefig("plots/"*name*".png")

end