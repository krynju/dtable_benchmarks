using CSV, DataFrames, Tables, TableOperations, Plots

d = vcat(DataFrame.(CSV.File.("benches//" .* readdir("benches/")))...)

# d = DataFrame(t)

d1 = combine(groupby(d, 1:6), propertynames(d)[7:10] .=> minimum .=> propertynames(d)[7:10])

d2 = groupby(d1, [2,6])

keys(d2)

for k in keys(d2)
    println(k)
    # name= "$(k.type)_csize$(string(k.chunksize))_uvals$(string(k.unique_vals))"
    name = "$(k.type)"
    p = d2[k]
    x = unique(p.n)
    techs = combine(p, :tech => unique).tech_unique
    ys = [p[p.tech .== t, [:n, :time]] for t in techs]
    plot()
    for (i, t) in enumerate(techs)
        s = ys[i]
        plot!(
            s.n,
            s.time./1e9,
            xscale=:log10,
            title=name,
            label=t,
            xlabel="n - single column length [Int32]",
            ylabel="time [s]"
            )
    end
    savefig("plots/"*name*".png")

end