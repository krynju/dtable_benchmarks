using CSV, DataFrames, Tables, TableOperations, Plots

ld = (filename) -> CSV.read(filename, DataFrame)

tech = ["dtable","dask" ,"dataframesjl" ]
tech_nicenames = ["DTable","Dask", "DataFrames.jl"]
mkpath("blog_plots")

prep_plot_input = (d) -> [d[d.tech .== t, [:n, :time]] for t in tech]

lrplot = (l, r) -> begin
    p = plot(
        layout=2,
        link=:both,
        plot_titlevspan=0.10,
        size=(600,300)
    )
    input = prep_plot_input(l)
    for (i, t) in enumerate(tech)
        s = input[i]
        plot!(
            s.n,
            s.time ./ 1e9,
            xscale=:log10,
            yscale=:log10,
            xticks=[1e6, 1e7, 1e8, 1e9],
            label=tech_nicenames[i],
            xlabel="n",
            ylabel="time [s]",
            legend=:topleft,
            subplot=1,
            title="chunksize: 1e6",
            titlefontsize=10,
            yticks=[1e-2,1e-1, 1e0, 1e1, 1e2],
        )
    end
    input = prep_plot_input(r)
    for (i, t) in enumerate(tech)
        s = input[i]
        plot!(
            s.n,
            s.time ./ 1e9,
            xscale=:log10,
            yscale=:log10,
            xticks=[1e6, 1e7, 1e8, 1e9],
            yticks=[1e-2,1e-1, 1e0, 1e1, 1e2],
            label=tech_nicenames[i],
            xlabel="n",
            legend=false,
            subplot=2,
            title="chunksize: 1e7",
            titlefontsize=10,
        )
    end
    return p
end

let
    l = ld("summary_benches/reduce_var_single-chunksize1000000_uniquevals1000.csv")
    r = ld("summary_benches/reduce_var_single-chunksize10000000_uniquevals1000.csv")

    p = lrplot(l, r)
    plot!(
        plot_title="Reduction: single column",

    )
    display(p)
    savefig(p, "blog_plots/reduce_single_col.svg")
end


let
    l = ld("summary_benches/reduce_var_all-chunksize1000000_uniquevals1000.csv")
    r = ld("summary_benches/reduce_var_all-chunksize10000000_uniquevals1000.csv")
    p = lrplot(l, r)
    plot!(
        plot_title="Reduction: all columns (4)",

    )
    display(p)
    savefig(p, "blog_plots/reduce_allcols.svg")
end

let
    l = ld("summary_benches/increment_map-chunksize1000000_uniquevals10000.csv")
    r = ld("summary_benches/increment_map-chunksize10000000_uniquevals10000.csv")
    p = lrplot(l, r)
    plot!(
        plot_title="Map: increment single column",
    )
    display(p)
    savefig(p, "blog_plots/inrement_map.svg")
end

let
    l = ld("summary_benches/filter_half-chunksize1000000_uniquevals1000.csv")
    r = ld("summary_benches/filter_half-chunksize10000000_uniquevals1000.csv")
    p = lrplot(l, r)
    plot!(
        plot_title="Filter: ~half of records",
    )
    display(p)
    savefig(p, "blog_plots/filter_half.svg")
end


plot4 = (tl, tr, bl, br) -> begin
    p = plot(
        layout=4,
        link=:both,
        plot_titlevspan=0.10,
        size=(600,600)
    )
    input = prep_plot_input(tl)
    for (i, t) in enumerate(tech)
        s = input[i]
        plot!(
            s.n,
            s.time ./ 1e9,
            xscale=:log10,
            yscale=:log10,
            xticks=[1e6, 1e7, 1e8, 1e9],
            label=tech_nicenames[i],
            xlabel="n",
            ylabel="time [s]",
            legend=:topleft,
            subplot=1,
            title="chunksize: 1e6, uvals=1e3",
            titlefontsize=10,
        )
    end

    titles = ["chunksize: 1e6, uvals=1e4","chunksize: 1e7, uvals=1e3","chunksize: 1e7, uvals=1e4"]
    data = [tr,bl, br]
    indx = [2,3,4]

    for (title, d, ii) in zip(titles,data,indx)
        input = prep_plot_input(d)
        for (i, t) in enumerate(tech)
            s = input[i]
            plot!(
                s.n,
                s.time ./ 1e9,
                xscale=:log10,
                yscale=:log10,
                xticks=[1e6, 1e7, 1e8, 1e9],
                label=tech_nicenames[i],
                xlabel="n",
                ylabel="time [s]",
                legend=false,
                subplot=ii,
                title=title,
                titlefontsize=10,
            )
        end
    end
    return p


end

let
    tl = ld("summary_benches/groupby_single_col-chunksize1000000_uniquevals1000.csv")
    tr = ld("summary_benches/groupby_single_col-chunksize1000000_uniquevals10000.csv")
    bl = ld("summary_benches/groupby_single_col-chunksize10000000_uniquevals1000.csv")
    br = ld("summary_benches/groupby_single_col-chunksize10000000_uniquevals10000.csv")

    p = plot4(tl,tr,bl,br)
    plot!(
        plot_title="groupby_singlecol",

    )
    display(p)
    savefig(p, "blog_plots/groupby_single_col.svg")
end


let
    tl = ld("summary_benches/grouped_reduce_mean_allcols-chunksize1000000_uniquevals1000.csv")
    tr = ld("summary_benches/grouped_reduce_mean_allcols-chunksize1000000_uniquevals10000.csv")
    bl = ld("summary_benches/grouped_reduce_mean_allcols-chunksize10000000_uniquevals1000.csv")
    br = ld("summary_benches/grouped_reduce_mean_allcols-chunksize10000000_uniquevals10000.csv")

    p = plot4(tl,tr,bl,br)
    plot!(
        plot_title="grouped_reduce_mean_allcols",

    )
    display(p)
    savefig(p, "blog_plots/grouped_reduce_mean_allcols.svg")
end

let
    tl = ld("summary_benches/grouped_reduce_mean_singlecol-chunksize1000000_uniquevals1000.csv")
    tr = ld("summary_benches/grouped_reduce_mean_singlecol-chunksize1000000_uniquevals10000.csv")
    bl = ld("summary_benches/grouped_reduce_mean_singlecol-chunksize10000000_uniquevals1000.csv")
    br = ld("summary_benches/grouped_reduce_mean_singlecol-chunksize10000000_uniquevals10000.csv")

    p = plot4(tl,tr,bl,br)
    plot!(
        plot_title="grouped_reduce_mean_singlecol",

    )
    display(p)
    savefig(p, "blog_plots/grouped_reduce_mean_singlecol.svg")
end


let
    tl = ld("summary_benches/groupby_reduce_mean_all-chunksize1000000_uniquevals1000.csv")
    tr = ld("summary_benches/groupby_reduce_mean_all-chunksize1000000_uniquevals10000.csv")
    bl = ld("summary_benches/groupby_reduce_mean_all-chunksize10000000_uniquevals1000.csv")
    br = ld("summary_benches/groupby_reduce_mean_all-chunksize10000000_uniquevals10000.csv")

    p = plot4(tl,tr,bl,br)
    plot!(
        plot_title="groupby_reduce_mean_all",

    )
    display(p)
    savefig(p, "blog_plots/groupby_reduce_mean_all.svg")
end

