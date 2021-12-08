using CSV, DataFrames, Tables, TableOperations, Plots, LaTeXStrings

ld = (filename) -> CSV.read(filename, DataFrame)

tech = ["dtable","dask" ,"dataframesjl" ]
tech_nicenames = ["DTable","Dask", "DataFrames.jl"]
mkpath("blog_plots")


TIMESTRING = L"\mathrm{time\hspace{0.5}[s]}"
TITLEFONTSIZE = 12
XLABEL = L"n"
CHUNKSIZETITLE = (x) -> L"\mathrm{chunksize: 10^%$x}"
CHUNKUNIQUETITLE = (x, y) -> L"\mathrm{chunksize: 10^%$x}, \mathrm{uniquevals: 10^%$y}"

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
            xlabel=XLABEL,
            ylabel=TIMESTRING,
            legend=:topleft,
            subplot=1,
            title=CHUNKSIZETITLE(6),
            titlefontsize=TITLEFONTSIZE,
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
            xlabel=XLABEL,
            legend=false,
            subplot=2,
            title=CHUNKSIZETITLE(7),
            titlefontsize=TITLEFONTSIZE,
        )
    end
    return p
end

let
    l = ld("summaries/reduce_var_single-chunksize1000000_uniquevals1000.csv")
    r = ld("summaries/reduce_var_single-chunksize10000000_uniquevals1000.csv")

    p = lrplot(l, r)
    plot!(
        plot_title="reduce (single column)",

    )
    display(p)
    savefig(p, "blog_plots/reduce_single_col.svg")
end


let
    l = ld("summaries/reduce_var_all-chunksize1000000_uniquevals1000.csv")
    r = ld("summaries/reduce_var_all-chunksize10000000_uniquevals1000.csv")
    p = lrplot(l, r)
    plot!(
        plot_title="reduce (all columns - 4)",

    )
    display(p)
    savefig(p, "blog_plots/reduce_allcols.svg")
end

let
    l = ld("summaries/increment_map-chunksize1000000_uniquevals10000.csv")
    r = ld("summaries/increment_map-chunksize10000000_uniquevals10000.csv")
    p = lrplot(l, r)
    plot!(
        plot_title="map (single column increment)",
    )
    display(p)
    savefig(p, "blog_plots/inrement_map.svg")
end

let
    l = ld("summaries/filter_half-chunksize1000000_uniquevals1000.csv")
    r = ld("summaries/filter_half-chunksize10000000_uniquevals1000.csv")
    p = lrplot(l, r)
    plot!(
        plot_title="filter (half of elements)",
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
            yticks=[1e-3,1e-2,1e-1, 1e0, 1e1, 1e2,1e3, 1e4],
            xlabel=XLABEL,
            ylabel=TIMESTRING,
            legend=:topleft,
            subplot=1,
            title=CHUNKUNIQUETITLE(6,3),
            titlefontsize=TITLEFONTSIZE,
        )
    end
    titles = [CHUNKUNIQUETITLE(c,v) for (c,v) in [(7,3), (6,4), (7,4)]]
    # titles = ["chunksize: 1e7, uvals=1e3","chunksize: 1e6, uvals=1e4","chunksize: 1e7, uvals=1e4"]
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
                yticks=[1e-3,1e-2,1e-1, 1e0, 1e1, 1e2,1e3, 1e4],
                label=tech_nicenames[i],
                xlabel=XLABEL,
                ylabel=TIMESTRING,
                legend=false,
                subplot=ii,
                title=title,
                titlefontsize=TITLEFONTSIZE,
            )
        end
    end
    return p


end

let
    tl = ld("summaries/groupby_single_col-chunksize1000000_uniquevals1000.csv")
    bl = ld("summaries/groupby_single_col-chunksize1000000_uniquevals10000.csv")
    tr = ld("summaries/groupby_single_col-chunksize10000000_uniquevals1000.csv")
    br = ld("summaries/groupby_single_col-chunksize10000000_uniquevals10000.csv")

    p = plot4(tl,tr,bl,br)
    plot!(
        plot_title="groupy (shuffle)",

    )
    display(p)
    savefig(p, "blog_plots/groupby_single_col.svg")
end


let
    tl = ld("summaries/grouped_reduce_mean_allcols-chunksize1000000_uniquevals1000.csv")
    bl = ld("summaries/grouped_reduce_mean_allcols-chunksize1000000_uniquevals10000.csv")
    tr = ld("summaries/grouped_reduce_mean_allcols-chunksize10000000_uniquevals1000.csv")
    br = ld("summaries/grouped_reduce_mean_allcols-chunksize10000000_uniquevals10000.csv")

    p = plot4(tl,tr,bl,br)
    plot!(
        plot_title="grouped reduce (all columns - 4)",

    )
    display(p)
    savefig(p, "blog_plots/grouped_reduce_mean_allcols.svg")
end

let
    tl = ld("summaries/grouped_reduce_mean_singlecol-chunksize1000000_uniquevals1000.csv")
    bl = ld("summaries/grouped_reduce_mean_singlecol-chunksize1000000_uniquevals10000.csv")
    tr = ld("summaries/grouped_reduce_mean_singlecol-chunksize10000000_uniquevals1000.csv")
    br = ld("summaries/grouped_reduce_mean_singlecol-chunksize10000000_uniquevals10000.csv")

    p = plot4(tl,tr,bl,br)
    plot!(
        plot_title="grouped reduce (single column)",

    )
    display(p)
    savefig(p, "blog_plots/grouped_reduce_mean_singlecol.svg")
end


let
    tl = ld("summaries/groupby_reduce_mean_all-chunksize1000000_uniquevals1000.csv")
    bl = ld("summaries/groupby_reduce_mean_all-chunksize1000000_uniquevals10000.csv")
    tr = ld("summaries/groupby_reduce_mean_all-chunksize10000000_uniquevals1000.csv")
    br = ld("summaries/groupby_reduce_mean_all-chunksize10000000_uniquevals10000.csv")

    p = plot4(tl,tr,bl,br)
    plot!(
        plot_title="groupby_reduce_mean_all",

    )
    display(p)
    savefig(p, "blog_plots/groupby_reduce_mean_all.svg")
end

