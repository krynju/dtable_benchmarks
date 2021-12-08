using Distributed
@everywhere using Pkg;
@everywhere Pkg.activate(".");
@everywhere using DataFrames
@everywhere using Statistics

filename_prefix = "dataframes_bench"
include("common_stuff.jl")

d = DataFrame(data)
data = nothing



fmap = (d) -> begin
    select(d, :a1 => (x) -> x.+1)
end
w_test("increment_map", fmap, d)

ffilter = (d) -> begin
    f = filter(row -> row.a1 < unique_values ÷ 2, d)
end
w_test("filter_half", ffilter, d)


fredall = (d) -> begin
    combine(d, propertynames(d) .=> var)
end
w_test("reduce_var_all", fredall, d)


fredsingle = (d) -> begin
    combine(d, :a1 => var)
end
w_test("reduce_var_single", fredsingle, d)



# groupby_reduce_mean_all = (d) -> begin
#     combine(groupby(d, :a1), propertynames(d) .=> mean)
# end
# w_test("groupby_reduce_mean_all", groupby_reduce_mean_all, d)



groupby_single_col = (d) -> begin
    groupby(d, :a1)
end
w_test("groupby_single_col", groupby_single_col, d)


################
# grouped prep
g = groupby(d, :a1)
d = nothing
GC.gc();GC.gc();
################


grouped_reduce_mean_singlecol = (g) -> begin
    r = combine(g, :a2 => mean)
end
w_test("grouped_reduce_mean_singlecol", grouped_reduce_mean_singlecol, g)


grouped_reduce_mean_allcols = (g) -> begin
    combine(g, names(g) .=> mean)
end
w_test("grouped_reduce_mean_allcols", grouped_reduce_mean_allcols, g)


close(file)
