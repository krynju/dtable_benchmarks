using Distributed
@everywhere using Pkg;
@everywhere Pkg.activate(".");
@everywhere using DataFrames

filename_prefix = "dataframes_bench"
include("common_stuff.jl")

d = DataFrame(data)
data = nothing



fmap = (d) -> begin
    select(d, :a1 => (x) -> x.+1)
end
w_test("increment_map", fmap, d; prefix="dataframesjl")

ffilter = (d) -> begin
    f = filter(:a1 => <(unique_values ÷ 2), d)
end
w_test("filter_half", ffilter, d; prefix="dataframesjl")


fredall = (d) -> begin
    combine(d, propertynames(d) .=> (x -> fit!(Variance(), x)))
end
w_test("reduce_var_all", fredall, d; prefix="dataframesjl")


fredsingle = (d) -> begin
    combine(d, :a1 => (x -> fit!(Variance(), x)))
end
w_test("reduce_var_single", fredsingle, d; prefix="dataframesjl")



# groupby_reduce_mean_all = (d) -> begin
#     combine(groupby(d, :a1), propertynames(d) .=> (x -> fit!(Mean(), x)))
# end
# w_test("groupby_reduce_mean_all", groupby_reduce_mean_all, d; prefix="dataframesjl")



groupby_single_col = (d) -> begin
    groupby(d, :a1)
end
w_test("groupby_single_col", groupby_single_col, d; prefix="dataframesjl")


################
# grouped prep
g = groupby(d, :a1)
d = nothing
GC.gc();GC.gc();
################


grouped_reduce_mean_singlecol = (g) -> begin
    r = combine(g, :a2 => (x -> fit!(Mean(), x)))
end
w_test("grouped_reduce_mean_singlecol", grouped_reduce_mean_singlecol, g; prefix="dataframesjl")


grouped_reduce_mean_allcols = (g) -> begin
    combine(g, names(g) .=> (x -> fit!(Mean(), x)))
end
w_test("grouped_reduce_mean_allcols", grouped_reduce_mean_allcols, g; prefix="dataframesjl")


close(file)
