using Distributed
@everywhere using Pkg;
@everywhere Pkg.activate(".")
@everywhere using Dagger

filename_prefix = "dtable_bench"
include("common_stuff.jl")
include("dagger_common.jl")


fmap = (d) -> begin
    m = map(row -> (r = row.a1 + 1,), d)
    wait.(m.chunks)
end
w_test("increment_map", fmap, d)

ffilter = (d) -> begin
    f = filter(row -> row.a1 < unique_values ÷ 2, d)
    wait.(f.chunks)
end
w_test("filter_half", ffilter, d)


fredall = (d) -> begin
    r = reduce(fit!, d, init=Variance())
    fetch(r)
end
w_test("reduce_var_all", fredall, d)

fredsingle = (d) -> begin
    r = reduce(fit!, d, cols=[:a1], init=Variance())
    fetch(r)
end
w_test("reduce_var_single", fredsingle, d)

# groupby_reduce_mean_all = (d) -> begin
#     _g = Dagger.groupby(d, :a1)
#     r = reduce(fit!, _g, init=Mean())
#     fetch(r)
# end
# w_test("groupby_reduce_mean_all", groupby_reduce_mean_all, d, s=1)

close(file)
