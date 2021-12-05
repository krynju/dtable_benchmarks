using Distributed
@everywhere using Pkg;
@everywhere Pkg.activate(".");
@everywhere using Dagger

filename_prefix = "dtable_bench"
include("scripts_benchmark/common_stuff.jl")
include("scripts_benchmark/dagger_common.jl")



################
# grouped prep
_gc()
g = Dagger.groupby(d, :a1)
d = nothing
_gc()
_gc()
################

grouped_reduce_mean_singlecol = (g) -> begin
    r = reduce(fit!, g, cols=[:a2], init=Mean())
    fetch(r)
end
w_test("grouped_reduce_mean_singlecol", grouped_reduce_mean_singlecol, g)


grouped_reduce_mean_allcols = (g) -> begin
    r = reduce(fit!, g, init=Mean())
    fetch(r)
end
w_test("grouped_reduce_mean_allcols", grouped_reduce_mean_allcols, g)


close(file)