using Distributed
@everywhere using Pkg;
@everywhere Pkg.activate(".");
@everywhere using Dagger

filename_prefix = "dtable_bench"
include("common_stuff.jl")
include("dagger_common.jl")


groupby_single_col = (d) -> begin
    g = Dagger.groupby(d, :a1)
    (x -> x isa Dagger.EagerThunk && wait(x)).(g.dtable.chunks)
end
w_test("groupby_single_col", groupby_single_col, d, s=1)


close(file)