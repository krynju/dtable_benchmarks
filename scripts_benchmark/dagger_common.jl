_gc = () -> begin
    for i in 1:10
        Dagger.@spawn 10+10
    end
    for i in 1:4
        GC.gc()
    end
end

d = DTable(data, max_chunksize)
data = nothing

_gc(); _gc(); _gc();