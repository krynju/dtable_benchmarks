threads=4
workers=2

# chunksizes=('1000000')
# ns=('100000')
# unique_vals_count=('1000')

chunksizes=('1000000' '10000000')
ns=('1000000' '10000000' '100000000' '500000000' '1000000000')
unique_vals_count=('1000' '10000')
ncols="4"

juliacmd="julia -p$workers -t$threads"
juliacmddfs="julia -t$threads"
pythoncmd="python daskb.py $workers $threads"

runcmd() {
    echo "starting $1"
    eval $1
    sleep 2
    echo "done $1"
}

eval "julia init.jl"

trap "exit" INT
for n in "${ns[@]}"; do
    for uvc in "${unique_vals_count[@]}"; do
        for chunksize in "${chunksizes[@]}"; do
            runcmd "$juliacmd dtable1.jl $n $chunksize $uvc $ncols"
            runcmd "$juliacmd dtable2.jl $n $chunksize $uvc $ncols"
            runcmd "$juliacmd dtable3.jl $n $chunksize $uvc $ncols"
            runcmd "$juliacmddfs dataframes.jl $n $chunksize $uvc $ncols"
        done
    done
done


sh python_prep.sh;
source venv/bin/activate

for n in "${ns[@]}"; do
    for uvc in "${unique_vals_count[@]}"; do
        for chunksize in "${chunksizes[@]}"; do
            runcmd "$pythoncmd $n $chunksize $uvc $ncols"
        done
    done
done
