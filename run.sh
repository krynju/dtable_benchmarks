threads=16
workers=0

# chunksizes=('1000000')
# ns=('100000')
# unique_vals_count=('1000')

chunksizes=('1000000' '10000000')
ns=('1000000' '10000000' '100000000' '500000000' '1000000000')
unique_vals_count=('1000' '10000')
ncols="4"

s="scripts_benchmark/"

if [[ $workers -eq 1 ]]; then
    juliacmd="julia -t$threads"
    pythoncmd="python ${s}daskb.py 1 $threads"
else
    juliacmd="julia -p$(($workers-1)) -t$threads"
    pythoncmd="python ${s}daskb.py $workers $threads"
fi

juliacmddfs="julia -t$threads"


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
            runcmd "$juliacmd ${s}dtable1.jl $n $chunksize $uvc $ncols"
            runcmd "$juliacmd ${s}dtable2.jl $n $chunksize $uvc $ncols"
            runcmd "$juliacmd ${s}dtable3.jl $n $chunksize $uvc $ncols"
            runcmd "$juliacmddfs ${s}dataframes.jl $n $chunksize $uvc $ncols"
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
