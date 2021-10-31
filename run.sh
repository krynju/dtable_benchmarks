threads="4"
workers="4"

chunksizes=('1000000' '10000000')
ns=('1000000' '10000000' '100000000' '500000000' '1000000000')
unique_vals_count=('1000' '10000')
ncols="4"

juliacmd="julia -p$workers -t$threads"
pythoncmd="python daskb.py"

runcmd() {
    echo "starting $1"
    eval $1
    sleep 2
    echo "done $1"
}

trap "exit" INT
for n in "${ns[@]}"; do
    for uvc in "${unique_vals_count[@]}"; do
        for chunksize in "${chunksizes[@]}"; do
            runcmd "$juliacmd dtable1.jl $n $chunksize $uvc $ncols"
            runcmd "$juliacmd dtable2.jl $n $chunksize $uvc $ncols"
            runcmd "$juliacmd dtable3.jl $n $chunksize $uvc $ncols"
            runcmd "$juliacmd dataframes.jl $n $chunksize $uvc $ncols"
        done
    done
done


sh python_prep.sh;

for n in "${ns[@]}"; do
    for uvc in "${unique_vals_count[@]}"; do
        for chunksize in "${chunksizes[@]}"; do
            runcmd "$pythoncmd $workers $threads $n $chunksize $uvc $ncols"
        done
    done
done

echo $juliacmd