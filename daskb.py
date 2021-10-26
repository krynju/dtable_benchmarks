import math, time
#from typing_extensions import runtime_checkable
from dask.distributed import Client, wait
from distributed.client import default_client
import pandas as pd
import timeit
import sys
import dask.array as da
import dask.dataframe as dd

client = Client(n_workers=1, threads_per_worker=16, processes=False, memory_limit='32GB')
client.restart()


n = int(sys.argv[1])
max_chunksize = int(sys.argv[2])
unique_values = int(sys.argv[3])
ncolumns = int(sys.argv[4])


# n = 100000000
# max_chunksize = 1000000
# unique_values = 1000
# ncolumns = 4


x = da.random.randint(0, unique_values, size=(int(n), ncolumns), chunks=(max_chunksize, ncolumns))
tablesize = 4 * ncolumns * n / 1_000_000
print("tablesize {} MB".format(tablesize))

filename = 'dask_bench' + str(round(time.time() * 1000)) + '.csv'
file = open(filename, 'w')
file.write('tech,type,n,chunksize,unique_vals,ncolumns,time,gctime,memory,allocs\n')


def runb(type, f):
    t = timeit.timeit(stmt=f, setup='gc.enable()', number=2)
    file.write('{},{},{},{},{},{},{},{},{},{}\n'.format('dask',type, n, max_chunksize,unique_values,ncolumns, t*1e9, 0, 0, 0))
    file.flush()
    print('done '+ type + '\n')

df = dd.from_dask_array(x).persist()
wait(df)
x = None




runb('increment_map', lambda : wait((df[0] + 1).persist()))

runb('filter_half', lambda : df[df[0] < unique_values /2].compute())

runb('reduce_var_all', lambda : df.var().compute())

runb('reduce_var_single', lambda : df[0].var().compute())

runb('groupby_reduce_mean_all', lambda : df.shuffle(0, shuffle='tasks', npartitions=unique_values).groupby(0).mean().compute())

runb('groupby_single_col', lambda : wait(df.shuffle(0, shuffle='tasks', npartitions=unique_values).persist()))



gf = df.shuffle(0, shuffle='tasks', npartitions=unique_values).persist()
df = None
wait(gf)

runb('grouped_reduce_mean_singlecol', lambda : gf.groupby(0)[1].mean().compute())
runb('grouped_reduce_mean_allcols', lambda : gf.groupby(0).mean().compute())

file.close()
