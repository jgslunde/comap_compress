import sys
import time
import os
from multiprocessing.pool import ThreadPool, Pool

path = "/mn/stornext/d16/cmbco/comap/pathfinder/ovro/2020-06"
target_path = "/mn/stornext/d16/cmbco/comap/jonas/compression"
filenames = ["comap-0014481-2020-06-24-205832.hd5",
             "comap-0014158-2020-06-11-132655.hd5",
             "comap-0014148-2020-06-11-050935.hd5",
             "comap-0014122-2020-06-10-083154.hd5",
             "comap-0013865-2020-06-01-101139.hd5",]

gzip_range = [1,2,3,4,5,6,7]

nr_threads = len(gzip_range)*len(filenames)
print("Total of %d threads" % nr_threads)

distributed_filenames = []
distributed_gzip = []

for i in range(len(gzip_range)):
    for j in range(len(filenames)):
        distributed_gzip.append(gzip_range[i])
        distributed_filenames.append(filenames[j])

print("Runlist:", distributed_gzip, distributed_filenames)

def compress(thread_idx):
    gzip_level = distributed_gzip[thread_idx]
    filename = distributed_filenames[thread_idx]
    raw_filepath = path + "/" + filename
    comp_filepath = target_path + "/" + ("comp_gzip%d_" % gzip_level) + filename
    print("Thread %d running gzip=%d on file %s to file %s" % (thread_idx, gzip_level, raw_filepath, comp_filepath))
    
    t00 = time.time()
    command = "h5repack -f /spectrometer/tod:GZIP=%d %s %s" % (gzip_level, raw_filepath, comp_filepath)
    exitcode = os.system(command)
    if exitcode != 0:
        print("ERROR: Finished with exit code %d" % exitcode)
    t01 = time.time()
    
    raw_size = os.path.getsize(raw_filepath)
    comp_size = os.path.getsize(comp_filepath)
    print("Finished file %s with gzip %d oldsize=%.2fGB  newsize=%.2fGB  ratio=%.2f  time spent=%.1fm" % (comp_filepath, gzip_level, raw_size*1e-9, comp_size*1e-9, raw_size/comp_size, (t01-t00)/60.0))


t0 = time.time()
with Pool(nr_threads) as p:
    p.map(compress, range(nr_threads))
t1 = time.time()
print("Finished compression in %f minutes" % ((t1-t0)/60.0))


import h5py
import numpy as np

for thread_idx in range(nr_threads):
    
    gzip_level = distributed_gzip[thread_idx]
    filename = distributed_filenames[thread_idx]
    raw_filepath = path + "/" + filename
    comp_filepath = target_path + "/" + ("comp_gzip%d_" % gzip_level) + filename
    
    t0 = time.time()
    data = h5py.File(comp_filepath, "r")
    asdf = np.array(data["spectrometer/tod"])
    t1 = time.time()
    print("Finished file %s with GZIP=%d in %d seconds " % (filename, gzip_level, (t1-t0)) )
