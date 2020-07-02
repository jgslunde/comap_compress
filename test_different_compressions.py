import sys
import time
import os
from multiprocessing.pool import ThreadPool, Pool

path = "/mn/stornext/d16/cmbco/comap/pathfinder/ovro/2019-05"
filenames = ["comap-0005978-2019-05-18-011318.hd5",
             "comap-0005979-2019-05-18-023832.hd5",
             "comap-0005980-2019-05-18-043705.hd5",
             "comap-0005981-2019-05-18-054046.hd5",]

gzip_range = [1,2,3,4,5,6]

nr_threads = len(gzip_range)*len(filenames)
print("Total of %d threads" % nr_threads)

distributed_filenames = []
distributed_gzip = []

for i in range(len(gzip_range)):
    for j in range(len(filenames)):
        distributed_gzip.append(gzip_range[i])
        distributed_filenames.append(filenames[j])
        
def compress(thread_idx):
    gzip_level = distributed_gzip[thread_idx]
    filename = distributed_filenames[thread_idx]
    raw_filepath = path + "/" + filename
    comp_filepath = "comp_gzip%f_" + filename
    print("Thread %d running gzip=%d on file %s to file %s" % (thread_idx, gzip_level, raw_filepath, comp_filepath))
    
    t00 = time.time()
    command = "h5repack -f /spectrometer/tod:GZIP=%d %s %s" % (gzip_level, raw_filepath, comp_filepath)
    exitcode = os.system(command)
    if exitcode != 0:
        print("ERROR: Finished with exit code %d" % exitcode)
    t01 = time.time()
    
    raw_size = os.path.getsize(raw_filepath)
    comp_size = os.path.getsize(comp_filepath)
    print("Finished file %s oldsize=%.2fGB  newsize=%.2fGB  ratio=%.2f  time spent=%.1fm" % (raw_filepath, raw_size*1e-9, comp_size*1e-9, raw_size/comp_size, (t01-t00)/60.0))


t0 = time.time()
with Pool(nr_threads) as p:
    p.map(compress, range(len(filenames)))
t1 = time.time()
print("Finished compression in %f minutes" % (t1-t0)/60.0)



import h5py
import numpy as np

for thread_idx in range(nr_threads):
    
    gzip_level = distributed_gzip[thread_idx]
    filename = distributed_filenames[thread_idx]
    raw_filepath = path + "/" + filename
    comp_filepath = "comp_gzip%f_" + filename
    
    t0 = time.time()
    data = h5py.File(raw_filepath, "r")
    asdf = np.array(data["spectrometer/tod"])
    t1 = time.time()
    print("Finished file %s with GZIP=%d in %d seconds " % (filename, gzip_level, (t1-t0)) )
