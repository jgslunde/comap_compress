import sys
import time
import os
from multiprocessing.pool import ThreadPool, Pool

path = "/mn/stornext/d22/cmbco/comap/protodir/level1/2022-03"
# path = "/mn/stornext/d22/cmbco/comap/protodir/level1/2022-02"
ramdisk_path = "/dev/shm"
filenames = ["comap-0028253-2022-03-06-155542.hd5",
             "comap-0028167-2022-03-03-222909.hd5",
             "comap-0028075-2022-03-01-003313.hd5",
             "comap-0028188-2022-03-04-145846.hd5",]
# filenames = ["comap-0028074-2022-02-28-233112.hd5",
#              "comap-0028052-2022-02-28-045901.hd5",
#              "comap-0027991-2022-02-26-071415.hd5",
#              "comap-0027876-2022-02-14-005706.hd5",]

print("Moving files to ram-disk...")
for filename in filenames:
    command = f"cp {path}/{filename} {ramdisk_path}/"
    os.system(command)

gzip_range = [1,2,3,4,5,6,7]


distributed_filenames = []
distributed_gzip = []

for i in range(len(gzip_range)):
    for j in range(len(filenames)):
        distributed_gzip.append(gzip_range[i])
        distributed_filenames.append(filenames[j])

nr_threads = len(distributed_gzip)
print("Total of %d threads" % nr_threads)
        
def compress(thread_idx):
    gzip_level = distributed_gzip[thread_idx]
    filename = distributed_filenames[thread_idx]
    raw_filepath = ramdisk_path + "/" + filename
    comp_filepath = "comp_gzip%d_%s" % (gzip_level, filename)
    print("Thread %d running gzip=%d on file %s to file %s" % (thread_idx, gzip_level, raw_filepath, comp_filepath))
    
    t00 = time.time()
    command = "h5repack -f /spectrometer/tod:SHUF -f /spectrometer/tod:GZIP=%d -l /spectrometer/tod:CHUNK=1x4x1024x4000 %s %s" % (gzip_level, raw_filepath, comp_filepath)
    exitcode = os.system(command)
    if exitcode != 0:
        print("ERROR: Finished with exit code %d" % exitcode)
    t01 = time.time()
    
    raw_size = os.path.getsize(raw_filepath)
    comp_size = os.path.getsize(comp_filepath)
    print("Finished file %s oldsize=%.2fGB  newsize=%.2fGB  ratio=%.2f  time spent=%.1fm" % (comp_filepath, raw_size*1e-9, comp_size*1e-9, raw_size/comp_size, (t01-t00)/60.0))


t0 = time.time()

with Pool(nr_threads) as p:
    p.map(compress, range(len(distributed_filenames)))
t1 = time.time()
print("Finished compression in %f minutes" % ((t1-t0)/60.0))



import h5py
import numpy as np

for thread_idx in range(nr_threads):
    
    gzip_level = distributed_gzip[thread_idx]
    filename = distributed_filenames[thread_idx]
    comp_filepath = "comp_gzip%d_%s" % (gzip_level, filename)
    
    t0 = time.time()
    with h5py.File(comp_filepath, "r") as f:
        asdf = f["spectrometer/tod"][()]
    t1 = time.time()
    print("Finished file %s with GZIP=%d in %d seconds " % (comp_filepath, gzip_level, (t1-t0)) )

print("Deleting files from ram-disk...")
command = f"rm {ramdisk_path}/*.hd5"
os.system(command)
