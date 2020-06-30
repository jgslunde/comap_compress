import os
from os import listdir
from os.path import isfile, join
import sys
from multiprocessing.pool import ThreadPool, Pool
import logging
import time
import datetime

#### Set up logging. ####
timestring = str(datetime.datetime.now()).replace(" ", "-").replace(":", "-")
logging.basicConfig(filename="comp-%s.log" % timestring, filemode="a", format="%(asctime)s - %(message)s", level=logging.DEBUG)
nr_errors = 0
raw_size_sum = 0
comp_size_sum = 0

#### Read command-line arguments. ####
if len(sys.argv) > 1:
    path = sys.argv[1]
else:
    raise IndexError("Must provide at least one argument: The folder path of the uncompressed files.")

if len(sys.argv) > 2:
    nr_threads = int(sys.argv[2])
else:
    nr_threads = 1


#### Parse through directory to find filenames matching pattern. ####
filenames = []
for file in listdir(path):
    if isfile(join(path, file)): # Check if file. Could be dir.
        if file[:6] == "comap-" and file[-4:] == ".hd5": # Do only files starting with comap- and ending with .hd5
            filenames.append(file)


### Define compression function. ####
def compress(file_index):
    # Compression function, which will be run by each thread. A unique file index is provided by the Pool.map func.
    filename = filenames[file_index]
    raw_filepath = path + filename  # Filepath of old uncompressed file.
    comp_filepath = path + "comp_" + filename  # Add "comp_" to compressed filename and write it to the same folder.
    command = "h5repack -f /spectrometer/tod:GZIP=1 %s %s" % (raw_filepath, comp_filepath)
    os.system(command)
    
    raw_size = os.path.getsize(raw_filepath)
    comp_size = os.path.getsize(comp_filepath)
    raw_size_sum += raw_size
    comp_size_sum += comp_size
    logging.info("Finished file %s (%d/%d) oldsize=%.2fGB newsize=%.2fGB ratio=%.2f" % (raw_filepath, file_index, len(filenames), raw_size*1e-9, comp_size*1e-9, raw_size/comp_size))


#### Define validation function. ####
def validate(file_index):
    filename = filenames[file_index]
    raw_filepath = path + filename
    comp_filepath = path + "comp_" + filename
    
    errorcode = os.system("h5diff %s %s" % (raw_filepath, comp_filepath))
    
    if errorcode != 0:
        nr_errors += 1
        logging.error("FILE %s DOES NOT MATCH COMPRESSED VERSION %s!" % (raw_filepath, comp_filepath))



#### Start execution over multiple threads. ####
logging.info("Starting compression job.")
logging.info("Compressing %d files with %d threads in dir %s." % (len(filenames), nr_threads, path))
t0 = time.time()
with Pool(nr_threads) as p:
    p.map(compress, range(len(filenames)))
t1 = time.time()
logging.info("Finished compression job. Old totsize=%.2fGB, new totsize=.2fGB, ratio=%.2f" %(raw_size_sum, comp_size_sum, raw_size_sum/comp_size_sum))
logging.info("Total execution time %.2f seconds." % (t1-t0))



logging.info("Starting validation. Comparing compressed to uncompressed version.")
t2 = time.time()
with Pool(nr_threads) as p:
    p.map(validate, range(len(filenames)))
t3 = time.time()
logging.info("Finished validation in %.2f seconds" % (t3-t2))
if nr_errors > 0:
    logging.error("THERE WERE %d ERROR DURING VALIDATION!" % nr_errors)
else:
    logging.info("There were no errors.")
