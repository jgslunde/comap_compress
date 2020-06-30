"""
Program to compress a series of hdf5 files in a series of directories, using the h5repack gzip compression algorithm.
In a series of specified direcotories, it compresses all files following the format comap-*.hd5, using a set number of threads.

Takes two or more command-line arguments.
The first argument is the number of threads to use, as an integer.
An arbritrary number of arguments can follow, specifying the folders in which to look for hdf5 files.
Use relative paths from where the program is executed, and include a trailing /

Example compressing all files in the folders 2019-05 and 2019-06 using 48 threads:
python3 compress.py 48 2019-05/ 2019-06/

A single logfile is written, on the format comp-[datetime].log, to the folder where the program was executed.
"""


import os
from os import listdir
from os.path import isfile, join
import sys
from multiprocessing.pool import ThreadPool, Pool
import logging
import time
import datetime

#### Read command-line arguments. ####
if len(sys.argv) < 2:
    raise IndexError("Must provide at least two arguments: Number of threads, and at least one folder path for the uncompressed files.")
else:
    nr_threads = int(sys.argv[1])
    paths = []
    for i in range(2, len(sys.argv)):
        paths.append(sys.argv[i])


#### Set up logging. ####
timestring = str(datetime.datetime.now()).replace(" ", "-").replace(":", "-")
logging.basicConfig(filename="comp-%s.log" % timestring, filemode="a", format="%(asctime)s - %(message)s", level=logging.DEBUG)
logging.info("Initializing compression over paths %s", paths)

print(paths)
for path in paths:
    print(path)

    nr_errors = 0

    #### Parse through directory to find filenames matching pattern. ####
    filenames = []
    for file in listdir(path):
        if isfile(join(path, file)): # Check if file. Could be dir.
            if file[:6] == "comap-" and file[-4:] == ".hd5": # Do only files starting with comap- and ending with .hd5
                filenames.append(file)


    ### Define compression function. ####
    def compress(file_index):
        t00 = time.time()
        # Compression function, which will be run by each thread. A unique file index is provided by the Pool.map func.
        filename = filenames[file_index]
        raw_filepath = path + filename  # Filepath of old uncompressed file.
        comp_filepath = path + "comp_" + filename  # Add "comp_" to compressed filename and write it to the same folder.
        command = "h5repack -f /spectrometer/tod:GZIP=1 %s %s" % (raw_filepath, comp_filepath)
        os.system(command)
        t01 = time.time()
        
        raw_size = os.path.getsize(raw_filepath)
        comp_size = os.path.getsize(comp_filepath)
        logging.info("Finished file %s (%d/%d)  oldsize=%.2fGB  newsize=%.2fGB  ratio=%.2f  time spent=%.1fm" % (raw_filepath, file_index+1, len(filenames), raw_size*1e-9, comp_size*1e-9, raw_size/comp_size, (t01-t00)/60.0))


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
    raw_size_sum = 0
    comp_size_sum = 0
    for filename in filenames:
        raw_filepath = path + filename
        comp_filepath = path + "comp_" + filename 
        raw_size_sum += os.path.getsize(raw_filepath)
        comp_size_sum += os.path.getsize(comp_filepath)
    logging.info("Finished compression job. Old totsize=%.2fGB  new totsize=%.2fGB  ratio=%.2f" % (raw_size_sum*1e-9, comp_size_sum*1e-9, raw_size_sum/comp_size_sum))
    logging.info("Total execution time %.2f seconds." % (t1-t0))


    ### Starting validation. ###
    logging.info("Starting validation.")
    logging.info("Counting files...")
    for filename in filenames:
        raw_filepath = path + filename
        comp_filepath = path + "comp_" + filename
        if not os.path.isfile(raw_filepath):
            logging.error("THE ORIGINAL FILE %s SHOULD EXIST, BUT I CAN'T FIND IT." % raw_filepath)
        if not os.path.isfile(comp_filepath):
            logging.error("THE COMPRESSED FILE %s SHOULD EXIST, BUT I CAN'T FIND IT." % comp_filepath)
            

    logging.info("Comparing compressed to uncompressed version.")
    t2 = time.time()
    with Pool(nr_threads) as p:
        p.map(validate, range(len(filenames)))
    t3 = time.time()
    logging.info("Finished validation in %.2f seconds" % (t3-t2))
    if nr_errors > 0:
        logging.error("THERE WERE %d ERRORS DURING VALIDATION!" % nr_errors)
    else:
        logging.info("There were no errors.")
