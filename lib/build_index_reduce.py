#from segments_index import TSegmentIndexWriter
#from lib.search_engine import TSearchEngine
from utils import TCustomCounter
#from pybloom.pybloom import BloomFilter
import pickle
#import numpy
#import shelve
import os
import sys
from struct import *

MIN_WORD_FREQ_FOR_INDEX = 5
MAX_WORD_FREQ_FOR_INDEX = 30000000 



def prepare_matches(chunk_fname, keys_out_fname, values_out_fname, pid=""):
    keys_out = open(keys_out_fname, "wb", buffering=1000000)
    values_out = open(values_out_fname, "wb", buffering=1000000)
    add_values_out = open(values_out_fname + ".add", "w", buffering=1000000)
    all_codes = []
    tokens = {}
    tokens_freqs = {}
    for line in open(chunk_fname):
        token, token_codes = line.strip().split("\t")
        token_codes = [int(code) for code in token_codes.split()]
        tokens.setdefault(token, []).append(len(all_codes))
        all_codes.append(token_codes)
        tokens_freqs.setdefault(token, 0)
        tokens_freqs[token] += len(token_codes)
    progress_counter = TCustomCounter("Reducer%s" % (str(pid)), sys.stdout, verbosity=1, interval=10000)
    for token, chunks in tokens.items():
        token_freq = tokens_freqs[token]
        if token_freq < MIN_WORD_FREQ_FOR_INDEX or token_freq > MAX_WORD_FREQ_FOR_INDEX:
            continue
        word_codes = []
        for chunk_index in chunks:
            word_codes += all_codes[chunk_index]
            all_codes[chunk_index] = []
        word_codes.sort()
        start_position = values_out.tell()
        for code in word_codes:
            values_out.write(pack('Q', code))
        pickle.dump((token, token_freq, start_position), keys_out)
        progress_counter.add()


def prepare_matches_worker(task):
    chunk_fname, keys_out_fname, values_out_fname, pid = task
    prepare_matches(chunk_fname, keys_out_fname, values_out_fname, pid)


def get_optimal_proc_count(fsizes_mb):
    import psutil
    import math
    mem_avail_mb = psutil.virtual_memory().available >> 20 #bytes -> mb
    mem_avail_mb = mem_avail_mb * 0.8 # leave some space
    if max(fsizes_mb) > mem_avail_mb:
        raise Exception("not enough phys. memory to process biggest chunk")
    fastest = None
    fastest_proc_count = None
    MAX_PROC_COUNT = 3 
    for proc_count in xrange(1, MAX_PROC_COUNT + 1):
        max_file_size4mult = mem_avail_mb / proc_count
        for_multiproc = [fsize for fsize in fsizes_mb if fsize < max_file_size4mult] 
        for_singleproc = [fsize for fsize in fsizes_mb if fsize >= max_file_size4mult]
        
        time_estim = sum(for_singleproc) + sum(for_multiproc) / math.log(proc_count + 1.0)
        if not fastest or fastest > time_estim:
            fastest = time_estim
            fastest_proc_count = proc_count
    return  proc_count, max_file_size4mult

def reduce(intermid_results_dir,indices_dir, log_out):
    reducers_chunks = [os.path.join(intermid_results_dir, fname) for fname in os.listdir(intermid_results_dir) \
                                    if fname.startswith("pool_")]
    
    tasks = []
    fsizes = []
    for pid, chunk_fname in zip(range(len(reducers_chunks)), reducers_chunks):
        keys_fname = os.path.join(intermid_results_dir, str(pid) + "_" + "keys.pickle")
        values_fname = os.path.join(intermid_results_dir, str(pid) + "_" + "values.pickle")
        fsize = os.path.getsize(chunk_fname) >> 20 #bytes -> mb
        fsizes.append(fsize)
        tasks += [(fsize, (chunk_fname, keys_fname, values_fname, pid))]
    
    proc_count, max_file_size4mult = get_optimal_proc_count(fsizes)
    
    single_proc_tasks = [task for fsize, task in tasks if fsize >= max_file_size4mult]
    mult_proc_tasks = [task for fsize, task in tasks if fsize < max_file_size4mult]
    
    log_out.write("..optimal proc count: %d\n" % (proc_count))
    log_out.write("..max size for mult proc: %dmb\n" % (max_file_size4mult))
    log_out.write("..files for single proc: %d\n" % (len(single_proc_tasks)))    
    log_out.write("..files for mult proc: %d\n" % (len(mult_proc_tasks))) 
    log_out.flush()
    
    log_out.write("Singleprocessing for big files.\n") 
    log_out.flush() 
       
    for task in single_proc_tasks:
        prepare_matches_worker(task)
        
    log_out.write("Multiprocessing.\n") 
    log_out.flush()
         
    from multiprocessing import Pool
    pool = Pool(proc_count)
    pool.map(prepare_matches_worker, mult_proc_tasks)
    
    log_out.write("Joining parts of the index.\n") 
    log_out.flush()
    
    index_values_fname = os.path.join(indices_dir, "main_index_values.pickle")
    index_keys_fname =  os.path.join(indices_dir, "main_index_keys.db")
    index_values_file = open(index_values_fname, "wb", buffering=1000000)
    #index_keys_db = shelve.open(index_keys_fname, writeback=True)
    index_keys_as_txt = open(index_keys_fname + ".txt", "w")
    
    for _, keys_fname, values_fname, _ in single_proc_tasks + mult_proc_tasks:
        chunk_offset = index_values_file.tell()
        values_data = open(values_fname, "rb").read()
        index_values_file.write(values_data)
        keys_file_size = os.path.getsize(keys_fname)
        keys_file = open(keys_fname, "rb")
        while keys_file.tell() < keys_file_size:
            token, token_freq, start_position = pickle.load(keys_file)
            start_position += chunk_offset
            #index_keys_db[token] = (token_freq, start_position)
            index_keys_as_txt.write("%s %d %d\n" % (token, token_freq, start_position))
    index_values_file.close()
    #index_keys_db.close()
    index_keys_as_txt.close()
