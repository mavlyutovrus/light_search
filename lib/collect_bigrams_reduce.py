from segments_index import TSegmentIndexWriter
from lib.search_engine import TSearchEngine
from utils import TCustomCounter
from pybloom.pybloom import BloomFilter
import pickle
import shelve
import os
import sys



MIN_WORD_FREQ_FOR_INDEX = 100
MAX_WORD_FREQ_FOR_INDEX = 10000000
SELECT_TOP_NEXT = 20


def prepare_matches(chunk_fname, processed_fname, pid=""):
    out = open(processed_fname, "wb", buffering=1000000)
    all_codes = []
    tokens = {}
    tokens_freqs = {}
    all_ntokens = []
    for line in open(chunk_fname):
        token, next_tokens = line.strip().split("\t")
        next_tokens = next_tokens.split(" ")
        tokens_freqs.setdefault(token, 0)
        tokens_freqs[token] += len(next_tokens)
        tokens.setdefault(token, []).append(len(all_ntokens))
        all_ntokens += [next_tokens]
    for token, chunks in tokens.items():
        if tokens_freqs[token] < MIN_WORD_FREQ_FOR_INDEX or tokens_freqs[token] > MAX_WORD_FREQ_FOR_INDEX:
            continue
        ntokens_freqs = {}
        for chunk_index in chunks:
            for ntoken in all_ntokens[chunk_index]:
                ntokens_freqs.setdefault(ntoken, 0)
                ntokens_freqs[ntoken] += 1
        by_freq = [(freq, next_token) for next_token, freq in ntokens_freqs.items()]
        by_freq.sort(reverse=True)
        selected = [next_token for _, next_token in by_freq[:SELECT_TOP_NEXT]]
        out.write(token + "\t" + " ".join(selected) + "\n")        
    out.close()


def prepare_matches_worker(task):
    chunk_fname, processed_fname, pid = task
    prepare_matches(chunk_fname, processed_fname, pid)


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

def collect_bigrams_reduce(intermid_results_dir,
                           indices_dir, 
                           log_out):
    reducers_chunks = [os.path.join(intermid_results_dir, fname) for fname in os.listdir(intermid_results_dir) \
                                    if fname.startswith("pool_")]
    
    tasks = []
    fsizes = []
    for pid, chunk_fname in zip(range(len(reducers_chunks)), reducers_chunks):
        processed_chunk_fname = os.path.join(intermid_results_dir, str(pid) + "_" + "processed.txt")
        fsize = os.path.getsize(chunk_fname) >> 20 #bytes -> mb
        fsizes.append(fsize)
        tasks += [(fsize, (chunk_fname, processed_chunk_fname, pid))]
    
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
    
    bigrams_db_fname =  os.path.join(indices_dir, "bigrams.db")
    bigrams_db = shelve.open(bigrams_db_fname, writeback=True)
    
    for _, processed_fname, _ in single_proc_tasks + mult_proc_tasks:
        for line in open(processed_fname):
            token, next_tokens = line.strip().split("\t")
            next_tokens = next_tokens.split(" ")
            bigrams_db[token] = next_tokens
    bigrams_db.sync()
