from segments_index import TSegmentIndexWriter
from lib.search_engine import TSearchEngine
from utils import TCustomCounter
from pybloom.pybloom import BloomFilter
import pickle
import shelve
import os
import sys

MIN_WORD_FREQ_FOR_INDEX = 5
MAX_WORD_FREQ_FOR_INDEX = 100000000 


def prepare_matches(chunk_fname, keys_out_fname, values_out_fname, pid=""):
    keys_out = open(keys_out_fname, "wb", buffering=1000000)
    values_out = open(values_out_fname, "wb", buffering=1000000)
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
        
    progress_counter = TCustomCounter("Reducer%s" % (str(pid)), sys.stdout, verbosity=1, interval=1000)
        
    for token, chunks in tokens.items():
        token_freq = tokens_freqs[token]
        if token_freq < MIN_WORD_FREQ_FOR_INDEX or token_freq > MAX_WORD_FREQ_FOR_INDEX:
            continue
        all2index = token_freq < 2 * TSearchEngine.MAX_WORD_FREQ
        prob_filter = None
        if not all2index:
            prob_filter_capacity = token_freq - TSearchEngine.MAX_WORD_FREQ
            prob_filter = BloomFilter(capacity=prob_filter_capacity, error_rate=0.1)
        codes2index = []
        send2index = TSearchEngine.MAX_WORD_FREQ
        for chunk_index in chunks:
            for code in all_codes[chunk_index]:
                if all2index or send2index > 0:
                    codes2index.append(code)
                    send2index -= 1
                else:
                    #TODO: move it to index_engine.py
                    code = (code >> 2) << 2 # remove match weight info
                    prob_filter.add(code)
            all_codes[chunk_index] = []
        start_position = values_out.tell()
        pickle.dump(codes2index, values_out)
        prob_filter_start_position = values_out.tell()
        if prob_filter:
            prob_filter.tofile(values_out)
        prob_filter_dump_size = values_out.tell() - prob_filter_start_position
        pickle.dump((token, token_freq, start_position, prob_filter_dump_size), keys_out)
        progress_counter.add()


def prepare_matches_worker(task):
    chunk_fname, keys_out_fname, values_out_fname, pid = task
    prepare_matches(chunk_fname, keys_out_fname, values_out_fname, pid)


def reduce(reducers_chunks, intermid_results_dir,indices_dir, processes_number=5):
    tasks = []
    for pid, chunk_fname in zip(range(len(reducers_chunks)), reducers_chunks):
        keys_fname = os.path.join(intermid_results_dir, str(pid) + "_" + "keys.pickle")
        values_fname = os.path.join(intermid_results_dir, str(pid) + "_" + "values.pickle")
        tasks += [(chunk_fname, keys_fname, values_fname, pid)]
    """
    for task in tasks:
        prepare_matches_worker(task)
    """
    from multiprocessing import Pool
    pool = Pool(processes_number)
    pool.map(prepare_matches_worker, tasks)
    
    
    index_values_fname = os.path.join(indices_dir, "main_index_values.pickle")
    index_keys_fname =  os.path.join(indices_dir, "main_index_keys.db")
    index_values_file = open(index_values_fname, "wb", buffering=1000000)
    index_keys_db = shelve.open(index_keys_fname, writeback=True)
    
    for _, keys_fname, values_fname, _ in tasks:
        chunk_offset = index_values_file.tell()
        values_data = open(values_fname, "rb").read()
        index_values_file.write(values_data)
        keys_file_size = os.path.getsize(keys_fname)
        keys_file = open(keys_fname, "rb")
        while keys_file.tell() < keys_file_size:
            token, token_freq, start_position, prob_filter_dump_size = pickle.load(keys_file)
            start_position += chunk_offset
            index_keys_db[token] = (token_freq, start_position, prob_filter_dump_size)
    index_values_file.close()
    index_keys_db.close()