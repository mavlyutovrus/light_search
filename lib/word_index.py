import os
import sys
import shelve
import pickle
import numpy
from pybloom import BloomFilter


class TWordIndexReader(object):
    MAX_CACHE_SIZE = 1000
    def __init__(self, indices_dir):
        self.values_file = open(os.path.join(indices_dir, "main_index_values.pickle"), "rb")
        self.keys_db = shelve.open(os.path.join(indices_dir, "main_index_keys.db"), writeback=False)
        self.cache = {}
        self.cache_key2time = {}
        self.cache_time2key = {}
    
    def update_cache(self, key, value=None):
        import heapq
        import datetime
        cur_time = str(datetime.datetime.now())
        if key in self.cache:
            last_time = self.cache_key2time[key]
            del self.cache_time2key[last_time]
        else:
            self.cache[key] = value
        self.cache_key2time[key] = cur_time
        self.cache_time2key[cur_time] = key
        if len(self.cache) > TWordIndexReader.MAX_CACHE_SIZE:
            time2del = min(self.cache_time2key)
            key2del = self.cache_time2key[time2del]
            del self.cache_time2key[time2del]
            del self.cache_key2time[key2del]
            del self.cache[key2del]
    
    def get_key_data(self, token):
        if token in self.cache:
            word_freq, offset, bloom_filter_dump_size = self.cache[token][-3:]
        elif token in self.keys_db:
            word_freq, offset, bloom_filter_dump_size = self.keys_db[token]
        else:
            word_freq, offset, bloom_filter_dump_size = None, None, None
        return word_freq, offset, bloom_filter_dump_size
    
    def get_values_by_key_data(self, token, word_freq, offset, bloom_filter_dump_size):
        if word_freq == None:
            return numpy.zeros(0), None, 0
        if token in self.cache:
            self.update_cache(token)
            return self.cache[token][:3]       
        self.values_file.seek(offset)
        codes = pickle.load(self.values_file)
        codes = numpy.array(codes, dtype=numpy.int64)
        prob_filter = None
        if bloom_filter_dump_size:
            prob_filter = BloomFilter.fromfile(self.values_file, bloom_filter_dump_size)
        self.update_cache(token, (codes, prob_filter, word_freq, offset, bloom_filter_dump_size))
        return codes, prob_filter, word_freq        
    
    def get_occurences(self, token):
        if not token in self.cache and not token in self.keys_db:
            return numpy.zeros(0), None, 0
        if token in self.cache:
            self.update_cache(token)
            return self.cache[token][:3]
        word_freq, offset, bloom_filter_dump_size = self.keys_db[token]
        return self.get_values_by_key_data(token, word_freq, offset, bloom_filter_dump_size)