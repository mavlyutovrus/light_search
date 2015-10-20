import os
import sys
import shelve
import pickle
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
    
    def get_occurences(self, token):
        if not token in self.keys_db:
            return [], None, 0
        if token in self.cache:
            self.update_cache(token)
            return self.cache[token]
        word_freq, offset, bloom_filter_dump_size = self.keys_db[token]
        self.values_file.seek(offset)
        codes = pickle.load(self.values_file)
        prob_filter = None
        if bloom_filter_dump_size:
            prob_filter = BloomFilter.fromfile(self.values_file, bloom_filter_dump_size)
        self.update_cache(token, (codes, prob_filter, word_freq))
        return codes, prob_filter, word_freq