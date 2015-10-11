import os
import sys
import shelve
import pickle
from pybloom import BloomFilter


class TWordIndexReader(object):
    def __init__(self, indices_dir):
        self.values_file = open(os.path.join(indices_dir, "main_index_values.pickle"), "rb")
        self.keys_db = shelve.open(os.path.join(indices_dir, "main_index_keys.db"), writeback=False)
    
    def get_occurences(self, token):
        if not token in self.keys_db:
            return [], None, 0
        word_freq, offset, bloom_filter_dump_size = self.keys_db[token]
        self.values_file.seek(offset)
        codes = pickle.load(self.values_file)
        prob_filter = None
        if bloom_filter_dump_size:
            prob_filter = BloomFilter.fromfile(self.values_file, bloom_filter_dump_size)
        return codes, prob_filter, word_freq