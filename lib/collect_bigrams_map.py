#-*- coding:utf8 -*-
from lib.crawler import TCrawler
from lib.search_engine import TSearchEngine
from segments_index import TSegmentIndexWriter
from lib.utils import TCustomCounter
from lib.ling_utils import TTokenMatch
from lib.ling_utils import CASE_UPPER, CASE_TITLE, CASE_LOWER
from lib.ling_utils import span_tokenize_windows1251
from lib.ling_utils import unify_word
import sys
import os


REDUCERS_COUNT = 100
REDUCERS_FILES_BUFFER = 1000000 #1MB

def collect_bigrams_map(books_dir, intermid_results_dir, log_out):
    #clear intermid_results_dir
    for fname in os.listdir(intermid_results_dir):
        fname = os.path.join(intermid_results_dir, fname)
        if os.path.isfile(fname):
            os.remove(fname)
    
    reducers_fnames = [os.path.join(intermid_results_dir, "pool_" + str(reducer_index) + ".txt")
                                                    for reducer_index in xrange(REDUCERS_COUNT)]
    reducers_pool = [open(reducer_fname, "wb", buffering=REDUCERS_FILES_BUFFER) 
                                            for reducer_fname in reducers_fnames]
    words_in_buffer = [0]
    words_index = [{}]
    MAX_BUFFER_SIZE = 10000000
    
    def flush_buffer():
        log_out.write("flushing buffer..\n")
        log_out.flush()
        for token, values in words_index[0].items():
            reducer_index = abs(hash(token)) % len(reducers_pool)
            reducers_pool[reducer_index].write(token + "\t" + " ".join(value for value in values) + "\n")
        words_index[0] = {}
        words_in_buffer[0] = 0
    
    def to_word_index(token, next_token):
        words_index[0].setdefault(token, []).append(next_token)
        words_in_buffer[0] += 1
        if words_in_buffer[0] >= MAX_BUFFER_SIZE:
            flush_buffer()
    
    crawler = TCrawler(verbosity=0)
    books_counter = TCustomCounter("ParsedBooks", log_out, verbosity=1, interval=100)
    def process_book(book_obj):
        obj_id = book_obj.object_id
        for field in book_obj.object_fields:
            field_id = field.field_id
            if field.field_file_path:
                field_value = open(field.field_file_path).read()
            else:
                field_value = field.field_value
            tokens_matches = span_tokenize_windows1251(field_value)
            for first_match_index in xrange(len(tokens_matches) - 1):
                first_token = tokens_matches[first_match_index][-1].decode("windows-1251").lower().encode("windows-1251")
                second_token = tokens_matches[first_match_index + 1][-1].decode("windows-1251").lower().encode("windows-1251")
                to_word_index(first_token, second_token)
        books_counter.add()
    for book_obj in crawler.crawl_folder(books_dir):
        process_book(book_obj)
    flush_buffer()
    for reducer in reducers_pool:
        reducer.close()


