#-*- coding:utf8 -*-
from crawler import TCrawler
#from search_engine import TSearchEngine
from segments_index import TSegmentIndexWriter
from utils import TCustomCounter
#from ling_utils import TTokenMatch
from ling_utils import CASE_UPPER, CASE_TITLE, CASE_LOWER
from ling_utils import span_tokenize_windows1251
from ling_utils import unify_word
import sys
import os



REDUCERS_COUNT = 100
REDUCERS_FILES_BUFFER = 1000000 #1MB

WEIGHT_SHIFT = 2
SEGMENT_POS_SHIFT = 6
BOOK_SHIFT = 20
SEGMENT_SIZE = (1 << SEGMENT_POS_SHIFT)


def match2code(segment_id, book_id, segment_position, match_weight=0):
    
    return  (segment_id         << (WEIGHT_SHIFT + SEGMENT_POS_SHIFT + BOOK_SHIFT)) + \
            (book_id            << (WEIGHT_SHIFT + SEGMENT_POS_SHIFT)) + \
            (segment_position   <<  WEIGHT_SHIFT) + \
             match_weight


""" dir = either path to csv or to folder with books """
def build_index_map(dir, intermid_results_dir, indices_dir, log_out):
    books_dir = ""
    csv_dir = ""
    if os.path.isfile(dir):
        csv_dir = dir
    else:
        books_dir = dir
    
    
    #clear intermid_results_dir
    for fname in os.listdir(intermid_results_dir):
        fname = os.path.join(intermid_results_dir, fname)
        if os.path.isfile(fname):
            os.remove(fname)
    
    reducers_fnames = [os.path.join(intermid_results_dir, "pool_" + str(reducer_index) + ".txt")
                                                    for reducer_index in xrange(REDUCERS_COUNT)]
    reducers_pool = [open(reducer_fname, "wb", buffering=REDUCERS_FILES_BUFFER) 
                                            for reducer_fname in reducers_fnames]
    segments_counter = [0]
    words_in_buffer = [0]
    words_index = [{}]
    MAX_BUFFER_SIZE = 10000000
    
    def flush_buffer():
        log_out.write("flushing buffer..\n")
        log_out.flush()
        for token, codes in words_index[0].items():
            unified_token = unify_word(token.decode("windows-1251"))
            reducer_index = abs(hash(unified_token)) % len(reducers_pool)
            reducers_pool[reducer_index].write(unified_token + "\t" + " ".join(str(code) for code in codes) + "\n")
        words_index[0] = {}
        words_in_buffer[0] = 0
    
    def to_word_index(token, code):
        words_index[0].setdefault(token, []).append(code)
        words_in_buffer[0] += 1
        if words_in_buffer[0] >= MAX_BUFFER_SIZE:
            flush_buffer()

    
    crawler = TCrawler(verbosity=0)
    segment_index_writer = TSegmentIndexWriter(indices_dir)
    books_counter = TCustomCounter("ParsedBooks", log_out, verbosity=1, interval=100)
    
    max_book_id = [0]
    
    def process_book(book_obj):
        obj_id = book_obj.object_id
        max_book_id[0] = max(int(obj_id), max_book_id[0])
        for field in book_obj.object_fields:
            field_id = field.field_id
            if field.field_file_path:
                field_value = open(field.field_file_path).read()
            else:
                field_value = field.field_value
            if not type(field_value) == str:
                print obj_id, ":", field_id, " - not a string value"
                continue
            tokens_matches = span_tokenize_windows1251(field_value)
            for first_match_index in xrange(0, len(tokens_matches), SEGMENT_SIZE):
                last_match_index = min(first_match_index + SEGMENT_SIZE - 1, 
                                       len(tokens_matches) - 1)
                first_match = tokens_matches[first_match_index] 
                last_match = tokens_matches[last_match_index]
                segment_start = first_match[0]
                segment_length = last_match[0] + last_match[1] - segment_start
                segment_id = segment_index_writer.add_segment(obj_id, field_id, segment_start, segment_length)                    
                for match_index in xrange(first_match_index, last_match_index + 1):
                    segment_match = tokens_matches[match_index]
                    segment_match_index = match_index - first_match_index
                    segment_token = segment_match[-1]
                    match_case = segment_match[-2]
                    word_case_weight = match_case == CASE_UPPER and 2 or match_case == CASE_TITLE and 1 or 0
                    code = match2code(segment_id, int(obj_id), segment_match_index, word_case_weight)
                    to_word_index(segment_token,  code)
        books_counter.add()
        
    if books_dir:
        for book_obj in crawler.crawl_folder(books_dir):
            process_book(book_obj)
    elif csv_dir:
        for book_obj in crawler.crawl_csv(csv_dir):
            process_book(book_obj)        
    
    flush_buffer()
    for reducer in reducers_pool:
        reducer.close()
    segment_index_writer.save()
    print "segments count: ", segment_index_writer.segments_count
    print "max book id: ", max_book_id

