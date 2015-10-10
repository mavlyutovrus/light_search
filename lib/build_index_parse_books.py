from lib.parsers import TParsersBundle
from lib.search_engine import TSearchEngine
from lib.crawler import TCrawler
from lib.utils import TCustomCounter
from lib.ling_utils import TTokenMatch
from lib.ling_utils import CASE_UPPER, CASE_TITLE, CASE_LOWER
import sys
import os


def parse_books_(books, matches_file_fname, word_freqs_file_fname, log_file_fname):
    buffering = 1000000 # 1MB file buffers
    matches_file = open(matches_file_fname, "w", buffering=buffering)
    word_freqs_file = open(word_freqs_file_fname, "w", buffering=buffering)
    log_file = open(log_file_fname, "w", buffering=buffering)
    parsers = TParsersBundle()
    word_freqs = {}
    segments_counter = [0]
    words_in_buffer = [0]
    words_index = [{}]
    
    MAX_BUFFER_SIZE = 3000000
    
    def flush_buffer():
        for token, codes in words_index[0].items():
            matches_file.write(token + "\t" + " ".join(str(code) for code in codes) + "\n")
        words_index[0] = {}
        words_in_buffer[0] = 0
        print "buffer flushed"
    
    def to_word_index(token, code):
        words_index[0].setdefault(token, []).append(code)
        words_in_buffer[0] += 1
        if words_in_buffer[0] >= MAX_BUFFER_SIZE:
            flush_buffer()
    
    def add_segment(segment_start, segment_length, object_id, field_id):
        segment_id = segments_counter[0]
        matches_file.write("[segment] %d %s %s %d %d\n" % (segment_id, str(object_id), str(field_id), segment_start, segment_length))
        segments_counter[0] += 1
        return segment_id
    
    def match2code(segment_id, position, match_weight=0):
        """ position < SEGMENT_SIZE """
        code = segment_id * TSearchEngine.SEGMENT_SIZE + position
        """ consider 3 word case weights (0, 1, 2) """
        code = (code << 2) + match_weight
        return code
    
    fields_counter = TCustomCounter("ParsedBooks", log_file, verbosity=1, interval=10)
    
    for book_obj in books:
        obj_id = book_obj.object_id
        for field in book_obj.object_fields:
            field_id = field.field_id
            if field.field_file_path:
                tokens_matches = parsers.parse_file(field.field_file_path, "windows-1251")
            else:
                tokens_matches = parsers.parse_file(field.field_value.encode("utf8"), "utf8")
            for first_match_index in xrange(0, len(tokens_matches), TSearchEngine.SEGMENT_SIZE):
                last_match_index = min(first_match_index + TSearchEngine.SEGMENT_SIZE - 1, 
                                       len(tokens_matches) - 1)
                first_match = tokens_matches[first_match_index] 
                last_match = tokens_matches[last_match_index]
                segment_start = first_match.start
                segment_length = last_match.start + last_match.length - segment_start
                segment_id = add_segment(segment_start, 
                                         segment_length,
                                         obj_id, 
                                         field_id)                    
                for match_index in xrange(first_match_index, last_match_index + 1):
                    segment_match = tokens_matches[match_index]
                    segment_match_index = match_index - first_match_index
                    if segment_match_index >= TSearchEngine.SEGMENT_SIZE:
                        print "FUCKUP", segment_match_index
                    segment_token = segment_match.token
                    offset_in_segment = segment_match.start - segment_start
                    word_freqs.setdefault(segment_token, 0)
                    word_freqs[segment_token] += 1
                    word_case_weight = segment_match.case == CASE_UPPER and 2 or segment_match.case == CASE_TITLE and 1 or 0
                    code = match2code(segment_id, segment_match_index, word_case_weight)
                    to_word_index(segment_token,  code)
        fields_counter.add()
    flush_buffer()
    matches_file.close()
    for word, freq in word_freqs.items():
        word_freqs_file.write(word + "\t" + str(freq) + "\n")
    word_freqs_file.close()


def parse_worker(task):
    books, matches_file_fname, word_freqs_file_fname, log_file_fname = task
    parse_books_(books, matches_file_fname, word_freqs_file_fname, log_file_fname)    

def parse_books(books, intermid_results_dir, processes_number=10):
    books_by_process = []
    files_by_process = []
    step = (len(books) / processes_number) + 1
    pid = 0
    for book_index in xrange(0, len(books), step):
        books_by_process.append(books[book_index:book_index + step])
        matches_fname = os.path.join(intermid_results_dir, str(pid) + "_matches.txt")
        word_freqs_fname = os.path.join(intermid_results_dir, str(pid) + "_word_freqs.txt")
        log_fname = os.path.join(intermid_results_dir, str(pid) + "_log.txt")
        files_by_process.append( (matches_fname, word_freqs_fname, log_fname) )
        pid += 1
    
    tasks = []
    for pid in xrange(len(books_by_process)):
        books_set = books_by_process[pid]
        matches_fname, word_freqs_fname, log_fname = files_by_process[pid]
        tasks += [(books_set, matches_fname, word_freqs_fname, log_fname)]
    
    from multiprocessing import Pool
    pool = Pool(processes_number)
    pool.map(parse_worker, tasks)
    
    words_freq_files = [word_freq_file_fname for _, _, word_freq_file_fname, _ in tasks]
    words_matches_files = [matches_fname for _, matches_fname, _, _ in tasks]
    return words_freq_files, words_matches_files