#-*- coding:utf8 -*-
from lib.parsers import TParsersBundle
from lib.index_engine import TSearchEngine
from lib.crawler import TCrawler
from lib.utils import TCustomCounter
from lib.ling_utils import TTokenMatch
from lib.ling_utils import CASE_UPPER, CASE_TITLE, CASE_LOWER
import sys
import os

run_id = sys.argv[1]
file_with_fields_desc = sys.argv[2]
parsers = TParsersBundle()

word_freqs = {}
out = open(run_id + "_matches.txt", "w")
segments_counter = [0]
words_in_buffer = [0]
words_index = [{}]

MAX_BUFFER_SIZE = 10000000

def flush_buffer():
    for token, codes in words_index[0].items():
        out.write(token + "\t" + " ".join(str(code) for code in codes) + "\n")
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
    out.write("[segment] %d %s %s %d %d\n" % (segment_id, str(object_id), str(field_id), segment_start, segment_length))
    segments_counter[0] += 1
    return segment_id

def match2code(segment_id, position, match_weight=0):
    """ position < SEGMENT_SIZE """
    code = segment_id * TSearchEngine.SEGMENT_SIZE + position
    """ consider 3 word case weights (0, 1, 2) """
    code = (code << 2) + match_weight
    return code


fields_counter = TCustomCounter("FieldsCounter", sys.stdout, verbosity=1, interval=1000)
for line in open(file_with_fields_desc):
    obj_id, field_id, fname = line[:-1].split("\t")
    tokens_matches = parsers.parse_file(fname, "windows-1251")
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
out.close()
word_freqs_file = open(run_id + "_word_freqs.txt", "w")
for word, freq in word_freqs.items():
    word_freqs_file.write(word + "\t" + str(freq) + "\n")
word_freqs_file.close()
print "finished."


