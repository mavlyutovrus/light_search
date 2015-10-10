from segments_index import TSegmentIndexWriter
from lib.search_engine import TSearchEngine
import os
import sys
import shelve
from utils import TCustomCounter

MIN_WORD_FREQ_FOR_INDEX = 5
MAX_WORD_FREQ_FOR_INDEX = 100000000 
REDUCERS_COUNT = 100
REDUCERS_FILES_BUFFER = 1000000 #1MB

def join_words_freqs_files(word_dicts_files):
    joined_words_dict = {}
    for dict_fname in word_dicts_files:
        for line in open(dict_fname):
            token, freq = line.split("\t")
            joined_words_dict.setdefault(token, 0)
            joined_words_dict[token] += int(freq)
    return joined_words_dict


def reshuffle(words_freq_files, 
              words_matches_files, 
              indices_dir, 
              intermid_results_dir, 
              log_out):
    log_out.write("join freqs dicts...\n")   
    joined_words_dict = join_words_freqs_files(words_freq_files)
    words2index = {}
    all_matches = 0
    matches2index = 0
    matches2stat_filter = 0
    matches2actual_index = 0
    words_with_stat_filter = 0
    
    for word, freq in joined_words_dict.items():
        all_matches += freq
        if freq < MIN_WORD_FREQ_FOR_INDEX:
            continue
        if freq > MAX_WORD_FREQ_FOR_INDEX:
            log_out.write("..high freq token (skipped): %s, freq: %d\n" % (word, freq))
            continue
        words2index[word] = freq
        matches2index += freq
        matches2stat_filter += max(0, freq - TSearchEngine.MAX_WORD_FREQ)
        words_with_stat_filter += freq > TSearchEngine.MAX_WORD_FREQ and 1 or 0
        matches2actual_index += min(freq, TSearchEngine.MAX_WORD_FREQ)
    
    log_out.write("total words %d\n" % (len(joined_words_dict)))
    log_out.write("words 2 index %d\n\n" % (len(words2index)))
    log_out.write("all matches %d\n" % (all_matches))
    log_out.write("matches 2 index %d\n" % (matches2index))
    log_out.write("--2stat filters %d\n" % (matches2stat_filter))
    log_out.write("--2actual_index %d\n" % (matches2actual_index))    
    log_out.write("words with stat filter %d\n" % (words_with_stat_filter))
    
    del joined_words_dict
    
    """ save words freqs 2 index """
    log_out.write("saving freqs to db...\n")  
    for fname in os.listdir(indices_dir):
        if fname in ["word_freqs.db"]:
            os.remove(os.path.join(indices_dir, fname))
    word_freqs_db = shelve.open(os.path.join(indices_dir, "word_freqs.db"), writeback=True)
    for token, freq in words2index.items():
        word_freqs_db[token] = freq
    word_freqs_db.close()
    
    segment_index_writer = TSegmentIndexWriter(indices_dir)
    
    def match2code(segment_id, position, match_weight=0):
        """ position < SEGMENT_SIZE """
        code = segment_id * TSearchEngine.SEGMENT_SIZE + position
        """ consider 3 word case weights (0, 1, 2) """
        code = (code << 2) + match_weight
        return code
    
    def code2match(code):
        match_weight, code = code % 4, (code >> 2)
        position, segment_id = code % TSearchEngine.SEGMENT_SIZE, code / TSearchEngine.SEGMENT_SIZE
        return segment_id, position, match_weight
    
    progress_counter = TCustomCounter("ProcessedMatchesFiles", log_out, verbosity=1, interval=1)
    
    reducers_fnames = [os.path.join(intermid_results_dir, "pool_" + str(reducer_index) + ".txt")
                                                    for reducer_index in xrange(REDUCERS_COUNT)]
    reduces_pool = [open(reducer_fname, "wb", buffering=REDUCERS_FILES_BUFFER) 
                                            for reducer_fname in reducers_fnames]
    segments_offset = 0
    for matches_filename in words_matches_files:
        last_segment_index = 0
        for line in open(matches_filename):
            if "[segment]" in line:
                _, local_segment_id, object_id, field_id, start, length = line.split()
                local_segment_id, start, length = int(local_segment_id), int(start), int(length)
                global_segment_id = segments_offset + local_segment_id 
                assigned_segment_id = segment_index_writer.add_segment(object_id, field_id, start, length)
                if global_segment_id != assigned_segment_id:
                    raise Exception("global segment id != assigned segment id")
                last_segment_index = max(last_segment_index, local_segment_id)
                continue
            token, codes = line.split("\t")
            if not token in words2index: #do not put low freq words
                continue
            codes = [int(code) for code in codes.split()]
            for code_index in xrange(len(codes)):
                local_segment_id, position, match_weight = code2match(codes[code_index])
                global_segment_id = segments_offset + local_segment_id
                updated_code = match2code(global_segment_id, position, match_weight)
                codes[code_index] = updated_code
            reducer_index = abs(hash(token)) % len(reduces_pool)
            reduces_pool[reducer_index].write(token + "\t" + " ".join(str(code) for code in codes) + "\n")
        segments_offset += last_segment_index + 1
        progress_counter.add()
    
    segment_index_writer.save()
    return reducers_fnames