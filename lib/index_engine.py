#-*- coding:utf8 -*-
from parsers import TParsersBundle
from ling_utils import TTokenMatch
from ling_utils import CASE_UPPER, CASE_TITLE, CASE_LOWER

class TIndexingObjectField(object):
    def __init__(self, field_id, field_value, field_file_path):
        self.field_id = field_id
        self.field_file_path = field_file_path
        self.field_value = field_value

class TIndexingObjectData(object):
    def __init__(self, object_id, object_fields):
        self.object_id = object_id
        self.object_fields = object_fields
        
class TSearchEngineResult(object):
    def __init__(self, segment_id, result_weight, words2select):
        self.result_weight = result_weight
        self.segment_id = segment_id
        self.words2select = words2select

class TSearchEngine(object):
    """if a word has a higher frequency in the dataset, 
        all occurrences after that amount will be served by  BloomFilter"""
    MAX_WORD_FREQ = 100000
    MAX_QUERY_SIZE = 5
    SEGMENT_SIZE = 256
    CRUDE_FILTER_TRIM_PROPORTION = 0.1
    CRUDE_FILTER_MAX_SELECT = 1000
    STAT_FILTER_CONTEXT = 2
    
    def __init__(self, index_location="./", readonly=True):
        import os
        import shelve
        from pybloom import ScalableBloomFilter
        self.readonly = readonly
        self.index_location = index_location
        self.parsers = TParsersBundle()
        self.word_freqs = shelve.open(os.path.join(index_location, "word_freqs.db"), writeback= (not readonly))
        self.word_index = shelve.open(os.path.join(index_location, "main_index.db"), writeback= (not readonly))
        self.segment_index = shelve.open(os.path.join(index_location, "segments.db"), writeback= (not readonly))
        #TODO: serialize counter
        self.segments_count = readonly and -1 or len(self.segment_index)
        self.filter_location = os.path.join(index_location, "prob_filter.db")
        if os.path.exists(self.filter_location):
            self.prob_filter = ScalableBloomFilter.fromfile(open(self.filter_location, "rb"))  
        else:
            self.prob_filter = ScalableBloomFilter(error_rate=0.01, mode=ScalableBloomFilter.LARGE_SET_GROWTH)      
    
    def save_updates(self):
        if self.readonly:
            return
        self.flush_word_index_buffer()
        self.word_index.sync()
        self.word_freqs.sync()
        self.segment_index.sync()
        out = open(self.filter_location, "wb")
        self.prob_filter.tofile(out)
        out.close()
    
    def __del__(self):
        print "Index is closing."
        self.save_updates()
        self.word_index.close()
        self.word_freqs.close()
        self.segment_index.close()
    
    def add_segment(self, start, length, 
                          field_value, object_id, 
                          field_id, segment_tokens):
        segment_id = self.segments_count
        self.segments_count += 1
        self.segment_index[str(segment_id)] = (object_id, field_id, start, length)
        return segment_id
    
    def hash4prob_filter(self, word, code):
        import hashlib
        return int(hashlib.md5(word + "_" + str(code)).hexdigest(), 16)
    
    def to_probalistic_filter(self, word, code):
        hash_value = self.hash4prob_filter(word, code)
        self.prob_filter.add(hash_value)
    
    def check_coocurence_in_filter(self, word, code):
        hash_value = self.hash4prob_filter(word, code)
        return hash_value in self.prob_filter
    
    def match2code(self, segment_id, position, match_weight=0):
        """ position < SEGMENT_SIZE """
        code = segment_id * TSearchEngine.SEGMENT_SIZE + position
        """ consider 3 word case weights (0, 1, 2) """
        code = (code << 2) + match_weight
        return code
    
    """ much faster than code2match """
    def code2segment_id(self, code):
        return (code >> 2) / TSearchEngine.SEGMENT_SIZE
    
    def code2match(self, code):
        match_weight, code = code % 4, (code >> 2)
        position, segment_id = code % TSearchEngine.SEGMENT_SIZE, code / TSearchEngine.SEGMENT_SIZE
        return segment_id, position, match_weight
        
    def to_word_index(self, token, coordinate):
        self.word_index.setdefault(token, []).append(coordinate)           
        
    def to_index(self, tokens_matches, field_value, indexing_object, field_id):
        """ !!! matches sorted by start value !!! """
        segment_start = 0
        segment_tokens = []
        #dummy match
        tokens_matches += [TTokenMatch(start=len(field_value), 
                                       length=TSearchEngine.SEGMENT_SIZE, 
                                       case=CASE_LOWER, 
                                       token="")]
        for match_index in xrange(len(tokens_matches)):
            is_dummy = match_index == len(tokens_matches) - 1
            match = tokens_matches[match_index]
            token_start = match.start
            if token_start - segment_start >= TSearchEngine.SEGMENT_SIZE or is_dummy:
                segment_length = token_start - segment_start
                segment_id = self.add_segment(segment_start, segment_length,
                                 field_value, indexing_object.object_id, 
                                 field_id, segment_tokens)
                for segment_match_index in xrange(len(segment_tokens)):
                    segment_match = segment_tokens[segment_match_index]
                    segment_token = segment_match.token
                    offset_in_segment = segment_match.start - segment_start
                    self.word_freqs.setdefault(segment_token, 0)
                    self.word_freqs[segment_token] += 1
                    word_case_weight = segment_match.case == CASE_UPPER and 2 or segment_match.case == CASE_TITLE and 1 or 0
                    if self.word_freqs[segment_token] > TSearchEngine.MAX_WORD_FREQ:
                        code = self.match2code(segment_id, segment_match_index)
                        self.to_probalistic_filter(segment_token, code)
                    else:
                        code = self.match2code(segment_id, segment_match_index, word_case_weight)
                        self.to_word_index(segment_token, code)
                segment_tokens = []
                segment_start = token_start
            segment_tokens.append(match)

    def index_object(self, indexing_object):
        import datetime
        for field in indexing_object.object_fields:
            tokens_matches = []
            field_value = ""
            if field.field_value:
                tokens_matches = self.parsers.parse_buffer(field.field_value)
                field_value = field.field_value
            elif field.field_file_path:
                tokens_matches = self.parsers.parse_file(field.field_file_path)
                field_value = open(field.field_file_path).read()
            if tokens_matches:
                self.to_index(tokens_matches, field_value, indexing_object, field.field_id)
    
    def shortest_span(self, positions):
        counts_inside = {}
        position_token_pairs = []
        for token, positions in positions.items():
            counts_inside[token] = len(positions)
            for position_data in positions:
                position_token_pairs += [(position_data, token)]
        position_token_pairs.sort()
        first = 0
        last = len(position_token_pairs) - 1
        while first < last:
            if counts_inside[position_token_pairs[first][1]] > 1:
                counts_inside[position_token_pairs[first][1]] -= 1
                first += 1
            else:
                break
        while first < last:
            if counts_inside[position_token_pairs[last][1]] > 1:
                counts_inside[position_token_pairs[last][1]] -= 1
                last -= 1
            else:
                break
        return position_token_pairs[first:last + 1]

    def token2idf(self, token):
        return 1.0 / (float(self.word_freqs[token]) + 1.0)
        
    def get_order_weight(self, query_tokens, span_word_matches):
        query_pairs = {}
        total_query_pairs = 1
        for first in xrange(len(query_tokens)):
            for second in xrange(first + 1, len(query_tokens)):
                key = (query_tokens[first], query_tokens[second])
                query_pairs.setdefault(key, 0)
                query_pairs[key] += 1
                total_query_pairs += 1
        matched_pairs = 1
        for first in xrange(len(span_word_matches)):
            for second in xrange(first + 1, len(span_word_matches)):
                key = (span_word_matches[first][1],span_word_matches[second][1])
                if key in query_pairs and query_pairs[key] > 0:
                    query_pairs[key] -= 1
                    matched_pairs += 1
        norm_order_weight = float(matched_pairs) / total_query_pairs
        return norm_order_weight        
    
    def calc_full_match_weight(self, span_word_matches, query_tokens, token2idf):
        match_weight = sum(token2idf[token] + position_and_case_weight[1] \
                           for position_and_case_weight, token in span_word_matches)
        query_weight = sum(token2idf[token] for token in query_tokens)
        norm_match_weight = float(match_weight) / (query_weight + 1.0)
        norm_order_weight =  self.get_order_weight(query_tokens, span_word_matches)
        span_len = span_word_matches[-1][0][0] - span_word_matches[0][0][0] + 1.0
        weight = norm_match_weight * norm_order_weight / span_len
        matched_words_count = len(set(token for _, token in span_word_matches))
        return (matched_words_count, weight)
    
    def add_matches_from_stat_filter(self, by_segment, query_tokens):
        hypos = [token for token in set(query_tokens) \
                 if self.word_freqs[token] > TSearchEngine.MAX_WORD_FREQ]
        hypos = set(hypos)
        hypo_checks = 0
        for segment_id in by_segment.keys():
            found_positions = by_segment[segment_id]
            words2check = hypos - set(found_positions.keys())
            if not words2check:
                continue
            taken_positions = []
            positions2check = []
            for token, token_positions in by_segment[segment_id].items():
                for position, word_case in token_positions:
                    taken_positions += [position]
                    context_start = max(0, position - TSearchEngine.STAT_FILTER_CONTEXT)
                    context_end = position + TSearchEngine.STAT_FILTER_CONTEXT + 1
                    positions2check += range(context_start, context_end)
            positions2check = set(positions2check) - set(taken_positions)        
            for hypo in words2check:
                hypo_checks += 1
                for position in positions2check:
                    code = self.match2code(segment_id, position)
                    in_filter = self.check_coocurence_in_filter(hypo, code)
                    if in_filter:
                        by_segment[segment_id].setdefault(hypo, []).append((position, CASE_LOWER))
        
    def get_initial_matches(self, query_tokens, token2idf):
        #TODO: currently not able to properly search queries with duplicated words
        query_tokens = set(query_tokens)
        tokens_occurences = [(token2idf[query_token], query_token, self.word_index[query_token], ) \
                                                    for query_token in query_tokens]
        tokens_occurences.sort(reverse=True) #max weight first
        max_possible_gain = sum(token_weight for token_weight, _, _ in tokens_occurences)
        add_segments = True
        by_segment_crude_weight = {}
        max_weight = 0.0
        for token_weight, token, codes in tokens_occurences:
            prev_segment_id = -1
            for code in codes:
                segment_id = self.code2segment_id(code)
                if segment_id != prev_segment_id:
                    if add_segments or segment_id in by_segment_crude_weight:
                        by_segment_crude_weight.setdefault(segment_id, 0.0)
                        by_segment_crude_weight[segment_id] += token_weight
                        max_weight = max(max_weight, by_segment_crude_weight[segment_id])
                    prev_segment_id = segment_id
            max_possible_gain -= token_weight
            if by_segment_crude_weight and max_possible_gain < TSearchEngine.CRUDE_FILTER_TRIM_PROPORTION * max_weight or\
                    max_possible_gain < token_weight and len(by_segment_crude_weight) >= TSearchEngine.CRUDE_FILTER_MAX_SELECT:
                add_segments = False
        by_segment_crude_weight = [(weight, segment_id) for segment_id, weight in by_segment_crude_weight.items() \
                                                if max_weight * TSearchEngine.CRUDE_FILTER_TRIM_PROPORTION < weight]
        by_segment_crude_weight.sort(reverse=True)
        by_segment = {}
        for _, segment_id in by_segment_crude_weight[:TSearchEngine.CRUDE_FILTER_MAX_SELECT]:
            by_segment[segment_id] = {}        
        for _, token, codes in tokens_occurences:
            for code in codes:
                segment_id = self.code2segment_id(code)
                if segment_id in by_segment:
                    segment_id, position, match_case = self.code2match(code)
                    by_segment[segment_id].setdefault(token, []).append((position, match_case))
        return by_segment 
    
    """ return list of TSearchEngineResult sorted by weights (high weight first) """
    def search(self, query):        
        query_matches = self.parsers.parse_buffer(query.encode("utf8"))
        query_tokens = [match.token for match in query_matches]
        query_tokens = query_tokens[:TSearchEngine.MAX_QUERY_SIZE]
        local_token2idf = {token:self.token2idf(token)  for token in query_tokens}
        
        by_segment = self.get_initial_matches(query_tokens, local_token2idf)
        self.add_matches_from_stat_filter(by_segment, query_tokens)
        
        results_with_weights = []
        for segment_id, positions in by_segment.items():
            span_word_matches = self.shortest_span(positions)
            match_weight = self.calc_full_match_weight(span_word_matches, query_tokens, local_token2idf)
            span_start, span_end = span_word_matches[0][0][0], span_word_matches[-1][0][0]
            words2select = []
            for token, positions in positions.items():
                words2select += [(position, token) for position in positions \
                                    if position >= span_start and position <= span_end]        
            words2select.sort()
            results_with_weights += [(match_weight, TSearchEngineResult(segment_id, match_weight, words2select))]
        results_with_weights.sort(reverse=True)
        results = [result for _, result in results_with_weights]
        return results

        