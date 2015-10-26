#-*- coding:utf8 -*-
from parsers import TParsersBundle
from ling_utils import TTokenMatch
from ling_utils import CASE_UPPER, CASE_TITLE, CASE_LOWER
from segments_index import TSegmentIndexReader
from word_index import TWordIndexReader
import pickle
import os
import sys
import numpy


        
class TSearchEngineResult(object):
    def __init__(self, segment_id, result_weight, words2select):
        self.result_weight = result_weight
        self.segment_id = segment_id
        self.words2select = words2select

class TSearchEngine(object):
    MAX_WORD_FREQ = 100000
    SEGMENT_SIZE = 32 # words
    MAX_QUERY_SIZE = 5
    CRUDE_FILTER_TRIM_PROPORTION = 0.1
    CRUDE_FILTER_MAX_SELECT = 5000
    STAT_FILTER_CONTEXT = 2
    
    def __init__(self, index_location="./"):
        self.parsers = TParsersBundle()
        self.word_index = TWordIndexReader(index_location)
        self.segment_index = TSegmentIndexReader(index_location)
        self.buffer_ = numpy.zeros((TSearchEngine.MAX_QUERY_SIZE * TSearchEngine.MAX_WORD_FREQ * 2, 2))
    
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
 

    def shortest_span(self, positions):
        position_token_pairs = []
        counts_inside = {}
        for token, token_positions in positions.items():
            counts_inside[token] = len(token_positions)
            for position_data in token_positions:
                position_token_pairs += [(position_data, token)]
        position_token_pairs.sort()
        first = 0
        last = 0
        uniq_tokens_count = len(positions.keys())
        counts_inside = {token: 0 for token in positions.keys()}
        counts_inside[position_token_pairs[first][1]] = 1
        def uniq_tokens_in_span():
            return sum(1 for value in counts_inside.values() if value)
        shortest_span = None
        shortest_span_len = -1
        while first <= len(position_token_pairs) - uniq_tokens_count:
            while uniq_tokens_in_span() < uniq_tokens_count and last + 1 < len(position_token_pairs):
                last += 1
                counts_inside[position_token_pairs[last][1]] += 1
            if uniq_tokens_in_span():
                span_len = last - first + 1
                if not shortest_span or span_len < shortest_span_len:
                    shortest_span = (first, last)
                    shortest_span_len = span_len
            counts_inside[position_token_pairs[first][1]] -= 1
            first += 1
        first, last = shortest_span
        return position_token_pairs[first:last + 1]

    def freq2idf(self, token_freq):
        token_freq = token_freq and token_freq or 0
        return 1.0 / (float(token_freq) + 1.0)
        
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
        max_case_weight = max(position_and_case_weight[1] \
                           for position_and_case_weight, _ in span_word_matches) 
        match_weight = sum(token2idf[token] \
                           for position_and_case_weight, token in span_word_matches)
        query_weight = sum(token2idf[token] for token in query_tokens)
        norm_match_weight = float(match_weight) / (query_weight + 1.0)
        norm_order_weight =  self.get_order_weight(query_tokens, span_word_matches)
        span_len = span_word_matches[-1][0][0] - span_word_matches[0][0][0] + 1.0
        weight = norm_match_weight * norm_order_weight / span_len
        weight *= max_case_weight > 0 and 1.1 or 1.0
        matched_words_count = len(set(token for _, token in span_word_matches))
        return (matched_words_count, weight)
    
    def add_matches_from_stat_filter(self, by_segment, tokens_occurrences):
        """token_occurence = (first 100K occurrences, bloom filter for the other occurrences)"""
        hypos = [token for token, token_occurrences in tokens_occurrences.items() \
                                                    if token_occurrences[1]]
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
                    context_end = min(TSearchEngine.SEGMENT_SIZE, position + TSearchEngine.STAT_FILTER_CONTEXT + 1)
                    positions2check += range(context_start, context_end)
            positions2check = set(positions2check) - set(taken_positions)        
            for hypo in words2check:
                hypo_checks += 1
                for position in positions2check:
                    code = self.match2code(segment_id, position)
                    in_filter = code in tokens_occurrences[hypo][1]
                    if in_filter:
                        by_segment[segment_id].setdefault(hypo, []).append((position, CASE_LOWER))
        
    def get_initial_matches(self, tokens_occurrences, token2idf):
        #TODO: currently not able to properly search queries with duplicated words
        """token_occurences = (first 100K occurrences, bloom filter for the other occurrences)"""
        tokens_occurrences = {token:codes_and_bloomfilter[0] \
                            for token, codes_and_bloomfilter in tokens_occurrences.items()}   
        tokens_occurrences = {token:codes for token, codes in tokens_occurrences.items() 
                                                    if codes.shape[0] > 0}        
        if not tokens_occurrences:
            return {}
        tokens_segments = {token:codes / (4 * TSearchEngine.SEGMENT_SIZE) \
                            for token, codes in tokens_occurrences.items()}
        """ get max segment_id == column_size """
        column_size = 0
        for segments in tokens_segments.values():
            if segments.shape[0]:
                column_size = max(column_size, numpy.amax(segments)  + 1)   
        """ get total weight of each segment """     
        from scipy.sparse import coo_matrix, find
        sum_values = None
        column_indices = numpy.zeros(1) #(always zeros)
        for token, segments in tokens_segments.items():
            column_indices.resize(segments.shape[0])
            weight = token2idf[token]
            values = numpy.repeat([weight], segments.shape[0])
            sub_mat = coo_matrix( (values, (segments, column_indices)  ), shape=(column_size, 1))
            sub_mat = sub_mat.tocsc()
            if sum_values == None:
                sum_values = sub_mat
            else:
                sum_values += sub_mat
        """ segments with weight > 0 """
        segment_ids, _, segment_weights = find(sum_values)        
        """ top CRUDE_FILTER_MAX_SELECT segments"""
        segments_by_weight = segment_ids[segment_weights.argsort()]
        min_row = max(segments_by_weight.shape[0] - TSearchEngine.CRUDE_FILTER_MAX_SELECT, 0)
        selected_segments = segments_by_weight[min_row:]
        
        """ selected segments matches """            
        by_segment = {}
        for token, codes in tokens_occurrences.items():
            segments = tokens_segments[token]
            selected_codes  = codes[numpy.in1d(segments, selected_segments)]
            for code in selected_codes:
                segment_id, position, match_case = self.code2match(code)
                by_segment.setdefault(segment_id, {})
                by_segment[segment_id].setdefault(token, []).append((position, match_case))
        return by_segment 
    
    
    def trim_query_tokens(self, query_tokens, token2freq):
        if len(query_tokens) <= TSearchEngine.MAX_QUERY_SIZE:
            return query_tokens
        freqs = [token2freq[token] for token in query_tokens]
        freqs.sort()
        max_allowed_freq = freqs[TSearchEngine.MAX_QUERY_SIZE - 1]
        trimmed_query_tokens = [token for token in query_tokens if token2freq[token] <= max_allowed_freq]
        #for the case of equal freqs in the tail
        trimmed_query_tokens = trimmed_query_tokens[:TSearchEngine.MAX_QUERY_SIZE]
        return trimmed_query_tokens
    
    """ return list of TSearchEngineResult sorted by weights (high weight first) """
    def search(self, query, query_tokens=[]):
        if not query_tokens:
            if type(query) == unicode:
                query = query.encode("utf8") 
            query_matches = self.parsers.parse_buffer(query)
            query_tokens = [match.token for match in query_matches]
        
        import datetime
        start = datetime.datetime.now()
        if len(query_tokens) > TSearchEngine.MAX_QUERY_SIZE * 2:
            query_tokens = query_tokens[:TSearchEngine.MAX_QUERY_SIZE]
        tokens_key_data = {token:self.word_index.get_key_data(token) for token in set(query_tokens)}
        token2freq = {token:tokens_key_data[token][0]  for token in tokens_key_data.keys()}
        local_token2idf = {token:self.freq2idf(freq) for token, freq in token2freq.items()}
        query_tokens = self.trim_query_tokens(query_tokens, token2freq)
        #to speed-up load
        token_by_value_offset = [(tokens_key_data[token][1], token) for token in query_tokens]
        token_by_value_offset.sort()
        tokens_occurences = {}
        for _, token in token_by_value_offset:
            word_freq, offset, bloom_filter_dump_size = tokens_key_data[token]
            tokens_occurences[token] = self.word_index.get_values_by_key_data(token, word_freq, offset, bloom_filter_dump_size)
        time_upload_words_occurences = int((datetime.datetime.now() - start).total_seconds() * 1000)
        
        start = datetime.datetime.now()
        by_segment = self.get_initial_matches(tokens_occurences, local_token2idf)
        time_initial_matches = int((datetime.datetime.now() - start).total_seconds() * 1000)
        
        start = datetime.datetime.now()
        self.add_matches_from_stat_filter(by_segment, tokens_occurences)
        time_stat_filter = int((datetime.datetime.now() - start ).total_seconds() * 1000)
        
        start = datetime.datetime.now()
        results_with_weights = []
        for segment_id, positions in by_segment.items():
            span_word_matches = self.shortest_span(positions)
            match_weight = self.calc_full_match_weight(span_word_matches, query_tokens, local_token2idf)
            #to have stable order in case if weights will be the same
            match_weight = (match_weight[0], match_weight[1], -float(segment_id))
            span_start, span_end = span_word_matches[0][0][0], span_word_matches[-1][0][0]
            words2select = [(token, position_word_case[0]) for token, token_positions in positions.items() \
                                    for position_word_case in token_positions \
                                        if position_word_case[0] >= span_start and position_word_case[0] <= span_end]
            results_with_weights += [(match_weight, TSearchEngineResult(segment_id, match_weight, words2select))]
        results_with_weights.sort(reverse=True)
        results = [result for _, result in results_with_weights]
        time_assign_weights = int((datetime.datetime.now() - start).total_seconds() * 1000)
        return results, (time_upload_words_occurences, time_initial_matches, time_stat_filter, time_assign_weights)

        
