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
    SEGMENT_SIZE = 64
    #maximum amount to store in index, the rest is in the bloomfilter
    SERVER_LOCATION = "http://5.9.104.49:8081/?q="
    SERVER_LOCATION = "http://127.0.0.1:8080/?q="
    
    def __init__(self, index_location="./"):
        self.parsers = TParsersBundle()
        self.word_index = TWordIndexReader(index_location)
        self.segment_index = TSegmentIndexReader(index_location)


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
        import math
        return 1.0 / math.log(float(token_freq) + 2.0)
        
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
        norm_match_weight = (float(match_weight) +  0.0000001) / (query_weight + 0.0000001)
        norm_order_weight =  self.get_order_weight(query_tokens, span_word_matches)
        span_len = span_word_matches[-1][0][0] - span_word_matches[0][0][0] + 1.0
        weight = norm_match_weight * norm_order_weight / span_len
        weight *= max_case_weight > 0 and 1.1 or 1.0
        matched_words_count = len(set(token for _, token in span_word_matches))
        return (matched_words_count, weight)
      
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
    def search(self, query, query_tokens=[], filter_segments=None):
        if not query_tokens:
            if type(query) == unicode:
                query = query.encode("utf8") 
            query_matches = self.parsers.parse_buffer(query)
            query_tokens = [match.token for match in query_matches]
        
        
        results = []
        if 1:
            import datetime
            start = datetime.datetime.now()
            import urllib2        
            print TSearchEngine.SERVER_LOCATION + ",".join(query_tokens)
            as_text = urllib2.urlopen(TSearchEngine.SERVER_LOCATION + ";".join(query_tokens)).read()
            for segment in as_text.split("}"):
                if not segment:
                    continue
                match_weight_and_segment_id, occurences = segment.split("|")
                segment_id, span_len, relevance = match_weight_and_segment_id.split(":")                
                segment_id, span_len, relevance = int(segment_id), int(span_len), float(relevance)
                words2select = []
                for occurence_str in occurences.split(";"):
                    if not "," in occurence_str:
                        continue
                    pos_in_segment, occ_weight, word_id = occurence_str.split(",")
                    word_id, pos_in_segment, occ_weight = int(word_id), int(pos_in_segment), int(occ_weight)
                    token = query_tokens[word_id]
                    words2select += [(token, pos_in_segment)]
                results += [TSearchEngineResult(segment_id, (span_len, relevance), words2select)]
        time_upload_words_occurences = int((datetime.datetime.now() - start).total_seconds() * 1000)
        time_initial_matches = 0
        time_stat_filter = 0
        time_assign_weights = 0
        
        """
        start = datetime.datetime.now()
        by_segment = self.get_initial_matches(tokens_occurences, local_token2idf, filter_segments=filter_segments)
        time_initial_matches = int((datetime.datetime.now() - start).total_seconds() * 1000)
        
        start = datetime.datetime.now()
        #self.add_matches_from_stat_filter(by_segment, tokens_occurences)
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
        """
        return results, (time_upload_words_occurences, time_initial_matches, time_stat_filter, time_assign_weights)

        
