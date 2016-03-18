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
from collections import namedtuple

OBJ_BITS = 20;
SEGMENT_POS_BITS = 6;
WEIGHT_BITS = 2;
NON_SEGMENT_BITS = SEGMENT_POS_BITS + WEIGHT_BITS + OBJ_BITS;

TSegmentMatch = namedtuple('TSegmentMatch', ['id', 'weight', 'words2select'], verbose=False)

        
class TSearchEngineResult(object):
    def __init__(self, object_id, relevance_score, matches_count):
        self.object_id = object_id
        self.object_relevance_score = relevance_score
        self.matches_count = matches_count
        self.segment_matches = []

class TSearchEngine(object):
    SEGMENT_SIZE = 64
    #maximum amount to store in index, the rest is in the bloomfilter
    SERVER_LOCATION = "http://5.9.104.49:8081/?q="
    SERVER_LOCATION = "http://127.0.0.1:8080/?q="
    
    def __init__(self, index_location="./"):
        self.parsers = TParsersBundle()
        self.word_index = TWordIndexReader(index_location)
        self.segment_index = TSegmentIndexReader(index_location)
    
    """ return list of TSearchEngineResult sorted by weights (high weight first) """
    def search(self, query, filter_objects=None, query_tokens=[], first_object2return=0, objects2return=10, ):
        if not query_tokens:
            if type(query) == unicode:
                query = query.encode("utf8") 
            query_matches = self.parsers.parse_buffer(query)
            query_tokens = [match.token for match in query_matches]
        print query_tokens
        
        import urllib2
        objects_suffix =""
        if  (filter_objects != None) and (len(filter_objects) > 0):
            objects_suffix = "&o=" + ",".join(str(object_id) for object_id in filter_objects)
        start_len_suffix = "&start=" + str(first_object2return) + "&len=" + str(objects2return)            
        as_text = urllib2.urlopen(TSearchEngine.SERVER_LOCATION + ",".join(query_tokens) + objects_suffix + start_len_suffix).read()
        
        results = []
        chunks = [chunk.strip() for chunk in as_text.split("<:::>") if chunk.strip()]
        total_results_count = int(chunks[0])
        for object_chunk in chunks[1:]:
            object_data, matches = object_chunk.split("||")
            object_id, object_relevance, matches_count = object_data.split(":")
            object_id, object_relevance, matches_count = int(object_id), float(object_relevance), int(matches_count)
            segment_matches = []
            for match_str in matches.split("}"):
                if not match_str.strip():
                    continue
                match_weight_and_segment_id, occurences = match_str.split("|")
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
                segment_matches.append(TSegmentMatch(segment_id, (span_len, relevance), words2select))
            result = TSearchEngineResult(object_id, object_relevance, matches_count)
            result.segment_matches = segment_matches
            results.append(result)
        
        return results, total_results_count

        
