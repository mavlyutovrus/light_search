#-*- coding:utf8 -*-
import BaseHTTPServer
import os
import sys
import socket
from lib.crawler import TCrawler, LIB_SECTION_FIELD
from lib.custom_fields_search_engine import TCustomFieldsSearchEngine
from lib.search_engine import TSearchEngine

def select_words_in_snippet(words2select, snippet, pages_search_engine):
    matches = pages_search_engine.parsers.parse_buffer(snippet, "windows-1251")
    to_select = []
    title_cases = 0
    for token, position in words2select:
        if matches[position].case > 0:
            title_cases += 1
        to_select += [(matches[position].start, matches[position].start + matches[position].length)]
    to_select.sort()
    for sel_index in xrange(len(to_select) - 1, -1, -1):
        sel_start, sel_end = to_select[sel_index]
        snippet = snippet[:sel_start] + "<b>" + snippet[sel_start:sel_end] + "</b>" + snippet[sel_end:]
    snippet = snippet.decode("windows-1251").replace(chr(13), " ").replace(chr(10), " ").replace('"', "'")
    import re
    snippet = re.subn("\s+", " ", snippet)[0]
    return snippet, title_cases

def get_pages_segment_data(segment_id, pages_search_engine, books_folder):
    obj_id, field_id, start, length = pages_search_engine.segment_index.get_segment(segment_id)
    import os
    location = os.path.join(books_folder, obj_id, field_id)
    f = open(location, "rb")
    f.seek(start)
    snippet = f.read(length)
    return obj_id, field_id, snippet

def generate_simple_referat(keyword, pages_search_engine, books_data_search_engine, books_folder):
    keyword_matches = pages_search_engine.parsers.parse_buffer(keyword.encode("utf8"))
    keyword_tokens = [match.token for match in keyword_matches]

    results, total_results_count = pages_search_engine.search(query=keyword,
                                                                   filter_objects=None,
                                                                   first_object2return=0,
                                                                   objects2return=100)
    all_segments = []
    for res_index in xrange(len(results)):
        result = results[res_index]
        for segment_match in result.segment_matches:
            if len(segment_match.words2select) < len(keyword_tokens):
                continue
            keyword_position = [position for keyword, position in segment_match.words2select]
            selection_size = max(keyword_position) + 1- min(keyword_position)
            if selection_size > len(keyword_tokens) + 1:
                continue
            obj_id_str, field_id, snippet_encoded = get_pages_segment_data(segment_match.id, pages_search_engine, books_folder)
            snippet, title_cases_in_match = select_words_in_snippet(segment_match.words2select, snippet_encoded, pages_search_engine)
            all_segments += [(title_cases_in_match, segment_match.weight, snippet, result.object_id, segment_match)]

    all_segments.sort(reverse=True)
    for title_case_first, weight, snippet, object_id, segment_match in all_segments:
        book_info = books_data_search_engine.objects[object_id]
        book_data = "[" + book_info.author.decode("windows-1251").strip() + " " + book_info.title.decode("windows-1251") + "]"
        print ("<p>" + snippet + "<br/>\n" + book_data + "</p>").encode("utf8")



books_folder = "/home/arslan/src/ngpedia/books_sample/"
pages_index_folder = "/home/arslan/src/ngpedia/indices/"
csv_path = "/home/arslan/src/ngpedia/books.csv"
pages_search_engine = TSearchEngine(index_location=pages_index_folder)
books_data = TCustomFieldsSearchEngine(csv_path)

keywords = [u"сетевые компоненты", u"учебное пособие"]
for keyword in keywords:
    print ("<h1>" + keyword + "</h1>").encode("utf8")
    generate_simple_referat(keyword, pages_search_engine, books_data, books_folder)
    print