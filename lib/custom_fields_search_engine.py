#-*- coding:utf8 -*-
from parsers import TParsersBundle
from ling_utils import TTokenMatch
from ling_utils import CASE_UPPER, CASE_TITLE, CASE_LOWER
from ling_utils import span_tokenize_windows1251, unify_word
from segments_index import TSegmentIndexReader
from crawler import *
from collections import namedtuple


import pickle
import os
import sys
import numpy


def normalize_udc(udc_str):
    udc_chain = []
    for item in udc_str.split("."):
        if not item.strip():
            continue
        try:
            udc_chain += [int(item)]
        except:
            continue
    return udc_chain

def get_surname(author_str_windows1251):
    words = [unify_word(match[-1].decode("windows-1251")) for match in span_tokenize_windows1251(author_str_windows1251)]  
    if not words:
        return ""
    surname = max((len(word), word)  for word in words)[1]
    return surname


TBook = namedtuple('TBooks', ['title', 'author', 'udc', 'year', 'pages_count', 'lib_sections'], verbose=False)


class TCustomFieldsSearchEngine(object):
    def add_title(self, title, object_id):
        for match in span_tokenize_windows1251(title):
            token = unify_word(match[-1].decode("windows-1251"))
            self.title_index.setdefault(token, []).append(object_id)
    
    def find_title(self, title_query):
        matched_objects = None
        for match in span_tokenize_windows1251(title_query):
            token = unify_word(match[-1].decode("windows-1251"))
            if not token in self.title_index:
                return []
            if matched_objects == None:
                matched_objects = set(self.title_index[token])
            else:
                matched_objects &= set(self.title_index[token])
            if not matched_objects:
                return []
        return matched_objects
    
    def add_year(self, year, object_id):
        try:
            year = int(year)
        except:
            return
        self.year_index.setdefault(int(year), []).append(object_id)
        
    def find_year(self, query_year, search_lower=False, search_bigger=False):
        try:
            query_year = int(query_year)
        except:
            return []
        matched_objects = []
        for year, objects in self.year_index.items():
            if year == query_year or \
                    search_lower and year < query_year or \
                    search_bigger and year > query_year:
                matched_objects += objects
        return matched_objects
    
    def add_udc(self, udc_str, object_id):
        udc_array = normalize_udc(udc_str)
        key = "|" + "|".join([str(number) for number in udc_array]) + "|"
        if udc_array:
            self.udc_index.setdefault(udc_array[0], []).append((key, object_id))
    
    def find_udc(self, query_udc_str):
        udc_array = normalize_udc(query_udc_str)
        query_key = "|" + "|".join([str(number) for number in udc_array]) + "|"
        if not udc_array:
            return []
        object_ids = []
        if udc_array[0] in self.udc_index:
            for key, object_id in self.udc_index[udc_array[0]]:
                if key.startswith(query_key):
                    object_ids.append(object_id)
        return object_ids
    
    def add_author(self, author_str_windows1251, object_id):
        surname = get_surname(author_str_windows1251)
        if surname:
            self.author_index.setdefault(surname, []).append(object_id)
    
    def find_author(self, author_str_windows1251):
        surname = get_surname(author_str_windows1251)
        if surname in self.author_index: 
            return self.author_index[surname]
        return []
            
    def add_pages_count(self, pages_count, object_id):
        try:
            pages_count = int(pages_count)
            self.pages_index.setdefault(pages_count, []).append(object_id)
        except:
            pass
        
    def find_pages_count(self, query_page_count, search_lower=False, search_bigger=False):
        try:
            query_page_count = int(query_page_count)
        except:
            return []
        matched_objects = []
        for page_count, objects in self.pages_index.items():            
            if page_count == query_page_count or \
                    search_lower and page_count < query_page_count or \
                    search_bigger and page_count > query_page_count:
                matched_objects += objects
        return matched_objects
    
    def add_lib_sections(self, lib_sections, object_id):
        for lib_section in lib_sections:
            self.lib_section_index.setdefault(lib_section, []).append(object_id)
    
    def find_lib_section(self, query_lib_section):
        if not query_lib_section:
            return []
        try:
            query_lib_section = int(query_lib_section)
        except:
            return []
        if query_lib_section in self.lib_section_index:
            return self.lib_section_index[query_lib_section]
        else:
            return []
            
    
    
    def __init__(self, csv_file):
        self.title_index = {}
        self.year_index = {}
        self.udc_index = {}
        self.author_index = {}
        self.pages_index = {}
        self.lib_section_index = {}
        self.objects = {}
        
        title, author, udc, year, pages_count, lib_sections = "", "", "", "", "", ()
        
        for object in TCrawler().crawl_csv(csv_file):
            object.object_id = int(object.object_id)
            for field in object.object_fields:
                key = field.field_id
                value = field.field_value
                if key == "year":
                    try:
                        year = int(value)
                    except:
                        year = -1
                    self.add_year(year, object.object_id)
                elif key == "udc":
                    udc = value
                    self.add_udc(udc, object.object_id)
                elif key == "pages_count":
                    try:
                        pages_count = int(value)
                    except:
                        pages_count = -1
                    self.add_pages_count(pages_count, object.object_id)
                    pass
                elif key == "author":
                    author = value
                    self.add_author(author, object.object_id)
                    pass
                elif key == "title":
                    title = value
                    self.add_title(title, object.object_id)
                elif key == LIB_SECTION_FIELD:
                    lib_sections = tuple(value)
                    self.add_lib_sections(lib_sections, object.object_id)
            self.objects[object.object_id] = TBook(title, author, udc, year, pages_count, lib_sections)
    
    def process_query(self, 
                            title="", 
                            author="", 
                            udc="", 
                            year="",
                            year_max="",
                            year_min="",
                            pages_count="", 
                            pages_count_max="", 
                            pages_count_min="",
                            lib_section=""):
        objects = []
        if title:
            objects += [self.find_title(title.encode("windows-1251"))]
        if author:
            objects += [self.find_author(author.encode("windows-1251"))]
        if udc:
            objects += [self.find_udc(udc)]
        if year:
            objects += [self.find_year(year)]
        if year_max:
            objects += [self.find_year(year_max, search_lower=True)]
        if year_min:
            objects += [self.find_year(year_min, search_bigger=True)]
        if pages_count:
            objects += [self.find_pages_count(pages_count)]
        if pages_count_max:
            objects += [self.find_pages_count(pages_count_max, search_lower=True)]
        if pages_count_min:
            objects += [self.find_pages_count(pages_count_min, search_bigger=True)]
        if lib_section:
            objects += [self.find_lib_section(lib_section)]
        if not objects:
            return 0
        
        cross_product = None
        for objects_set in objects:
            if not objects_set:
                return -1
            if cross_product == None:
                cross_product = set(objects_set)
            else:
                cross_product &= set(objects_set)
            if not cross_product:
                return -1
        return cross_product

                     
    
"""
print "start"
index = TCustomFieldsSearchEngine("/home/arslan/src/ngpedia/search_system/books.csv")
print "uploaded", len(index.objects)

import datetime
print "query"
start = datetime.datetime.now()
objects = index.process_query(author=u"юрьева")

print len(objects), (datetime.datetime.now() - start)
if 1:
    for object_id in objects:
        title = index.objects[object_id].title
        author = index.objects[object_id].author    
        udc = index.objects[object_id].udc        
        year = index.objects[object_id].year    
        pages_count = index.objects[object_id].pages_count
        lib_sections = index.objects[object_id].lib_sections
                          
        print author.decode("windows-1251"), "||", title.decode("windows-1251"), "||", udc, "||", year, "||", pages_count, "||", lib_sections
"""