#-*- coding:utf8 -*-
import BaseHTTPServer
import sys
import socket
from lib.crawler import TCrawler, LIB_SECTION_FIELD


class TSearchServer():
    def __init__(self, books_folder, pages_index_folder, csv_path, cfields_index_folder):
        self.books_folder = books_folder
        self.pages_index_folder = pages_index_folder
        self.csv_path = csv_path
        self.cfields_index_folder = cfields_index_folder
        from lib.search_engine import TSearchEngine
        self.pages_search_engine = TSearchEngine(index_location=self.pages_index_folder)
        self.cfields_search_engine = TSearchEngine(index_location=self.cfields_index_folder)
        self.object_cfields = {}
        self.upload_csv_data_(self.csv_path)
    
    def upload_csv_data_(self, csv_path):
        crawler = TCrawler()
        self.object_cfields = {}
        for book in crawler.crawl_csv(csv_path):
            self.object_cfields[book.object_id] = {}
            for field in book.object_fields:
                self.object_cfields[book.object_id][field.field_id] = field.field_value
    """ fast """
    def get_pages_obj_id(self, segment_id):
        return self.pages_search_engine.segment_index.get_obj_id_by_segment_id(segment_id)
    
    """ fast """
    def get_cfields_obj_id(self, segment_id):
        return self.cfields_search_engine.segment_index.get_obj_id_by_segment_id(segment_id)
    
    """ fast: numpy array of segment ids """
    def get_pages_obj_id_segments(self, obj_id):
        return self.pages_search_engine.segment_index.get_segments_by_obj_id(obj_id)
    
    """ fast: numpy array of segment ids """
    def get_cfields_obj_id_segments(self, obj_id):
        return self.cfields_search_engine.segment_index.get_segments_by_obj_id(obj_id)    
    
    def get_pages_segment_data(self, segment_id):
        obj_id, field_id, start, length = self.pages_search_engine.segment_index.get_segment(segment_id)
        import os
        location = os.path.join(self.books_folder, obj_id, field_id)
        f = open(location, "rb")
        f.seek(start)
        snippet = f.read(length)
        return obj_id, field_id, snippet

    def get_cfields_segment_data(self, segment_id):
        obj_id, field_id, start, length = self.cfields_search_engine.segment_index.get_segment(segment_id)
        snippet = self.object_cfields[obj_id][field_id]
        return obj_id, field_id, snippet
    
    def select_words_in_snippet(self, words2select, snippet):
        matches = self.pages_search_engine.parsers.parse_buffer(snippet, "windows-1251")
        to_select = []
        for token, position in words2select:
            to_select += [(matches[position].start, matches[position].start + matches[position].length)]
        to_select.sort()
        for sel_index in xrange(len(to_select) - 1, -1, -1):
            sel_start, sel_end = to_select[sel_index]
            snippet = snippet[:sel_start] + "<b>" + snippet[sel_start:sel_end] + "</b>" + snippet[sel_end:]
        snippet = snippet.decode("windows-1251").replace(chr(13), " ").replace(chr(10), " ").replace('"', "'")
        import re
        snippet = re.subn("\s+", " ", snippet)[0]
        return snippet
    
    def search(self, query, query_tokens=[], #filter params:
                 filter_field_type=None,
                 filter_obj_id=None, min_year=None, 
                 max_year=None, filter_year=None, 
                 max_pages_count=None, min_pages_count=None,
                 filter_library_section_code=None):
        if filter_obj_id != None:
            cfields_segments, pages_segments = self.get_cfields_obj_id_segments(filter_obj_id), \
                                               self.get_pages_obj_id_segments(filter_obj_id)
        else:
            cfields_segments, pages_segments = None, None
        if filter_field_type != None and filter_field_type == "pages":
            cfields_results, cfields_timing = [], []
        else:
            cfields_results, cfields_timing = self.cfields_search_engine.search(query=query, 
                                                                                query_tokens=query_tokens, 
                                                                                filter_segments=cfields_segments)
        if filter_field_type != None and filter_field_type != "pages":
            pages_results, pages_timing = [], []
        else:
            pages_results, pages_timing = self.pages_search_engine.search(query=query, 
                                                                          query_tokens=query_tokens,
                                                                          filter_segments=pages_segments)
        def filter_match(obj_id):
            try:
                year = int(self.object_cfields[obj_id]["year"])
            except:
                year = 0
            try:
                pages_count = int(self.object_cfields[obj_id]["pages_count"])
            except:
                pages_count = 0
            try:
                library_section_codes = self.object_cfields[obj_id][LIB_SECTION_FIELD]
            except:
                library_section_codes = []
            if filter_obj_id != None and obj_id != filter_obj_id:
                return False
            if filter_year != None and filter_year != year:
                return False
            if max_year != None and max_year < year:
                return False
            if min_year != None and min_year > year:
                return False
            if max_pages_count != None and max_pages_count < pages_count:
                return False
            if min_pages_count != None and min_pages_count > pages_count:
                return False
            if filter_library_section_code != None and not filter_library_section_code in library_section_codes:
                return False
            return True
        # filter by custom field type
        if filter_field_type != None and filter_field_type != "pages":
            segments = [result.segment_id for result in cfields_results]
            segments = [segment_id for segment_id in set(segments)]
            segments.sort()
            segment2field = {segment_id : self.cfields_search_engine.segment_index.get_segment(segment_id)[1] 
                                            for segment_id in segments}
            cfields_results = [result for result in cfields_results \
                                            if segment2field[result.segment_id] == filter_field_type]
            
        # filter by object features
        if      filter_obj_id != None or \
                max_year != None or \
                min_year != None or \
                max_pages_count !=None or \
                min_pages_count != None or \
                filter_library_section_code != None:
            cfields_results = [result for result in cfields_results \
                                    if filter_match(self.get_cfields_obj_id(result.segment_id))]
            pages_results = [result for result in pages_results \
                                    if filter_match(self.get_pages_obj_id(result.segment_id))]
        return [(cfields_results, cfields_timing), (pages_results, pages_timing) ]
        
MACHINE_NETWORK_NAME = socket.gethostbyname(socket.gethostname())


port = int(sys.argv[1])
books_folder = sys.argv[2]
pages_index_folder = sys.argv[3]
csv_path = sys.argv[4]
cfields_index_folder = sys.argv[5]
"""
port = 8334
books_folder = "/home/arslan/src/ngpedia/books1000"
pages_index_folder ="indices/"
csv_path = "/home/arslan/src/ngpedia/search_system/books.csv"
cfields_index_folder = "/home/arslan/src/ngpedia/search_system/custom_fields_indices/"
"""



server = TSearchServer(books_folder=books_folder, 
                       pages_index_folder=pages_index_folder, 
                       csv_path=csv_path, 
                       cfields_index_folder=cfields_index_folder)

"""
custom_fields_matches, pages_matches = server.search(u"или по также или не")
print pages_matches[1]
exit()
"""

form_html = """
<html>
<head>
<meta charset="utf-8">
<title>Search page</title>
</head>
<body>
<form name="search" action="http://%s:%d/" method="get">
  <input type="text" name="q" length="20"/>
  <input type="submit" value="search"/>
</form>
<pre>
##RESULT##
</pre>
</body>
</html>
""" % (MACHINE_NETWORK_NAME, port)

class TGetHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    def do_GET(self):
        full_query = self.path
        full_query = full_query.replace("?callback=", "&callback=")
        import urlparse
        query = urlparse.parse_qs(urlparse.urlparse(full_query).query)
        response = ""
        query_text = query.has_key("q") and query["q"][0] or ""
        try:
            start = int(query["start"][0])
        except:
            start = 0
        try:
            length = int(query["len"][0])
        except:
            length = 10
        try:
            add_fields = set([item.strip() for item in query["add"][0].split(",") if item.strip()])
        except:
            add_fields = set()
        """ filter params """
        filter_field_type = None
        filter_obj_id = None
        min_year = None
        max_year = None
        filter_year = None
        max_pages_count = None
        min_pages_count = None
        library_section_code = None
        try:
            filter_field_type = query["field"][0]
        except:
            filter_field_type = None
        try:
            filter_obj_id = query["obj_id"][0]
        except:
            filter_obj_id = None
        try:
            min_year = int(query["min_year"][0])
        except:
            min_year = None
        try:
            max_year = int(query["max_year"][0])
        except:
            max_year = None
        try:
            filter_year = int(query["year"][0])
        except:
            filter_year = None 
        try:
            max_pages_count = int(query["max_pcount"][0])
        except:
            max_pages_count = None          
        try:
            min_pages_count = int(query["min_pcount"][0])
        except:
            min_pages_count = None    
        try:
            library_section_code = int(query["lib_section"][0])
        except:
            library_section_code = None   
        
        return_json = query.has_key("json")   
        import datetime
        if query_text:
            custom_fields_matches, pages_matches = server.search(query_text, [], 
                                                                 filter_field_type=filter_field_type,
                                                                 filter_obj_id=filter_obj_id, 
                                                                 min_year=min_year, max_year=max_year, 
                                                                 filter_year=filter_year, 
                                                                 max_pages_count=max_pages_count, 
                                                                 min_pages_count=min_pages_count,
                                                                 filter_library_section_code=library_section_code)
            timings = pages_matches[-1]
            custom_fields_matches, pages_matches = custom_fields_matches[0], pages_matches[0]
            #small hack to allow custom fields with same words count be on top
            joined = [((match.result_weight[0]* 1.1, match.result_weight[1], match.result_weight[2]), 1, match) \
                                                 for match in custom_fields_matches]
            joined += [(match.result_weight, 0, match)  for match in pages_matches]
            joined.sort(reverse=True)
            match_objects = [(match_type, match) for _, match_type, match in joined]
        else:
            match_objects, timings = [], (0,0,0,0) 
        response_object = {}
        response_object["count"] = len(match_objects)
        response_object["ms"] = str(timings)
        response_elems = []
        for res_index in xrange(start, start + length):
            if res_index >= len(match_objects):
                break
            is_custom_field, result = match_objects[res_index]
            if not is_custom_field:
                obj_id, field_id, snippet_encoded = server.get_pages_segment_data(result.segment_id)
            else:
                obj_id, field_id, snippet_encoded = server.get_cfields_segment_data(result.segment_id)
            snippet = server.select_words_in_snippet(result.words2select, snippet_encoded)
            response_elem = {"index": res_index, 
                             "obj_id": obj_id, 
                             "field_id": field_id, 
                             "snippet": snippet, 
                             "weight": result.result_weight}
            if add_fields:
                add_data_dict = {}
                for field_id in add_fields:
                    field_value_encoded = field_id in server.object_cfields[obj_id] and server.object_cfields[obj_id][field_id] or ""
                    field_value = type(field_value_encoded) == str and field_value_encoded.decode("windows-1251") or field_value_encoded
                    add_data_dict[field_id] = field_value
                response_elem["add"] = add_data_dict
            response_elems.append(response_elem)
        response_object["results"] = response_elems
        import json
        callback_name = query.has_key("callback") and query["callback"][0] or ""
        response_str = json.dumps(response_object, ensure_ascii=False, indent=(not return_json and 1 or None))
        if callback_name:
            response_str = callback_name + "(" + response_str + "}"      
        if not return_json:
            response_str = form_html.replace("##RESULT##", response_str)
        response_str = response_str.encode("utf8")
        self.send_response(200)
        request_headers = self.headers.__str__().replace(chr(10), " ").replace(chr(13), " ")
        log_line = "[STAT]\tclient:" + str(self.client_address) + "\theaders: " + request_headers + "\tquery:" + query_text.decode("utf8") + "\n"
        sys.stdout.write(log_line.encode("utf8"))
        sys.stdout.flush()
        self.send_header("Content-Length", str(len(response_str)))
        self.end_headers()
        self.wfile.write(response_str)

if __name__ == '__main__':
    server_address = ('', port)
    httpd = BaseHTTPServer.HTTPServer(server_address, TGetHandler)
    httpd.serve_forever()
    print "started"    
