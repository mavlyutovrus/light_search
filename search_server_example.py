#-*- coding:utf8 -*-
import BaseHTTPServer
import os
import sys
import socket
from lib.crawler import TCrawler, LIB_SECTION_FIELD
from lib.custom_fields_search_engine import TCustomFieldsSearchEngine
#from lib.custom_fields_search_engine import TBook
from lib.search_engine import TSearchEngine
from lib.search_engine import TSearchEngineResult

#from scipy.constants.constants import year

class TSearchServer():
    def __init__(self, books_folder, pages_index_folder, csv_path):
        self.books_folder = books_folder
        self.pages_index_folder = pages_index_folder
        self.csv_path = csv_path
        self.pages_search_engine = TSearchEngine(index_location=self.pages_index_folder)
        self.cfields_search_engine = TCustomFieldsSearchEngine(csv_path)
        
    def get_pages_segment_data(self, segment_id):
        obj_id, field_id, start, length = self.pages_search_engine.segment_index.get_segment(segment_id)
        import os
        location = os.path.join(self.books_folder, obj_id, field_id)
        f = open(location, "rb")
        f.seek(start)
        snippet = f.read(length)
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
    
    """ returns object of class TBook"""
    def get_book_data(self, object_id):
        return self.cfields_search_engine.objects[int(object_id)]
    
    def search(self, params):
        EMPTY_RESPONSE = [[], 0]
        filtered_object_ids = self.cfields_search_engine.process_query(title=params["title"],
                                                                       author=params["author"],
                                                                       udc=params["udc"],
                                                                       year=params["year"],
                                                                       year_max=params["year_max"],
                                                                       year_min=params["year_min"],
                                                                       pages_count=params["pages_count"] ,
                                                                       pages_count_max=params["pages_count_max"],
                                                                       pages_count_min=params["pages_count_min"],
                                                                       lib_section=params["filter_lib_section"])
        #no books satisfying filters
        if filtered_object_ids == -1:
            return EMPTY_RESPONSE        
        
        if params["filter_object_id"]:
            filter_object_id = int(params["filter_object_id"])
            if filtered_object_ids == 0: # no restrictions introduced
                filtered_object_ids = [filter_object_id]
            elif filter_object_id in filtered_object_ids:
                filtered_object_ids = [filter_object_id]
            else:
                return EMPTY_RESPONSE

        if filtered_object_ids == 0: #all books are accepted
            filtered_object_ids = None
            
        if not filtered_object_ids and not params["pages_query"]: #all accepted
            return EMPTY_RESPONSE
        
        if not params["pages_query"]:# no query to the pages index
            return EMPTY_RESPONSE

        objects_matching_custom_field = self.cfields_search_engine.find_mentions_of_author_and_title(params["pages_query"])
        if filtered_object_ids != None:
            objects_matching_custom_field = [obj_id for obj_id in objects_matching_custom_field \
                                                if obj_id in filtered_object_ids]

        first_object2return = params["start"]
        objects2return = params["len"]

        first_from_custom_matchings = min(len(objects_matching_custom_field), first_object2return)
        take_custom_matchings_count = min(objects2return, max(0, len(objects_matching_custom_field) - first_object2return))

        objects2return = max(0, objects2return - take_custom_matchings_count)
        first_object2return = max(0, first_object2return - len(objects_matching_custom_field))

        total_results_count = take_custom_matchings_count
        joined_results = []
        for obj_id in objects_matching_custom_field[first_from_custom_matchings: first_from_custom_matchings + take_custom_matchings_count]:
            search_result = TSearchEngineResult(obj_id, 0, 0)
            joined_results += [search_result]
        pages_results, pages_results_count = self.pages_search_engine.search(query=params["pages_query"],
                                                                      filter_objects=filtered_object_ids,
                                                                      first_object2return=first_object2return,
                                                                      objects2return=objects2return)
        joined_results += pages_results
        total_results_count += pages_results_count
        return joined_results, total_results_count
        
MACHINE_NETWORK_NAME = socket.gethostbyname(socket.gethostname())

if os.getcwd() == "/home/arslan/src/light_search":
    port = 8334
    books_folder = "/home/arslan/src/ngpedia/books_sample/"
    pages_index_folder = "/home/arslan/src/ngpedia/indices/"
    csv_path = "/home/arslan/src/ngpedia/books.csv"
else:
    if len(sys.argv) < 5:
        print "launch params: <port to serve> <books folder> <index folder> <books.csv location>"
        exit()
    port = int(sys.argv[1])
    books_folder = sys.argv[2]
    pages_index_folder = sys.argv[3]
    csv_path = sys.argv[4]


server = TSearchServer(books_folder=books_folder, 
                       pages_index_folder=pages_index_folder, 
                       csv_path=csv_path) 

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
    def parse_query(self, full_query):
        import urlparse
        query = urlparse.parse_qs(urlparse.urlparse(full_query).query)
        
        internal_key2query_param  = {"start": "start", 
                                     "len" : "len", 
                                     "filter_object_id":"obj_id",
                                     "filter_lib_section":"lib_section",
                                     "add": "add", 
                                     "pages_query":"q",
                                     "title":"title",
                                     "author":"author",
                                     "udc":"udc", 
                                     "year":"year", 
                                     "year_max":"max_year",
                                     "year_min":"min_year",
                                     "pages_count":"NONE",
                                     "pages_count_max":"max_pcount",
                                     "pages_count_min":"min_pcount",
                                     "snippets_per_object": "snps",
                                     "json" : "json"}
        integer_params = ["start", "len", 
                          "filter_object_id", "filter_lib_section", 
                          "year", "year_min", "year_max", 
                          "pages_count", "pages_count_max","pages_count_min", "snippets_per_object"]
        
        default_values = {"start" : 0, "len" : 10, "add" : "author, title", "filter_object_id":None, "snippets_per_object": 1}
        
        params = {} 
        for key, query_param in internal_key2query_param.items():
            params[key] = ""
            if key in default_values:
                params[key] = default_values[key]
            if query_param in query:
                value = query[query_param][0].decode("utf8")
                if key in integer_params:
                    try:
                        value = int(value)
                    except:
                        continue
                params[key] = value
        return params
        
    
    
    def do_GET(self):
        full_query = self.path
        full_query = full_query.replace("?callback=", "&callback=")
        query_params = self.parse_query(full_query)

        snippets_per_object = query_params["snippets_per_object"]
        match_objects, total_relevant_objects = server.search(query_params)
        if match_objects == -1:
            match_objects = []
        response_object = {}
        response_object["count"] = total_relevant_objects
        response_elems = []
        for res_index in xrange(len(match_objects)):
            result = match_objects[res_index]
            book_info = server.get_book_data(result.object_id)
            abs_result_index = res_index + query_params["start"]
            book_data = {
                         "title" : book_info.title.decode("windows-1251"), "author": book_info.author.decode("windows-1251"), 
                         "udc" : book_info.udc, "year": book_info.year, 
                         "pages count" : book_info.pages_count, "lib_sections" : book_info.lib_sections }
            
            top_segments = []
            for segment_match_index in xrange(min(snippets_per_object, len(result.segment_matches))):
                segment_match = result.segment_matches[-segment_match_index - 1]
                obj_id_str, field_id, snippet_encoded = server.get_pages_segment_data(segment_match.id)
                book_data["id"] = obj_id_str
                snippet = server.select_words_in_snippet(segment_match.words2select, snippet_encoded)
                top_segments.append({
                                     "field_id": field_id, 
                                     "weight": segment_match.weight,
                                     "snippet": snippet })
            result = { "result_index" : abs_result_index,
                       "book": book_data,
                       "top_segments": top_segments, 
                       "cumul_relev": result.object_relevance_score, 
                       "segments_matched": result.matches_count
                      }
            response_elems += [result]
        response_object["results"] = response_elems
        import json
        return_json = query_params["json"]
        callback_name = query_params.has_key("callback") and query_params["callback"][0] or ""
        response_str = json.dumps(response_object, ensure_ascii=False, indent=(not return_json and 1 or None))
        if callback_name:
            response_str = callback_name + "(" + response_str + "}"      
        if not return_json:
            response_str = form_html.replace("##RESULT##", response_str)
        response_str = response_str.encode("utf8")
        self.send_response(200)
        request_headers = self.headers.__str__().replace(chr(10), " ").replace(chr(13), " ")
        log_line = "[STAT]\tclient:" + str(self.client_address) + "\theaders: " + request_headers + "\tquery:" + full_query + "\n"
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
