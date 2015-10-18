#-*- coding:utf8 -*-
import BaseHTTPServer
import sys

class TSearchServer():
    def __init__(self, books_folder, index_folder):
        self.books_folder = books_folder
        self.index_folder = index_folder
        from lib.search_engine import TSearchEngine
        self.search_engine = TSearchEngine(index_location=self.index_folder)
    
    def get_segment_data(self, segment_id):
        obj_id, field_id, start, length = self.search_engine.segment_index.get_segment(segment_id)
        import os
        location = os.path.join(self.books_folder, obj_id, field_id)
        f = open(location, "rb")
        f.seek(start)
        snippet = f.read(length)
        return obj_id, field_id, snippet
    
    def select_words_in_snippet(self, words2select, snippet):
        matches = self.search_engine.parsers.parse_buffer(snippet, "windows-1251")
        to_select = []
        for token, position in words2select:
            to_select += [(matches[position].start, matches[position].start + matches[position].length)]
        to_select.sort()
        for sel_index in xrange(len(to_select) - 1, -1, -1):
            sel_start, sel_end = to_select[sel_index]
            snippet = snippet[:sel_start] + "<b>" + snippet[sel_start:sel_end] + "</b>" + snippet[sel_end:]
        snippet = snippet.decode("windows-1251").replace(chr(13), " ").replace(chr(10), " ").replace('"', "'")   
        return snippet     
    
    def search(self, query, query_tokens=[]):
        return self.search_engine.search(query=query, query_tokens=query_tokens)

"""
port = int(sys.argv[1])
books_folder = sys.argv[2]
index_folder = sys.argv[3]
"""
port = 8334
books_folder = "/home/arslan/src/ngpedia/books1000"
index_folder ="indices/"


server = TSearchServer(books_folder=books_folder, 
                       index_folder=index_folder)

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
        import datetime
        start_time = datetime.datetime.now()
        match_objects = server.search(query_text)
        delta_ms = int((datetime.datetime.now() - start_time).total_seconds() * 1000)
        response_object = {}
        response_object["count"] = len(match_objects)
        response_object["ms"] = delta_ms
        response_elems = []
        for res_index in xrange(start, start + length):
            if res_index >= len(match_objects):
                break
            result = match_objects[res_index]
            obj_id, field_id, snippet_encoded = server.get_segment_data(result.segment_id)
            snippet = server.select_words_in_snippet(result.words2select, snippet_encoded)
            response_elem = {"index:": res_index, 
                             "obj_id": obj_id, 
                             "field_id": field_id, 
                             "snippet": snippet}
            response_elems.append(response_elem)
        response_object["results"] = response_elems
        import json
        callback_name = query.has_key("callback") and query["callback"][0] or ""
        response_str = json.dumps(response_object, ensure_ascii=False)
        if callback_name:
            response_str = callback_name + "(" + response_str + "}"        
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
