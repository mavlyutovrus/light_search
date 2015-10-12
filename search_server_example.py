#-*- coding:utf8 -*-

class TSearchServer(object):
    def __init__(self, port, books_folder, index_folder):
        self.books_folder = books_folder
        self.index_folder = index_folder
        from lib.search_engine import TSearchEngine
        self.search_engine = TSearchEngine(index_location=self.index_folder)
    
    def get_segment_snippet(self, segment_id):
        obj_id, field_id, start, length = self.search_engine.segment_index.get_segment(segment_id)
        import os
        location = os.path.join(self.books_folder, obj_id, field_id)
        f = open(location, "rb")
        f.seek(start)
        snippet = f.read(length)
        return snippet
    
    def search(self, query, query_tokens=[]):
        results = self.search_engine.search(query=query, query_tokens=query_tokens)
        to_return = results
        #return to_return
        for result in results[:10]:
            snippet = self.get_segment_snippet(result.segment_id)
            matches = self.search_engine.parsers.parse_buffer(snippet, "windows-1251")
            to_select = []
            for token, position in result.words2select:
                to_select += [(matches[position].start, matches[position].start + matches[position].length)]
            to_select.sort()
            for sel_index in xrange(len(to_select) - 1, -1, -1):
                sel_start, sel_end = to_select[sel_index]
                snippet = snippet[:sel_start] + "[[[" + snippet[sel_start:sel_end] + "]]]" + snippet[sel_end:]
            snippet = snippet.decode("windows-1251").replace(chr(13), " ").replace(chr(10), " ")
            print "->>>", snippet
        return to_return

server = TSearchServer(port=1234, 
                       books_folder="/home/arslan/src/ngpedia/books1000", 
                       index_folder="indices/")

print "start"

#server.search(u"получение этиленгddasddsfликоля")

for line in open("selected_queries.txt"):
    import datetime
    query, fields = line[:-1].split("\t")
    fields = fields.split()
    query_tokens = query.split("_")
    if len(query_tokens) < 5:
        continue
    start = datetime.datetime.now()
    matches = server.search("", query_tokens=query_tokens)
    timedelta = datetime.datetime.now() - start
    found_fields = []
    for match in matches:
        field_id = server.search_engine.segment_index.get_segment(match.segment_id)[1]
        found_fields += [field_id]
    recall = len(set(found_fields) & set(fields)) / float(len(fields))
    print recall, timedelta.total_seconds(), len(fields), query_tokens
