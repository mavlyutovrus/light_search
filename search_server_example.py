#-*- coding:utf8 -*-

class TSearchServer(object):
    def __init__(self, port, books_folder, index_folder):
        self.books_folder = books_folder
        self.index_folder = index_folder
        from lib.index_engine import TSearchEngine
        self.search_index = TSearchEngine(index_location=self.index_folder, readonly=True)
    
    def get_segment_snippet(self, segment_id):
        obj_id, field_id, start, length = self.search_index.segment_index[str(segment_id)]
        import os
        location = os.path.join(self.books_folder, obj_id, field_id)
        f = open(location, "rb")
        f.seek(start)
        snippet = f.read(length)
        return snippet
    
    def search(self, query):
        results = self.search_index.search(query)
        for result in results[:10]:
            snippet = self.get_segment_snippet(result.segment_id)
            matches = self.search_index.parsers.parse_buffer(snippet)
            to_select = []
            for token, position in result.words2select:
                to_select += [(matches[position].start, matches[position].start + matches[position].length)]
            to_select.sort()
            for sel_index in xrange(len(to_select) - 1, -1, -1):
                sel_start, sel_end = to_select[sel_index]
                snippet = snippet[:sel_start] + "[[[" + snippet[sel_start:sel_end] + "]]]" + snippet[sel_end:]
            snippet = snippet.decode("windows-1251").replace(chr(13), " ").replace(chr(10), " ")
            print "->>>", snippet

server = TSearchServer(port=1234, 
                       books_folder="/home/arslan/src/ngpedia/books1000", 
                       index_folder="indices/")



server.search(u"для олефинов корова человек зум рис рис")
