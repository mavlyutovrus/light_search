from lib.index_engine import TSearchEngine
from lib.crawler import TCrawler
from lib.utils import TCustomCounter
from datetime import datetime
import sys
import os


""" remove existing indices """
books_dir = "/home/arslan/src/ngpedia/books1000"
indices_dir = "indices/"
for fname in os.listdir(indices_dir):
    os.remove(indices_dir + fname)

search = TSearchEngine(index_location=indices_dir, readonly=False)
books_counter = TCustomCounter("BooksCounter", sys.stdout, verbosity=1, interval=1)
crawler = TCrawler(verbosity=1)
print "OBJECT: start start"
books_counter.add()
for indexing_object in crawler.crawl_folder(books_dir):
    print "OBJECT:", indexing_object.object_id, len(indexing_object.object_fields)
    search.index_object(indexing_object)
    books_counter.add()
    #if books_counter.value >= 30:
    #    break
del search
print "OBJECT: end end"
books_counter.add()