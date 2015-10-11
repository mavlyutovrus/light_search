from lib.search_engine import TSearchEngine
from lib.crawler import TCrawler
from lib.utils import TCustomCounter
from lib.build_index_map import build_index_map
from lib.build_index_reduce import reduce, prepare_matches
from datetime import datetime
import sys
import os


books_dir = "/home/arslan/src/ngpedia/books1000/" # sys.argv[1]
intermid_results_dir = "./tmp"# sys.argv[2]
indices_dir = "indices/" # sys.argv[3]


start = datetime.now()
abs_start = start

crawler = TCrawler(verbosity=1)
books = [indexing_object for indexing_object in crawler.crawl_folder(books_dir)]
print "Crawling:", datetime.now() - start

start = datetime.now()
reducers_fnames = build_index_map(books, intermid_results_dir, indices_dir, sys.stdout)
print "Map:", datetime.now() - start

start = datetime.now()
reduce(reducers_fnames, intermid_results_dir, indices_dir, processes_number=5)
print "Reduce:", datetime.now() - start

print "Total time:", datetime.now() - abs_start

