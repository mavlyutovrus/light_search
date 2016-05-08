from search_engine import TSearchEngine
from utils import TCustomCounter
from lib.collect_bigrams_map import collect_bigrams_map
from lib.collect_bigrams_reduce import collect_bigrams_reduce
from datetime import datetime
import sys
import os

books_dir = sys.argv[1]
intermid_results_dir = sys.argv[2]
bigrams_indices_dir = sys.argv[3]
"""
books_dir = "/home/arslan/src/ngpedia/books1000/"
intermid_results_dir = "/home/arslan/src/ngpedia/search_system/tmp"
bigrams_indices_dir = "/home/arslan/src/ngpedia/search_system/bigrams"
"""

abs_start = datetime.now()

start = datetime.now()
collect_bigrams_map(books_dir, intermid_results_dir, sys.stdout)
print "Map:", datetime.now() - start

start = datetime.now()
collect_bigrams_reduce(intermid_results_dir, bigrams_indices_dir, sys.stdout)
print "Reduce:", datetime.now() - start

print "Total time:", datetime.now() - abs_start

