from lib.search_engine import TSearchEngine
from lib.utils import TCustomCounter
from lib.build_index_map import build_index_map
from lib.build_index_reduce import reduce
from datetime import datetime
import sys
import os

books_dir = sys.argv[1]
intermid_results_dir = sys.argv[2]
indices_dir = sys.argv[3]

abs_start = datetime.now()

start = datetime.now()
build_index_map(books_dir, intermid_results_dir, indices_dir, sys.stdout)
print "Map:", datetime.now() - start

start = datetime.now()
reduce(intermid_results_dir, indices_dir, sys.stdout)
print "Reduce:", datetime.now() - start

print "Total time:", datetime.now() - abs_start

