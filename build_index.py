from lib.search_engine import TSearchEngine
from lib.crawler import TCrawler
from lib.utils import TCustomCounter
from lib.build_index_parse_books import parse_books
from lib.build_index_reshuffle import reshuffle
from lib.build_index_reduce import reduce
from datetime import datetime
import sys
import os


books_dir = "/home/arslan/src/ngpedia/books1000/" # sys.argv[1]
intermid_results_dir = "./tmp"# sys.argv[2]
indices_dir = "indices2/" # sys.argv[3]


start = datetime.now()
abs_start = start

crawler = TCrawler(verbosity=1)
books = [indexing_object for indexing_object in crawler.crawl_folder(books_dir)]

print "Crawling:", datetime.now() - start


start = datetime.now()

words_freq_files, words_matches_files = parse_books(books, 
                                                    intermid_results_dir, 
                                                    processes_number=10)

print "Parsing:", datetime.now() - start

start = datetime.now()

reducers_fnames = reshuffle(words_freq_files, 
                            words_matches_files, 
                            indices_dir, 
                            intermid_results_dir, 
                            sys.stdout)

print "Reshuffling:", datetime.now() - start

start = datetime.now()

reduce(reducers_fnames, indices_dir, intermid_results_dir, processes_number=10)

print "Reduce:", datetime.now() - start


print "Total time:", datetime.now() - abs_start








