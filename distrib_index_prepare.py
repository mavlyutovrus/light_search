from lib.index_engine import TSearchEngine
from lib.crawler import TCrawler
from lib.utils import TCustomCounter
from datetime import datetime
import sys
import os

books_dir = "/home/arslan/src/ngpedia/books1000"
crawler = TCrawler(verbosity=1)
books_counter = TCustomCounter("BooksCounter", sys.stdout, verbosity=1, interval=1)
all_fields = []
for indexing_object in crawler.crawl_folder(books_dir):
    print "OBJECT:", indexing_object.object_id, len(indexing_object.object_fields)
    for field in indexing_object.object_fields:
        all_fields += [(indexing_object.object_id, field.field_id, field.field_file_path)]
    books_counter.add()

THREADS_COUNT = 8
print "TOTAL FIELDS", len(all_fields)
chunk_size = (len(all_fields) / THREADS_COUNT) + 1

commands_file = open("launch.sh", "w")
run_id = 0
for start_index in xrange(0, len(all_fields), chunk_size):
    work_file_name = "work_load_" + str(run_id) + ".txt"
    command = "nohup python distrib_index_run.py %d %s &> %d_log \n" % (run_id, work_file_name, run_id)
    commands_file.write(command)
    workload_file = open(work_file_name, "w")
    for index in xrange(start_index, min(len(all_fields), start_index + chunk_size)):
        obj_id, field_id, file_path = all_fields[index]
        workload_file.write(str(obj_id) + "\t" + str(field_id) + "\t" + file_path + "\n")
    workload_file.close()
    run_id += 1
commands_file.close()