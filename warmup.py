#-*- coding:utf8 -*-
import BaseHTTPServer
import sys
import socket
MACHINE_NETWORK_NAME = socket.gethostbyname(socket.gethostname())

port = int(sys.argv[1])
books_folder = sys.argv[2]
index_folder = sys.argv[3]
"""
port = 8334
books_folder = "/home/arslan/src/ngpedia/books1000"
index_folder ="indices/"
"""

from lib.search_engine import TSearchEngine
search_engine = TSearchEngine(index_location=index_folder)

top_freq = []
more_million = 0
more_500k = 0
for key, values in search_engine.word_index.keys_db.items():
    if values[0] > 1000000:
        more_million += 1
    if values[0] > 500000:
        more_500k += 1
    top_freq += [(values[0], key)]
print "freq > 1mln:",  more_million
print "freq > 500k:",  more_500k
top_freq.sort(reverse=True)
top_freq = top_freq[:800]
import urllib2
index = 0
for _, token in top_freq:
    json_val = urllib2.urlopen("http://" + MACHINE_NETWORK_NAME + ":" + str(port) + "/?q=" + token + "&json=1").read()
    print json_val
    print "INdex::", index
    index += 1

