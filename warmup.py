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

for key, values in search_engine.word_index.keys_db.items():
    print key, values