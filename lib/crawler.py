#-*- coding:utf8 -*-
from utils import crawl_folder
from utils import TCustomCounter
import chardet
            
class TIndexingObjectField(object):
    def __init__(self, field_id, field_value, field_file_path):
        self.field_id = field_id
        self.field_file_path = field_file_path
        self.field_value = field_value

class TIndexingObjectData(object):
    def __init__(self, object_id, object_fields):
        self.object_id = object_id
        self.object_fields = object_fields    

class TCrawler(object):
    def __init__(self, verbosity = 0):
        self.verbosity = verbosity
    
    def crawl_object_fields(self, folder, object_id):
        object_fields = crawl_folder(folder)
        to_update = []
        for field_path, field_id in object_fields:
            to_update.append( TIndexingObjectField(field_id=field_id, 
                                                  field_value="", 
                                                  field_file_path=field_path ) )
        return to_update
    
    def crawl_folder(self, folder):
        object_folders = crawl_folder(folder)
        import sys
        processed_counter = TCustomCounter("Crawler, found objects", sys.stderr, self.verbosity, 100)
        for object_folder, object_id in object_folders:
            fields2update = self.crawl_object_fields(object_folder, object_id)
            object2update = TIndexingObjectData(object_id=object_id,
                                                object_fields=fields2update)
            yield object2update
            processed_counter.add()

    def crawl_csv(self, csv_file_path):
        print csv_file_path
        field_index2name = {1:"year", 
                            2:"udc", 
                            #3:"class_level1", 
                            #4:"class_level2", 
                            #5:"class_level3",
                            6:"pages_count", 
                            7: "author", 
                            8:"title"  }
        import sys
        processed_counter = TCustomCounter("Crawler, found objects", sys.stderr, self.verbosity, 1000)
        encoding = ""
        for line in open(csv_file_path):
            if not encoding:
                encoding = chardet.detect(line)['encoding']
                print encoding
            line = line.decode(encoding)
            chunks = line.strip().split(";")
            object_id = chunks[0]
            fields = []
            for field_index, field_id in field_index2name.items():
                if len(chunks) > field_index:
                    field_value_encoded = chunks[field_index].encode("windows-1251")
                    fields.append(TIndexingObjectField(field_id, 
                                                       field_value=field_value_encoded, 
                                                       field_file_path=""))
            
            object2update = TIndexingObjectData(object_id=object_id,
                                                object_fields=fields)
            yield object2update
            processed_counter.add()

