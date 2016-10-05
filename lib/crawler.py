#-*- coding:utf8 -*-
from utils import crawl_folder
from utils import TCustomCounter
import chardet


LIB_SECTION_FIELD = "lib_section"
DEFAULT_ENCODING = "windows-1251"
            
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
        field_index2name = {1:"year", 
                            2:"udc", 
                            #3:"class_level1", 
                            #4:"class_level2", 
                            #5:"class_level3",
                            6:"pages_count", 
                            7: "author", 
                            8:"title"  }
        hierarchy_indices = [3, 4, 5] 
        
        import sys
        processed_counter = TCustomCounter("Crawler, found objects", sys.stderr, self.verbosity, 1000)
        encoding = chardet.detect(open(csv_file_path).read())['encoding']
        all_hierarchy_codes = {}
        for line in open(csv_file_path):
            line = line.decode(encoding)
            field_values = line.strip().split(";")
            object_id = field_values[0]
            fields = []
            for field_index, field_id in field_index2name.items():
                if len(field_values) > field_index:
                    field_value_encoded = field_values[field_index].encode(DEFAULT_ENCODING)
                    fields.append(TIndexingObjectField(field_id, 
                                                       field_value=field_value_encoded, 
                                                       field_file_path=""))
            """ library section feature """   
            hierarchy_codes = []
            import hashlib
            hash = hashlib.md5()
            path = ""
            for hierarchy_feat_index in hierarchy_indices:
                node_name = field_values[hierarchy_feat_index].strip()
                if not node_name:
                    break
                hash.update(node_name.encode("utf8"))
                code = int(hash.hexdigest(), 16) % 1000000007
                path += node_name + ";"
                hierarchy_codes.append(code)
                if not code in all_hierarchy_codes:
                    all_hierarchy_codes[code] = path
                elif code in all_hierarchy_codes and all_hierarchy_codes[code] != path:
                    print "Hash collision:", path.encode("utf8"), "vs.", all_hierarchy_codes[code].encode("utf8")
                    print "FULL STOP"
                    exit()
                     
            fields.append(TIndexingObjectField(field_id=LIB_SECTION_FIELD, 
                                               field_value=hierarchy_codes, 
                                               field_file_path=""))
            
            object2update = TIndexingObjectData(object_id=object_id,
                                                object_fields=fields)
            yield object2update
            processed_counter.add()
        """
        all_hierarchy_codes = [(path, code) for code, path in all_hierarchy_codes.items()]
        all_hierarchy_codes.sort()
        print "~~~~~ Library sections codes ~~~~~"
        for path, code in all_hierarchy_codes:
            print path.encode("utf8") + "\t" + str(code)
        """

