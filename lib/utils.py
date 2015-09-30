#-*- coding:utf8 -*-

def crawl_folder(folder):
    import os
    os_objects = []
    seen = set([folder])
    for os_object_name in os.listdir(folder):
        full_path = os.path.normpath(os.path.join(folder, os_object_name))
        if not full_path in seen:  
            os_objects.append((full_path, os_object_name,))
            seen.add(full_path)
    return os_objects


class TCustomCounter:
    def __init__(self, name, log_stream, verbosity, interval=10):
        self.name = name
        self.verbosity = verbosity
        self.log_stream = log_stream
        self.interval = interval
        self.value = 0
    def add(self):
        from datetime import datetime
        self.value += 1
        if self.verbosity and self.value % self.interval == 0:
            self.log_stream.write("Logger: " + self.name + ", value: " + str(self.value) + ", time: " + str(datetime.now())+ "\n")
            self.log_stream.flush()

            
        
        