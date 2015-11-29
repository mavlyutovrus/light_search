import os
import pickle

SEGMENTS_OFFSETS_BLOCK_SIZE = 50

class TSegmentIndexWriter():
    def __init__(self, indices_dir):
        
        self.indices_dir = indices_dir
        self.values_file = open(os.path.join(self.indices_dir, "segments.pickle"), 
                                "wb", 
                                buffering=1000000)
        self.obj2first_segment = []
        self.skip_list = []
        self.segments_count = 0
        self.prev_doc_id = ""
        self.prev_field_id = ""
    
    """ returns id of a saved segment """
    """ NB: expects that calls for each documents will be collocated. 
        Do not allow calls for different docs intermingle with each other """
    def add_segment(self, doc_id, field_id, start, length):
        new_segment_id = self.segments_count
        if doc_id != self.prev_doc_id:
            self.obj2first_segment.append((doc_id, new_segment_id))
        import pickle
        new_block = False
        if new_segment_id % SEGMENTS_OFFSETS_BLOCK_SIZE == 0:
            self.skip_list.append(self.values_file.tell())
            new_block = True
        if not new_block and doc_id == self.prev_doc_id:
            doc_id = ""
        if not new_block and field_id == self.prev_field_id:
            field_id = ""
        if doc_id:
            self.prev_doc_id = doc_id
        if field_id:
            self.prev_field_id = field_id
        obj = (doc_id, field_id, start, length,)
        pickle.dump(obj, self.values_file)
        self.segments_count += 1
        return new_segment_id
    
    def save(self):
        self.values_file.flush()
        skip_list_out = open(os.path.join(self.indices_dir, "segments.offsets"), "wb")
        pickle.dump(self.skip_list, skip_list_out)
        skip_list_out.close()
        obj2first_segment_out = open(os.path.join(self.indices_dir, "obj2first_segment.pickle"), "wb")
        pickle.dump(self.obj2first_segment, obj2first_segment_out)
        obj2first_segment_out.close()
    
    def __del__(self):
        self.save()

class TSegmentIndexReader():
    def __init__(self, indices_dir):        
        self.indices_dir = indices_dir
        self.values_file = open(os.path.join(self.indices_dir, "segments.pickle"), "rb")  
        skip_list_in = open(os.path.join(self.indices_dir, "segments.offsets"), "rb")  
        self.skip_list = pickle.load(skip_list_in)
        obj2first_segment_in = open(os.path.join(self.indices_dir, "obj2first_segment.pickle"), "rb")
        self.obj2first_segment = pickle.load(obj2first_segment_in)
        self.obj2segments_range = {}
        for index in xrange(1, len(self.obj2first_segment)):
            obj_id = self.obj2first_segment[index - 1][0]
            start = self.obj2first_segment[index - 1][1]
            end = self.obj2first_segment[index][1]
            self.obj2segments_range[obj_id] = (start, end)
        last_obj_id, last_obj_id_start = self.obj2first_segment[-1]
        self.obj2segments_range[last_obj_id] = (last_obj_id_start, last_obj_id_start + 1000)
    
    def get_obj_id_by_segment_id(self, segment_id):
        left = 0
        right = len(self.obj2first_segment) - 1
        if self.obj2first_segment[right][1] <= segment_id:
            return self.obj2first_segment[right][0]
        if right < left or self.obj2first_segment[left][1] > segment_id:
            return None
        while right > left + 1:
            mid = (left + right) >> 1
            mid_value = self.obj2first_segment[mid][1]
            if mid_value > segment_id:
                right = mid
            else:
                left = mid
        return self.obj2first_segment[left][0]
    
    def get_segments_by_obj_id(self, obj_id):
        if not obj_id in self.obj2segments_range:
            return None
        start, end = self.obj2segments_range[obj_id]
        import numpy
        return numpy.array(range(start, end), dtype=numpy.int64)     
    
    def get_segment(self, segment_id):
        block_index, offset_in_block = divmod(segment_id, SEGMENTS_OFFSETS_BLOCK_SIZE)
        block_offset = self.skip_list[block_index]
        self.values_file.seek(block_offset)
        obj_id = ""
        field_id = ""
        for _ in xrange(offset_in_block + 1):
            curr_obj_id, curr_field_id, start, length = pickle.load(self.values_file)
            if curr_obj_id:
                obj_id = curr_obj_id
            if curr_field_id:
                field_id = curr_field_id
        return obj_id, field_id, start, length
        
        
        