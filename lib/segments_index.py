import os
import pickle

SEGMENTS_OFFSETS_BLOCK_SIZE = 50

class TSegmentIndexWriter():
    def __init__(self, indices_dir):
        
        self.indices_dir = indices_dir
        self.values_file = open(os.path.join(self.indices_dir, "segments.pickle"), 
                                "wb", 
                                buffering=1000000)
        self.skip_list = []
        self.segments_count = 0
        self.prev_doc_id = ""
        self.prev_field_id = ""
    """ returns id of a saved segment """
    def add_segment(self, doc_id, field_id, start, length):
        import pickle
        new_block = False
        if self.segments_count % SEGMENTS_OFFSETS_BLOCK_SIZE == 0:
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
        return self.segments_count - 1
    
    def save(self):
        self.values_file.flush()
        skip_list_out = open(os.path.join(self.indices_dir, "segments.offsets"), "wb")
        pickle.dump(self.skip_list, skip_list_out)
        skip_list_out.close()
    def __del__(self):
        self.save()

class TSegmentIndexReader():
    def __init__(self, indices_dir):        
        self.indices_dir = indices_dir
        self.values_file = open(os.path.join(self.indices_dir, "segments.pickle"), "rb")  
        skip_list_in = open(os.path.join(self.indices_dir, "segments.offsets"), "rb")  
        self.skip_list = pickle.load(skip_list_in)
    def get_segment(self, segment_id):
        block_index, offset_in_block = divmod(segment_id, SEGMENTS_OFFSETS_BLOCK_SIZE)
        block_offset = self.skip_list[block_index]
        self.values_file.seek(block_offset)
        doc_id = ""
        field_id = ""
        for _ in xrange(offset_in_block + 1):
            curr_doc_id, curr_field_id, start, length = pickle.load(self.values_file)
            if curr_doc_id:
                doc_id = curr_doc_id
            if curr_field_id:
                field_id = curr_field_id
        return object_id, field_id, start, length
        
        
        