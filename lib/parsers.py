#-*- coding:utf8 -*-

class TParser(object):
    def __init__(self, block_size=256):
        self.block_size = block_size
    
    def file_of_my_type(self, file_path):
        return False
    
    def buffer_of_my_type(self, buffer):
        return True
    
    def parse_file(self, file_path, encoding=""):
        pass
    
    def parse_buffer(self, buffer, encoding=""):
        pass
    
    def split_on_blocks(self, tokens):
        for start, length, token in tokens:
            pass

class TTextParser(TParser):
    def __init__(self):
        super(TTextParser, self).__init__()
        pass
    
    def file_of_my_type(self, file_path):
        return file_path.endswith(".txt")
    
    def parse_buffer(self, undecoded_text_buffer, encoding=""):
        from ling_utils import span_tokenize
        tokens = span_tokenize(undecoded_text_buffer, encoding=encoding)
        from ling_utils import unify_tokens   
        """replace tokens with latin keywords + all to lowercase"""
        unify_tokens(tokens)
        return tokens 

    def parse_file(self, file_path, encoding=""):
        if not self.file_of_my_type(file_path):
            raise Exception("Parser", '%s Not a Text file: expect it to end with .txt' % (file_path))
        undecoded_content = open(file_path, "rb").read()
        return self.parse_buffer(undecoded_content, encoding)

class TParsersBundle(object):
    def __init__(self):
        self.parsers = [TTextParser()]

    def parse_file(self, file_name, encoding=""):
        for parser in self.parsers:
            if parser.file_of_my_type(file_name):
                return parser.parse_file(file_name, encoding)
        raise Exception("Parsing:", "No parser for file %s" % (file_name))
    
    def parse_buffer(self, buffer, encoding=""):
        for parser in self.parsers:
            if parser.buffer_of_my_type(buffer):
                return parser.parse_buffer(buffer, encoding)
        raise Exception("Parsing:", "No parser for buffer %s" % (buffer))
        


