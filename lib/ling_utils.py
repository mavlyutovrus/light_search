#-*- coding:utf8 -*-
import re
import sys
#pip install chardet
import chardet
#pip install unidecode
import unidecode
#pip install snowballstemmer
import snowballstemmer


CASE_UPPER = 2
CASE_TITLE = 1
CASE_LOWER = 0

RussianStemmer = snowballstemmer.stemmer("russian");

"""
1) http links
2) words, numbers
3) meaningful signs 
"""
TOKENIZATION_PATTERNS =  [u"http[^,\"\']+", \
                          u"[а-яА-ЯЁё0-9a-zA-Z]+", \
                          u"[\+\=]"]
TOKENIZATION_PATTERN  = re.compile(u"|".join(TOKENIZATION_PATTERNS))

def is_title_case_char(char):
    return char >= u'А' and char <= u'Я' or char >= u'A' and char <= u'Z' \
              or char == '\'' or char == '-' or char == u'Ё'

def is_title_case_word(word):
    return word and is_title_case_char(word[0])

def is_upper_case_word(word):
    if not word:
        return False
    for char in word:
        if not is_title_case_char(char):
            return False
    return True

class TTokenMatch(object):
    def __init__(self, start, length, case, token):
        self.start = start
        self.length = length
        self.case = case
        self.token = token
    def __str__(self):
        return "[%d:%d]=%s{%f}" % (self.start, self.start + self.length, self.token, self.case)

#encoded_text_buffer - text before applying .decode(...)
def span_tokenize(encoded_text_buffer, encoding="", fast=False):
    #detect encoding
    if not encoding:
        try:
            encoding = chardet.detect(encoded_text_buffer)['encoding']
            """ in russia we call it kostyl' """
            if encoding == "MacCyrillic":
                encoding = "windows-1251"
        except:
            sys.stderr.write(str(sys.exc_info()) + "\n\n")
            encoding = ""
        if not encoding:
            encoding = "windows-1251"
    try:
        text = encoded_text_buffer.decode(encoding)
    except:
        sys.stderr.write(str(sys.exc_info()) + "\n\n")
        sys.stderr.write("Orig text: \n--------------\n" + encoded_text_buffer[:50].replace("\n", " ") + "..." + "\n----------\n")
        sys.stderr.write("Recognized encoding: " + encoding + "\n\n")
        text = ""
    matches = []
    buffer_position = 0
    for match in re.finditer(TOKENIZATION_PATTERN, text):
        word = match.group()
        start, length = match.start(), len(word)
        word_in_orig_encoding = word.encode(encoding)
        buffer_position = encoded_text_buffer.index(word_in_orig_encoding, buffer_position)
        word_case = is_upper_case_word(word) and CASE_UPPER or is_title_case_word(word) and CASE_TITLE or CASE_LOWER
        match = TTokenMatch(start=buffer_position, 
                            length=len(word_in_orig_encoding), 
                            case=word_case, 
                            token=word)
        buffer_position += len(word_in_orig_encoding)
        matches.append(match)
    return matches


#tokens = list of TTokenMatch
def unify_tokens(tokens):
    for index in xrange(len(tokens)):
        word = tokens[index].token
        #only russian words + do not stem short words
        stemmed_word = len(word) > 4 and RussianStemmer.stemWord(word) or word
        unified_word = unidecode.unidecode(stemmed_word)
        tokens[index].token = unified_word.lower()


def adjust2next_space(text, position):
    while position < len(text) and not text[position].isspace():
        position += 1
    return position

        