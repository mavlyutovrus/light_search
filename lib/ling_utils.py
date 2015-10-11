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
TOKENIZATION_PATTERNS =  [u"http[^,\"\'\s]+", \
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
def span_tokenize(encoded_text_buffer, encoding=""):
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
    word = ""
    text += " " # dummy to not check the tail
    for char in text:
        if                 char >= u'а' and char <= u'я' or char >= u'А' and char <= u'Я'\
                        or char == u'Ё'\
                        or char == u'ё' \
                        or char >= 'a' and char <= 'z' or char >= u'A' and char <= u'Z' \
                        or char >= '0' and char <= '9':
            word += char
        elif word:
            word_in_orig_encoding = word.encode(encoding)
            buffer_position = encoded_text_buffer.index(word_in_orig_encoding, buffer_position)
            word_case = is_upper_case_word(word) and CASE_UPPER or is_title_case_word(word) and CASE_TITLE or CASE_LOWER
            match = TTokenMatch(start=buffer_position, 
                                length=len(word_in_orig_encoding), 
                                case=word_case, 
                                token=word)
            buffer_position += len(word_in_orig_encoding)
            matches.append(match)
            word = ""
    return matches

A_SMALL_CODE = ord('a'.encode("windows-1251"))
Z_SMALL_CODE = ord('z'.encode("windows-1251"))
A_BIG_CODE = ord('A'.encode("windows-1251"))
Z_BIG_CODE = ord('Z'.encode("windows-1251"))
ZERO_CODE = ord('0'.encode("windows-1251"))
NINE_CODE = ord('9'.encode("windows-1251"))
AR_SMALL_CODE = ord(u'а'.encode("windows-1251"))
YAR_SMALL_CODE = ord(u'я'.encode("windows-1251"))
AR_BIG_CODE = ord(u'А'.encode("windows-1251"))
YAR_BIG_CODE = ord(u'Я'.encode("windows-1251"))
YOR_SMALL_CODE = ord(u'ё'.encode("windows-1251"))
YOR_BIG_CODE = ord(u'Ё'.encode("windows-1251"))

GOOD_CODES_WIN1251 = range(A_SMALL_CODE, Z_SMALL_CODE + 1) + \
             range(A_BIG_CODE, Z_BIG_CODE + 1) + \
             range(ZERO_CODE, NINE_CODE + 1) + \
             range(AR_SMALL_CODE, YAR_SMALL_CODE + 1) + \
             range(AR_BIG_CODE, YAR_BIG_CODE + 1) + \
             [YOR_BIG_CODE, YOR_SMALL_CODE]
GOOD_CODES_WIN1251 = set(GOOD_CODES_WIN1251)
GOOD_CHARS_WIN1251 = set(chr(code) for code in GOOD_CODES_WIN1251)
UPPER_GOOD_CHARS_WIN1251 = set(chr(code) for code in range(A_BIG_CODE, Z_BIG_CODE + 1) + \
                                                range(AR_BIG_CODE, YAR_BIG_CODE + 1) + [YOR_BIG_CODE])

"""blazingly fast, super-over-optimized, vital for speed!!!"""
def span_tokenize_windows1251(encoded_text_buffer):
    matches = []
    encoded_text_buffer += " " # dummy to not check the tail
    start = -1
    position = -1
    first_char = ''
    for char in encoded_text_buffer:
        position += 1
        if char in GOOD_CHARS_WIN1251:
            if start == -1:
                start = position
                first_char = char
        elif start != -1:
            word_case = first_char in UPPER_GOOD_CHARS_WIN1251 and CASE_TITLE or CASE_LOWER
            match = (start, position-start, word_case, encoded_text_buffer[start:position])
            matches.append(match)
            start = -1
    return matches


def unify_word(word):
    stemmed_word = len(word) > 4 and RussianStemmer.stemWord(word) or word
    unified_word = unidecode.unidecode(stemmed_word)
    return unified_word.lower()


        