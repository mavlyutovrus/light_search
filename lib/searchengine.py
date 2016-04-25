# This file was automatically generated by SWIG (http://www.swig.org).
# Version 2.0.11
#
# Do not make changes to this file unless you know what you are doing--modify
# the SWIG interface file instead.





from sys import version_info
if version_info >= (2,6,0):
    def swig_import_helper():
        from os.path import dirname
        import imp
        fp = None
        try:
            fp, pathname, description = imp.find_module('_searchengine', [dirname(__file__)])
        except ImportError:
            import _searchengine
            return _searchengine
        if fp is not None:
            try:
                _mod = imp.load_module('_searchengine', fp, pathname, description)
            finally:
                fp.close()
            return _mod
    _searchengine = swig_import_helper()
    del swig_import_helper
else:
    import _searchengine
del version_info
try:
    _swig_property = property
except NameError:
    pass # Python < 2.2 doesn't have 'property'.
def _swig_setattr_nondynamic(self,class_type,name,value,static=1):
    if (name == "thisown"): return self.this.own(value)
    if (name == "this"):
        if type(value).__name__ == 'SwigPyObject':
            self.__dict__[name] = value
            return
    method = class_type.__swig_setmethods__.get(name,None)
    if method: return method(self,value)
    if (not static):
        self.__dict__[name] = value
    else:
        raise AttributeError("You cannot add attributes to %s" % self)

def _swig_setattr(self,class_type,name,value):
    return _swig_setattr_nondynamic(self,class_type,name,value,0)

def _swig_getattr(self,class_type,name):
    if (name == "thisown"): return self.this.own()
    method = class_type.__swig_getmethods__.get(name,None)
    if method: return method(self)
    raise AttributeError(name)

def _swig_repr(self):
    try: strthis = "proxy of " + self.this.__repr__()
    except: strthis = ""
    return "<%s.%s; %s >" % (self.__class__.__module__, self.__class__.__name__, strthis,)

try:
    _object = object
    _newclass = 1
except AttributeError:
    class _object : pass
    _newclass = 0


class TSearchIndex(_object):
    __swig_setmethods__ = {}
    __setattr__ = lambda self, name, value: _swig_setattr(self, TSearchIndex, name, value)
    __swig_getmethods__ = {}
    __getattr__ = lambda self, name: _swig_getattr(self, TSearchIndex, name)
    __repr__ = _swig_repr
    def __init__(self, *args): 
        this = _searchengine.new_TSearchIndex(*args)
        try: self.this.append(this)
        except: self.this = this
    __swig_destroy__ = _searchengine.delete_TSearchIndex
    __del__ = lambda self : None;
    def ExecuteQuery(self, *args): return _searchengine.TSearchIndex_ExecuteQuery(self, *args)
TSearchIndex_swigregister = _searchengine.TSearchIndex_swigregister
TSearchIndex_swigregister(TSearchIndex)
cvar = _searchengine.cvar
OBJ_BITS = cvar.OBJ_BITS
SEGMENT_POS_BITS = cvar.SEGMENT_POS_BITS
WEIGHT_BITS = cvar.WEIGHT_BITS
NON_SEGMENT_BITS = cvar.NON_SEGMENT_BITS
MAX_SEGMENTS_PER_OBJECT = cvar.MAX_SEGMENTS_PER_OBJECT
MAX_WORDS4QUERY = cvar.MAX_WORDS4QUERY
MAX_KEYS2CONSIDER = cvar.MAX_KEYS2CONSIDER
CRUDE_FILTER_TRIM_PROPORTION = cvar.CRUDE_FILTER_TRIM_PROPORTION
MAX_OCCURENCES2RETURN = cvar.MAX_OCCURENCES2RETURN

# This file is compatible with both classic and new-style classes.


