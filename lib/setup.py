from distutils.core import setup, Extension

#extension_mod = Extension("searchengine", ["search_engine_wrapper.cc", "search_engine.hpp"])
searchengine_mod = Extension("_searchengine", 
                          ["search_engine_wrapper.cc"], 
                          extra_compile_args=['-O3', '-std=c++11'])

setup(name = "searchengine", ext_modules=[searchengine_mod], py_modules=["searchengine"])
