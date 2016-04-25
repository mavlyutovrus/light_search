swig -python -c++ -o search_engine_wrapper.cc swig.i
python setup.py build_ext --inplace
