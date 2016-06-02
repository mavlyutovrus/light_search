set -e
sudo apt-get install python-dev
wget https://bootstrap.pypa.io/get-pip.py
sudo python get-pip.py
sudo pip install unidecode
sudo pip install chardet
sudo pip install snowballstemmer
sudo pip install pybloom
sudo pip install psutil
sudo pip install numpy
sudo apt-get install swig
cd lib
sh compile_cpp.sh 
cd -
echo "Successfuly installed"
