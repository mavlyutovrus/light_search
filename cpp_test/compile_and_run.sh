g++ -O3 run_server.cpp server.cpp -std=gnu++11  -lboost_system -lpthread -o run; echo "start"; ./run /books/indices/ 
