#include <chrono>


typedef std::chrono::time_point<std::chrono::system_clock> TTime;

TTime GetTime() {
    return std::chrono::system_clock::now();
}

double GetElapsedInSeconds(TTime start, TTime end) {
    std::chrono::duration<double> elapsed = end - start;
    return elapsed.count();
}
