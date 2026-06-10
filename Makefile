CXX      = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -O2 -pthread

all: netbalancer

netbalancer: main.cpp logger.hpp httplib.h dashboard.html
	$(CXX) $(CXXFLAGS) -o netbalancer main.cpp

run: netbalancer
	./netbalancer

backends:
	./backends.sh

clean:
	rm -f netbalancer netbalancer.pipe netbalancer.log
	-kill $$(cat backends.pid 2>/dev/null) 2>/dev/null
	rm -f backends.pid