CXX = g++
CFLAGS = -O0 -Wall -g
CXXFLAGS = $(CFLAGS) -std=c++0x
CPPFLAGS = -I/usr/include/RapidJSON-1

BINDIR = /usr/bin

LDFLAGS = -pthread -lrt -lcurl -lelliptics_client -lelliptics \
		  -lelliptics_cpp -lboost_thread -lboost_system -lmsgpack \
		  -lcocaine-core -lcocaine-framework

DEPS = $(wildcard *.h)
SOURCES = Discovery.cpp Node.cpp Parser.cpp ProcfsParser.cpp \
		  BackendParser.cpp TimestampParser.cpp ThreadPool.cpp \
		  Storage.cpp Group.cpp Couple.cpp DiscoveryTimer.cpp \
		  ConfigParser.cpp WorkerApplication.cpp CocaineHandlers.cpp \
		  main.cpp
OBJECTS = $(SOURCES:.cpp=.o)

%.o: %.cpp $(DEPS)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $< -o $@

cpp-worker: $(OBJECTS)
	$(CXX) $(CFLAGS) -o $@ $^ $(LDFLAGS) 

all: cpp-worker

clean:
	rm -f $(OBJECTS) cpp-worker
