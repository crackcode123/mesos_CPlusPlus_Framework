CXX = g++
CXXFLAGS = -Wall -g -O2 -pthread -std=c++0x
INCLUDES=-I/usr/local/include -I/usr/local/include/protobuf-3.3.0/src -I/usr/local/include/boost-1.53.0 -I/usr/local/include/glog-0.3.3/src
LIBRARY_PATH=/usr/local/lib
LDFLAGS += -L$(LIBRARY_PATH) -lmesos -lpthread -lprotobuf
CXXCOMPILE = $(CXX) $(INCLUDES) $(CXXFLAGS) -c -o $@
CXXLINK = $(CXX) $(INCLUDES) $(CXXFLAGS) -o $@

default: all
all: my_framework my_executor 


my_framework: my_framework.cpp 
	$(CXXLINK) $<  $(LDFLAGS)  -lcurl
my_executor: my_executor.cpp 
	$(CXXLINK) $<  $(LDFLAGS)  -lcurl


clean:
	(rm -f core my-framework my-executor)
