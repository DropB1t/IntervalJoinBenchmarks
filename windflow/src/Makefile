
FF_ROOT         = $(HOME)/fastflow
WF_INCLUDES     = $(HOME)/WindFlow-4.2.0/wf
INCLUDE_DIR     = ../includes
OUT_DIR         = ../bin

CXX             = g++
CXXFLAGS        = -std=c++17
INCLUDES        = -I $(FF_ROOT) -I $(WF_INCLUDES) -I $(INCLUDE_DIR)
MACRO           = -DFF_BOUNDED_BUFFER -DDEFAULT_BUFFER_CAPACITY=32786 -DNDEBUG -DMETRICS_COLLECTION #-DWF_JOIN_MEASUREMENT
OPTFLAGS        = -g -O3 -finline-functions
LDFLAGS         = -pthread

all: ij

metric.o: ../includes/util/metric.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACRO) $(OPTFLAGS) $< -c

metric_group.o: ../includes/util/metric_group.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACRO) $(OPTFLAGS) $< -c

sampler.o: ../includes/util/sampler.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACRO) $(OPTFLAGS) $< -c

ij.o: ij.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACRO) $(OPTFLAGS) $< -c

ij: clean metric.o metric_group.o sampler.o ij.o
	$(CXX) metric.o metric_group.o sampler.o ij.o -o $(OUT_DIR)/ij $(LDFLAGS)

clean:
	rm -f $(OUT_DIR)/ij
	rm -f *.o

.DEFAULT_GOAL := all
.PHONY: all clean
