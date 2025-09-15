FF_ROOT         = $(HOME)/fastflow
WF_INCLUDES     = $(HOME)/dev/uni/WindFlow/wf
INCLUDE_DIR     = ../includes
OUT_DIR         = ../bin

CXX             = g++
CXXFLAGS        = -std=c++17
INCLUDES        = -I $(FF_ROOT) -I $(WF_INCLUDES) -I $(INCLUDE_DIR)
MACRO           = -DFF_BOUNDED_BUFFER -DDEFAULT_BUFFER_CAPACITY=32786 -DNDEBUG -DMETRICS_COLLECTION -DJOIN_ALL_LATENCIES
OPTFLAGS        = -g -O3 -finline-functions
LDFLAGS         = -pthread

all: test_comparison

metric.o: ../includes/util/metric.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACRO) $(OPTFLAGS) $< -c

metric_group.o: ../includes/util/metric_group.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACRO) $(OPTFLAGS) $< -c

sampler.o: ../includes/util/sampler.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACRO) $(OPTFLAGS) $< -c

test_comparison.o: test_comparison.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACRO) $(OPTFLAGS) $< -c

test_comparison: metric.o metric_group.o sampler.o test_comparison.o
	$(CXX) metric.o metric_group.o sampler.o test_comparison.o -o $(OUT_DIR)/test_comparison $(LDFLAGS)

clean:
	rm -f $(OUT_DIR)/test_comparison
	rm -f *.o

.DEFAULT_GOAL := all
.PHONY: all clean
