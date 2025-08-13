FF_ROOT         = $(HOME)/fastflow
WF_INCLUDES     = $(HOME)/WindFlow-Hybrid/wf
INCLUDE_DIR     = ../includes
OUT_DIR         = ../bin

CXX             = g++
CXXFLAGS        = -std=c++17
INCLUDES        = -I $(FF_ROOT) -I $(WF_INCLUDES) -I $(INCLUDE_DIR)
MACRO           = -DFF_BOUNDED_BUFFER -DDEFAULT_BUFFER_CAPACITY=32786 -DNDEBUG #-DMETRICS_COLLECTION -DJOIN_ALL_LATENCIES
OPTFLAGS        = -g -O3 -finline-functions
LDFLAGS         = -pthread

all: ij ij2

metric.o: metric.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACRO) $(OPTFLAGS) $< -c -o $(OUT_DIR)/$@

metric_group.o: metric_group.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACRO) $(OPTFLAGS) $< -c -o $(OUT_DIR)/$@

sampler.o: sampler.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACRO) $(OPTFLAGS) $< -c -o $(OUT_DIR)/$@

ij.o: ij.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACRO) $(OPTFLAGS) $< -c -o $(OUT_DIR)/$@

ij: metric.o metric_group.o sampler.o ij.o
	$(CXX) $(OUT_DIR)/metric.o $(OUT_DIR)/metric_group.o $(OUT_DIR)/sampler.o $(OUT_DIR)/ij.o -o $(OUT_DIR)/ij $(LDFLAGS)

ij2.o: ij2.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(MACRO) $(OPTFLAGS) $< -c -o $(OUT_DIR)/$@

ij2: metric.o metric_group.o sampler.o ij2.o
	$(CXX) $(OUT_DIR)/metric.o $(OUT_DIR)/metric_group.o $(OUT_DIR)/sampler.o $(OUT_DIR)/ij2.o -o $(OUT_DIR)/ij2 $(LDFLAGS)

clean:
	rm -f $(OUT_DIR)/ij
	rm -f $(OUT_DIR)/ij2
	rm -f $(OUT_DIR)*.o

.DEFAULT_GOAL := all
.PHONY: all clean
