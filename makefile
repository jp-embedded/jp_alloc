CC ?= cc
CXX ?= g++
CFLAGS ?= -O2
LDFLAGS ?=

all: jp_alloc.so jp_allocd.so

# Detect if g++ is available for the C++ operator new/delete shim
HAS_CXX := $(shell command -v $(CXX) >/dev/null 2>&1 && echo yes)

jp_alloc.so: jp_alloc.o
ifneq ($(HAS_CXX),)
jp_alloc.so: jp_alloc_cpp.o
endif
	$(CC) -shared -o $@ $^

jp_allocd.so: jp_allocd.o
ifneq ($(HAS_CXX),)
jp_allocd.so: jp_alloc_cppd.o
endif
	$(CC) -shared -g -ggdb -o $@ $^

%.o: %.c jp_alloc.h makefile
	$(CC) -std=c11 -fpic -c $(CFLAGS) -DJP_ALLOC_IMPLEMENTATION -o $@ $<

%d.o: %.c jp_alloc.h makefile
	$(CC) -std=c11 -fpic -c -g -ggdb $(CFLAGS) -DJP_ALLOC_IMPLEMENTATION -DJP_ALLOC_DEBUG -o $@ $<

# C++ operator new/delete shim (only built if g++ is available)
ifneq ($(HAS_CXX),)
jp_alloc_cpp.o: jp_alloc_cpp.cpp makefile
	$(CXX) -std=c++17 -fpic -c $(CFLAGS) -o $@ $<

jp_alloc_cppd.o: jp_alloc_cpp.cpp makefile
	$(CXX) -std=c++11 -fpic -c -g -ggdb $(CFLAGS) -o $@ $<
endif

# Benchmark / correctness test
bench: jpbench jpbenchd

jpbench: jp_alloc.c jp_alloc_bench.c jp_alloc.h makefile
	$(CC) -O2 -std=c11 -DJP_ALLOC_IMPLEMENTATION -DJP_ALLOC_BENCH \
		jp_alloc.c jp_alloc_bench.c \
		-o jpbench -lpthread -lrt -lm

jpbenchd: jp_alloc.c jp_alloc_bench.c jp_alloc.h makefile
	$(CC) -O1 -g -std=c11 -DJP_ALLOC_IMPLEMENTATION -DJP_ALLOC_DEBUG -DJP_ALLOC_BENCH \
		jp_alloc.c jp_alloc_bench.c \
		-o jpbenchd -lpthread -lrt -lm

clean:
	rm -f jp_alloc.so jp_allocd.so jp_alloc.o jp_allocd.o jp_alloc_cpp.o jp_alloc_cppd.o jpbench jpbenchd

.PHONY: all bench clean