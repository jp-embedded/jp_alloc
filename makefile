CC ?= cc
CFLAGS ?= -O2
LDFLAGS ?=

all: jp_alloc.so jp_allocd.so

jp_alloc.so: jp_alloc.o
	$(CC) -shared -o $@ $^

jp_allocd.so: jp_allocd.o
	$(CC) -shared -g -ggdb -o $@ $^

%.o: %.c jp_alloc.h makefile
	$(CC) -std=c11 -fpic -c $(CFLAGS) -DJP_ALLOC_IMPLEMENTATION -o $@ $<

%d.o: %.c jp_alloc.h makefile
	$(CC) -std=c11 -fpic -c -g -ggdb $(CFLAGS) -DJP_ALLOC_IMPLEMENTATION -DJP_ALLOC_DEBUG -o $@ $<

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
	rm -f jp_alloc.so jp_allocd.so jp_alloc.o jp_allocd.o jpbench jpbenchd

.PHONY: all bench clean