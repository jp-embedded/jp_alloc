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

clean:
	rm -f jp_alloc.so jp_allocd.so jp_alloc.o jp_allocd.o

.PHONY: all clean