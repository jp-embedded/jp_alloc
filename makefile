all: jp_alloc.so jp_allocd.so

jp_alloc.so: jp_alloc.o
	g++ -shared -o $@ $^

jp_allocd.so: jp_allocd.o
	g++ -shared -g -ggdb -o $@ $^

%.o: %.cpp makefile
	g++ -std=c++20 -c -O2 -fpic -c -o $@ $<

%d.o: %.cpp makefile
	g++ -std=c++20 -c -g -ggdb -fpic -DDEBUG -o $@ $<
