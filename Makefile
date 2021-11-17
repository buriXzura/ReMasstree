exe: example.o masstree.o
	g++ -o exe example.o masstree.o

example.o: example.cc masstree.h
	g++ -c example.cc

masstree.o: masstree.cc masstree.h
	g++ -c masstree.cc