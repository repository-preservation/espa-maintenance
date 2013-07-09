CC=g++
CFLAGS=-03 -Wall
BUILDDIR = build
OUTFILE = $(BUILDDIR)/lsrdgrid
DEPS = Grid.h Tile.h
SRC=src

build: 
	mkdir $(BUILDDIR)
	$(CC) -o $(OUTFILE) $(SRC)/* 

clean:
	rm -rf $(BUILDDIR)
