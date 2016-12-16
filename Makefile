CC = g++
C = gcc

#-----------------------------------------------
# Uncomment exactly one of the lines labelled (A), (B), and (C) below
# to switch between compilation modes.

OPT = -O2 -DNDEBUG       # (A) Production use (optimized mode)
# OPT = -g2              # (B) Debug mode, w/ full line-level debugging symbols
# OPT = -O2 -g2 -DNDEBUG # (C) Profiling mode: opt, but w/debugging symbols
#-----------------------------------------------

#$(shell sh ./build_detect_platform)

include build_config.mk


DEBUGFLAGS =

CFLAGS = -c -I. $(DEBUGFLAGS) $(PORT_CFLAGS) $(PLATFORM_CFLAGS) $(OPT) 

LDFLAGS=$(PLATFORM_LDFLAGS) 

FUSEFLAGS=`pkg-config fuse --cflags`

FSLIBOJECTS=`pkg-config fuse --libs`


LIBOBJECTS = \
./fs/testfs.o \
./util/properties.o \
./util/logging.o \
./util/monitor.o \
./util/allocator.o \
./util/traceloader.o \
./util/command.o \
./util/testutil.o \
./util/myhash.o \
./util/socket.o


PROGRAMS = testfs  


all: $(LIBOBJECTS)

clean:
	-rm -f $(PROGRAMS) ./*.o */*.o
testfs: ./testfs_main.o $(LIBOBJECTS)
	$(CC) $(LDFLAGS) $(FUSEFLAGS) testfs_main.o $(LIBOBJECTS) $(FSLIBOJECTS)  -o $@
.cpp.o:
	$(CC) $(FUSEFLAGS) $(CFLAGS) $< -o $@

