
include ../makefile.inc

all: libix.a ixtest1 ixtest2 ixtest3

# lib file dependencies
libix.a: libix.a(ix.o)  # and possibly other .o files
libix.a: libix.a(btree.o)  # and possibly other .o files

# c file dependencies
ix.o: ix.h
btree.o: ix.h

ixtest1.o: ixtest_util.h

ixtest2.o: ixtest_util.h

# binary dependencies
ixtest1: ixtest1.o libix.a $(CODEROOT)/rbf/librbf.a 

ixtest2: ixtest2.o libix.a $(CODEROOT)/rbf/librbf.a 

ixtest3: ixtest3.o libix.a $(CODEROOT)/rbf/librbf.a 

# dependencies to compile used libraries
.PHONY: $(CODEROOT)/rbf/librbf.a
$(CODEROOT)/rbf/librbf.a:
	$(MAKE) -C $(CODEROOT)/rbf librbf.a

.PHONY: clean
clean:
	-rm ixtest1 ixtest2 *.a *.o
	$(MAKE) -C $(CODEROOT)/rbf clean

test:
	rm -rf *_idx
	./ixtest1
