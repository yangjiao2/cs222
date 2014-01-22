
include ../makefile.inc

all: librm.a rmtest rmtest_extra

# lib file dependencies
librm.a: librm.a(rm.o)  # and possibly other .o files

# c file dependencies
rm.o: rm.h

rmtest.o: rm.h

rmtest_extra.o: rm.h

# binary dependencies
rmtest: rmtest.o librm.a $(CODEROOT)/rbf/librbf.a

rmtest_extra: rmtest_extra.o librm.a $(CODEROOT)/rbf/librbf.a

# dependencies to compile used libraries
.PHONY: $(CODEROOT)/rbf/librbf.a
$(CODEROOT)/rbf/librbf.a:
	$(MAKE) -C $(CODEROOT)/rbf librbf.a

.PHONY: clean
clean:
	-rm rmtest rmtest_extra *.a *.o *~
	$(MAKE) -C $(CODEROOT)/rbf clean
