
include ../makefile.inc

all: librm.a rmtest_1 rmtest_2 rmtest_3 rmtest_extra

# lib file dependencies
librm.a: librm.a(rm.o)  # and possibly other .o files

# c file dependencies
rm.o: rm.h

rmtest_1.o: rm.h test_util.h

rmtest_2.o: rm.h test_util.h

rmtest_3.o: rmtest_3.cc rm.h test_util.h 

rmtest_extra.o: rm.h

# binary dependencies
rmtest_1: rmtest_1.o librm.a $(CODEROOT)/rbf/librbf.a $(CODEROOT)/ix/libix.a

rmtest_2: rmtest_2.o librm.a $(CODEROOT)/rbf/librbf.a $(CODEROOT)/ix/libix.a

rmtest_3: rmtest_3.o librm.a $(CODEROOT)/rbf/librbf.a $(CODEROOT)/ix/libix.a

rmtest_extra: rmtest_extra.o librm.a $(CODEROOT)/rbf/librbf.a

# dependencies to compile used libraries
.PHONY: $(CODEROOT)/rbf/librbf.a
$(CODEROOT)/rbf/librbf.a:
	$(MAKE) -C $(CODEROOT)/rbf librbf.a
    
.PHONY: $(CODEROOT)/ix/libix.a
$(CODEROOT)/ix/libix.a:
	$(MAKE) -C $(CODEROOT)/ix libix.a

.PHONY: clean
clean:
	rm -rf catalog attributes rmtest_1 rmtest_2 rmtest_3 rmtest_extra *.a *.o *~
	$(MAKE) -C $(CODEROOT)/rbf clean
cleanup:
	rm -rf catalog attributes index_table tbl_*

test:
	make cleanup
	./rmtest_1
