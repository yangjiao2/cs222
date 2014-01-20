//
//  adt.h
//
//  Created by bluesea on 14-1-18.
//  Copyright (c) 2014年 bluesea. All rights reserved.
//

#ifndef _adt_h_
#define _adt_h_

#include <iostream>
#include "rbfm.h"


#define ACCESS_FIELD_METHODS(fieldName) \
int get_##fieldName(void);\
int set_##fieldName(int v);

#define ACCESS_ENTRY_METHODS(entryName) \
int get_##entryName(int index);\
int set_##entryName(int index, int val);


//class themselves do not store data, it just refers to outside buffer

class RecordPage;
class RecordHeader;

class PageDirectory{
public:
    char *data; //bad naming styles, easy to conflict with local variable with same name
    int in_the_page;
    PageDirectory(char *dt = NULL): data(dt), in_the_page(0){}
    ACCESS_FIELD_METHODS(pgnum)
    ACCESS_FIELD_METHODS(next)
    ACCESS_ENTRY_METHODS(pgid)
    ACCESS_ENTRY_METHODS(pgfreelen)
    int moveToNext(FileHandle &fh);
    int firstPageWithFreelen(FileHandle &fh, int len);     //return matching pgnum
    int allocRecordPage(FileHandle &fh);           //return allocated pgnum
    static int MaximunEntryNum(void);
    int set_pgfree_by_id(int pageID, int newlen);
};


class RecordPage{
public:
    char *data; //points to beginning of page
    RecordPage(char *dt = NULL): data(dt) {}
    ACCESS_FIELD_METHODS(rcdnum)
    ACCESS_FIELD_METHODS(freeptr) //offset of freeSpace starting from beginning of page
    ACCESS_ENTRY_METHODS(offset)
    ACCESS_ENTRY_METHODS(rcdlen)
    int getFreelen(void);
    RecordHeader getRecordHeaderAt(int index); //setting returning rh's data
    RecordHeader allocRecordHeader(int len, int& slotID);  //alloc recordHeader by len
};

class RecordHeader{
public:
    char *data; //points to beginning of header
    RecordHeader(char *dt = NULL): data(dt) {}
    ACCESS_FIELD_METHODS(fieldnum)
    ACCESS_ENTRY_METHODS(fieldOffset)
    
    int writeHeader(vector<Attribute> recordDescriptor);
    int writeRecord(vector<Attribute> recordDescriptor, char *bytes);
    char *getContentPointer(void);

    //record's length, including header
    static int getRecordLength(vector<Attribute> descriptor);
    static int getRecordContentLength(vector<Attribute> descriptor);
    //TODO: add get/set field method, not required in project1
};

extern int writeInt(char *data, int val);
extern int readInt(char *data);

#endif /* defined(__cs222__adt__) */

