//
//  ixtest3.cpp
//  cs222
//
//  Created by bluesea on 14-2-20.
//  Copyright (c) 2014年 bluesea. All rights reserved.
//

#include "ix.h"


IndexManager *idm = IndexManager::instance();
string indexFileName = "Age_idx";
Attribute attrAge;

void test1();
void test2();
void clean();

int main(){
    
	attrAge.length = 4;
	attrAge.name = "Age";
	attrAge.type = TypeInt;
    clean();
    test1();
    test2();
    return 0;
}

void clean(){
    idm->destroyFile(indexFileName);
}


void test1(){
    // Functions tested
    // 1. Create Index File **
    // 2. Open Index File **
    // 3. Create Index File -- when index file is already created **
    // 4. Close Index File **
    
    RC rc;
    FileHandle fileHandle;
    
    cout << endl << "****In Test Case 1****" << endl;
    // create index file
    rc = idm->createFile(indexFileName);
    assert(rc == 0);
    
    // open index file
    rc = idm->openFile(indexFileName, fileHandle);
    assert(rc == 0);
    // create duplicate index file
    rc = idm->createFile(indexFileName);
    assert(rc != 0);
    
    // close index file
    rc = idm->closeFile(fileHandle);
    assert(rc == 0);
}

void test2(){
    RC rc;
    FileHandle fileHandle;
    cout << endl << "****In Test Case 2****" << endl;
    rc = idm->openFile(indexFileName, fileHandle);
    assert(rc == 0);
    int akey;
    RID rid;
    for (int i = 0; i < 5; i++) {
        akey = i + 1;
        rid.pageNum = i + 1;
        rid.slotNum = i + 2;
        rc = idm->insertEntry(fileHandle, attrAge, &akey, rid);
        assert(rc == 0);
    }
    
    akey = 3;
    rid.pageNum = 3;
    rid.slotNum = 4;
    rc = idm->deleteEntry(fileHandle, attrAge, &akey, rid);
    assert(rc == 0);
    
    rc = idm->deleteEntry(fileHandle, attrAge, &akey, rid);
    assert(rc != 0);
    
    
}