#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

#include <fstream>
#include <iostream>
#include <cassert>
#include <sys/time.h>
#include <sys/resource.h>
#include "rm.h"

using namespace std;

RelationManager *rm = RelationManager::instance();
const int success = 0;


void memProfile()
{
    int who = RUSAGE_SELF;
    struct rusage usage;
    getrusage(who,&usage);
    cout<<usage.ru_maxrss<<"KB"<<endl;
}

// Function to prepare the data in the correct form to be inserted/read/updated
void prepareTuple(const int nameLength, const string &name, const int age, const float height, const int salary, void *buffer, int *tupleSize)
{
    int offset = 0;
    
    memcpy((char *)buffer + offset, &nameLength, sizeof(int));
    offset += sizeof(int);    
    memcpy((char *)buffer + offset, name.c_str(), nameLength);
    offset += nameLength;
    
    memcpy((char *)buffer + offset, &age, sizeof(int));
    offset += sizeof(int);
    
    memcpy((char *)buffer + offset, &height, sizeof(float));
    offset += sizeof(float);
    
    memcpy((char *)buffer + offset, &salary, sizeof(int));
    offset += sizeof(int);
    
    *tupleSize = offset;
}


// Function to parse the data in buffer and print each field
void printTuple(const void *buffer, const int tupleSize)
{
    int offset = 0;
    cout << "****Printing Buffer: Start****" << endl;
   
    int nameLength = 0;     
    memcpy(&nameLength, (char *)buffer+offset, sizeof(int));
    offset += sizeof(int);
    cout << "nameLength: " << nameLength << endl;
   
    char *name = (char *)malloc(100);
    memcpy(name, (char *)buffer+offset, nameLength);
    name[nameLength] = '\0';
    offset += nameLength;
    cout << "name: " << name << endl;
    
    int age = 0; 
    memcpy(&age, (char *)buffer+offset, sizeof(int));
    offset += sizeof(int);
    cout << "age: " << age << endl;
   
    float height = 0.0; 
    memcpy(&height, (char *)buffer+offset, sizeof(float));
    offset += sizeof(float);
    cout << "height: " << height << endl;
       
    int salary = 0; 
    memcpy(&salary, (char *)buffer+offset, sizeof(int));
    offset += sizeof(int);
    cout << "salary: " << salary << endl;

    cout << "****Printing Buffer: End****" << endl << endl;    
}


// Function to get the data in the correct form to be inserted/read after adding
// the attribute ssn
void prepareTupleAfterAdd(const int nameLength, const string &name, const int age, const float height, const int salary, const int ssn, void *buffer, int *tupleSize)
{
    int offset=0;
    
    memcpy((char*)buffer + offset, &(nameLength), sizeof(int));
    offset += sizeof(int);    
    memcpy((char*)buffer + offset, name.c_str(), nameLength);
    offset += nameLength;
    
    memcpy((char*)buffer + offset, &age, sizeof(int));
    offset += sizeof(int);
        
    memcpy((char*)buffer + offset, &height, sizeof(float));
    offset += sizeof(float);
        
    memcpy((char*)buffer + offset, &salary, sizeof(int));
    offset += sizeof(int);
    
    memcpy((char*)buffer + offset, &ssn, sizeof(int));
    offset += sizeof(int);

    *tupleSize = offset;
}


void printTupleAfterDrop( const void *buffer, const int tupleSize)
{
    int offset = 0;
    cout << "****Printing Buffer: Start****" << endl;
   
    int nameLength = 0;     
    memcpy(&nameLength, (char *)buffer+offset, sizeof(int));
    offset += sizeof(int);
    cout << "nameLength: " << nameLength << endl;
   
    char *name = (char *)malloc(100);
    memcpy(name, (char *)buffer+offset, nameLength);
    name[nameLength] = '\0';
    offset += nameLength;
    cout << "name: " << name << endl;
    
    int age = 0; 
    memcpy(&age, (char *)buffer+offset, sizeof(int));
    offset += sizeof(int);
    cout << "age: " << age << endl;
   
    float height = 0.0; 
    memcpy(&height, (char *)buffer+offset, sizeof(float));
    offset += sizeof(float);
    cout << "height: " << height << endl;
       
    cout << "****Printing Buffer: End****" << endl << endl;    
}   


void printTupleAfterAdd(const void *buffer, const int tupleSize)
{
    int offset = 0;
    cout << "****Printing Buffer: Start****" << endl;
   
    int nameLength = 0;     
    memcpy(&nameLength, (char *)buffer+offset, sizeof(int));
    offset += sizeof(int);
    cout << "nameLength: " << nameLength << endl;
   
    char *name = (char *)malloc(100);
    memcpy(name, (char *)buffer+offset, nameLength);
    name[nameLength] = '\0';
    offset += nameLength;
    cout << "name: " << name << endl;
    
    int age = 0; 
    memcpy(&age, (char *)buffer+offset, sizeof(int));
    offset += sizeof(int);
    cout << "age: " << age << endl;
   
    float height = 0; 
    memcpy(&height, (char *)buffer+offset, sizeof(float));
    offset += sizeof(float);
    cout << "height: " << height << endl;
	
	int salary = 0; 
    memcpy(&salary, (char *)buffer+offset, sizeof(int));
    offset += sizeof(int);
    cout << "salary: " << salary << endl;
    
    int ssn = 0;   
    memcpy(&ssn, (char *)buffer+offset, sizeof(int));
    offset += sizeof(int);
    cout << "SSN: " << ssn << endl;

    cout << "****Printing Buffer: End****" << endl << endl;    
}


// Create an employee table
void createTable(const string &tableName)
{
    cout << "****Create Table " << tableName << " ****" << endl;
    
    // 1. Create Table ** -- made separate now.
    vector<Attribute> attrs;

    Attribute attr;
    attr.name = "EmpName";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)30;
    attrs.push_back(attr);

    attr.name = "Age";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    attrs.push_back(attr);

    attr.name = "Height";
    attr.type = TypeReal;
    attr.length = (AttrLength)4;
    attrs.push_back(attr);

    attr.name = "Salary";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    attrs.push_back(attr);

    RC rc = rm->createTable(tableName, attrs);
    assert(rc == success);
    cout << "****Table Created: " << tableName << " ****" << endl << endl;
}


void prepareLargeTuple(const int index, void *buffer, int *size)
{
    int offset = 0;
    
    // compute the count
    int count = index % 50 + 1;

    // compute the letter
    char text = index % 26 + 97;

    for(int i = 0; i < 10; i++)
    {
        memcpy((char *)buffer + offset, &count, sizeof(int));
        offset += sizeof(int);

        for(int j = 0; j < count; j++)
        {
            memcpy((char *)buffer + offset, &text, 1);
            offset += 1;
        }
   
        // compute the integer 
        memcpy((char *)buffer + offset, &index, sizeof(int));
        offset += sizeof(int);
   
        // compute the floating number
        float real = (float)(index + 1); 
        memcpy((char *)buffer + offset, &real, sizeof(float));
        offset += sizeof(float);
    }
    *size = offset; 
}


// Create a large table for pressure test
void createLargeTable(const string &tableName)
{
    cout << "****Create Large Table " << tableName << " ****" << endl;
    
    // 1. Create Table ** -- made separate now.
    vector<Attribute> attrs;

    int index = 0;
    char *suffix = (char *)malloc(10);
    for(int i = 0; i < 10; i++)
    {
        Attribute attr;
        sprintf(suffix, "%d", index);
        attr.name = "attr";
        attr.name += suffix;
        attr.type = TypeVarChar;
        attr.length = (AttrLength)50;
        attrs.push_back(attr);
        index++;

        sprintf(suffix, "%d", index);
        attr.name = "attr";
        attr.name += suffix;
        attr.type = TypeInt;
        attr.length = (AttrLength)4;
        attrs.push_back(attr);
        index++;

        sprintf(suffix, "%d", index);
        attr.name = "attr";
        attr.name += suffix;
        attr.type = TypeReal;
        attr.length = (AttrLength)4;
        attrs.push_back(attr);
        index++;
    }

    RC rc = rm->createTable(tableName, attrs);
    assert(rc == success);
    cout << "****Large Table Created: " << tableName << " ****" << endl << endl;

    free(suffix);
}


void secA_0(const string &tableName)
{
    // Functions Tested
    // 1. Get Attributes
    cout << "****In Test Case 0****" << endl;

    // GetAttributes
    vector<Attribute> attrs;
    RC rc = rm->getAttributes(tableName, attrs);
    assert(rc == success);

    for(unsigned i = 0; i < attrs.size(); i++)
    {
        cout << "Attribute Name: " << attrs[i].name << endl;
        cout << "Attribute Type: " << (AttrType)attrs[i].type << endl;
        cout << "Attribute Length: " << attrs[i].length << endl << endl;
    }
    cout<<"** Test Case 0 passed"<<endl;
    return;
}


void secA_1(const string &tableName, const int nameLength, const string &name, const int age, const float height, const int salary)
{
    // Functions tested
    // 1. Create Table ** -- made separate now.
    // 2. Insert Tuple **
    // 3. Read Tuple **
    // NOTE: "**" signifies the new functions being tested in this test case. 
    cout << "****In Test Case 1****" << endl;
   
    RID rid; 
    int tupleSize = 0;
    void *tuple = malloc(100);
    void *returnedData = malloc(100);

    // Insert a tuple into a table
    prepareTuple(nameLength, name, age, height, salary, tuple, &tupleSize);
    cout << "Insert Data:" << endl;
    printTuple(tuple, tupleSize);
    RC rc = rm->insertTuple(tableName, tuple, rid);
    assert(rc == success);
    
    // Given the rid, read the tuple from table
    rc = rm->readTuple(tableName, rid, returnedData);
    assert(rc == success);

    cout << "Returned Data:" << endl;
    printTuple(returnedData, tupleSize);

    // Compare whether the two memory blocks are the same
    if(memcmp(tuple, returnedData, tupleSize) == 0)
    {
        cout << "****Test case 1 passed****" << endl << endl;
    }
    else
    {
        cout << "****Test case 1 failed****" << endl << endl;
    }

    free(tuple);
    free(returnedData);
    return;
}


void secA_2(const string &tableName, const int nameLength, const string &name, const int age, const float height, const int salary)
{
    // Functions Tested
    // 1. Insert tuple
    // 2. Delete Tuple **
    // 3. Read Tuple
    cout << "****In Test Case 2****" << endl;
   
    RID rid; 
    int tupleSize = 0;
    void *tuple = malloc(100);
    void *returnedData = malloc(100);

    // Test Insert the Tuple    
    prepareTuple(nameLength, name, age, height, salary, tuple, &tupleSize);
    cout << "Insert Data:" << endl;
    printTuple(tuple, tupleSize);
    RC rc = rm->insertTuple(tableName, tuple, rid);
    assert(rc == success);
    

    // Test Delete Tuple
    rc = rm->deleteTuple(tableName, rid);
    assert(rc == success);
    cout<< "delete data done"<<endl;
    
    // Test Read Tuple
    memset(returnedData, 0, 100);
    rc = rm->readTuple(tableName, rid, returnedData);
    assert(rc != success);

    cout << "After Deletion." << endl;
    
    // Compare the two memory blocks to see whether they are different
    if (memcmp(tuple, returnedData, tupleSize) != 0)
    {   
        cout << "****Test case 2 passed****" << endl << endl;
    }
    else
    {
        cout << "****Test case 2 failed****" << endl << endl;
    }
        
    free(tuple);
    free(returnedData);
    return;
}


void secA_3(const string &tableName, const int nameLength, const string &name, const int age, const float height, const int salary)
{
    // Functions Tested
    // 1. Insert Tuple    
    // 2. Update Tuple **
    // 3. Read Tuple
    cout << "****In Test Case 3****" << endl;
   
    RID rid; 
    int tupleSize = 0;
    int updatedTupleSize = 0;
    void *tuple = malloc(100);
    void *updatedTuple = malloc(100);
    void *returnedData = malloc(100);
   
    // Test Insert Tuple 
    prepareTuple(nameLength, name, age, height, salary, tuple, &tupleSize);
    RC rc = rm->insertTuple(tableName, tuple, rid);
    assert(rc == success);
    cout << "Original RID slot = " << rid.slotNum << endl;

    // Test Update Tuple
    prepareTuple(6, "Newman", age, height, 100, updatedTuple, &updatedTupleSize);
    rc = rm->updateTuple(tableName, updatedTuple, rid);
    assert(rc == success);
    cout << "Updated RID slot = " << rid.slotNum << endl;

    // Test Read Tuple 
    rc = rm->readTuple(tableName, rid, returnedData);
    assert(rc == success);
    cout << "Read RID slot = " << rid.slotNum << endl;
   
    // Print the tuples 
    cout << "Insert Data:" << endl; 
    printTuple(tuple, tupleSize);

    cout << "Updated data:" << endl;
    printTuple(updatedTuple, updatedTupleSize);

    cout << "Returned Data:" << endl;
    printTuple(returnedData, updatedTupleSize);
    
    if (memcmp(updatedTuple, returnedData, updatedTupleSize) == 0)
    {
        cout << "****Test case 3 passed****" << endl << endl;
    }
    else
    {
        cout << "****Test case 3 failed****" << endl << endl;
    }

    free(tuple);
    free(updatedTuple);
    free(returnedData);
    return;
}


void secA_4(const string &tableName, const int nameLength, const string &name, const int age, const float height, const int salary)
{
    // Functions Tested
    // 1. Insert tuple
    // 2. Read Attributes **
    cout << "****In Test Case 4****" << endl;
    
    RID rid;    
    int tupleSize = 0;
    void *tuple = malloc(100);
    void *returnedData = malloc(100);
    
    // Test Insert Tuple 
    prepareTuple(nameLength, name, age, height, salary, tuple, &tupleSize);
    RC rc = rm->insertTuple(tableName, tuple, rid);
    assert(rc == success);

    // Test Read Attribute
    rc = rm->readAttribute(tableName, rid, "Salary", returnedData);
    assert(rc == success);
 
    cout << "Salary: " << *(int *)returnedData << endl;
    if (memcmp((char *)returnedData, (char *)tuple+18, 4) != 0)
    {
        cout << "****Test case 4 failed" << endl << endl;
    }
    else
    {
        cout << "****Test case 4 passed" << endl << endl;
    }
    
    free(tuple);
    free(returnedData);
    return;
}


void secA_5(const string &tableName, const int nameLength, const string &name, const int age, const float height, const int salary)
{
    // Functions Tested
    // 0. Insert tuple;
    // 1. Read Tuple
    // 2. Delete Tuples **
    // 3. Read Tuple
    cout << "****In Test Case 5****" << endl;
    
    RID rid;
    int tupleSize = 0;
    void *tuple = malloc(100);
    void *returnedData = malloc(100);
    void *returnedData1 = malloc(100);
   
    // Test Insert Tuple 
    prepareTuple(nameLength, name, age, height, salary, tuple, &tupleSize);
    RC rc = rm->insertTuple(tableName, tuple, rid);
    assert(rc == success);

    // Test Read Tuple
    rc = rm->readTuple(tableName, rid, returnedData);
    assert(rc == success);
    printTuple(returnedData, tupleSize);

    cout << "Now Deleting..." << endl;

    // Test Delete Tuples
    rc = rm->deleteTuples(tableName);
    assert(rc == success);
    
    // Test Read Tuple
    memset((char*)returnedData1, 0, 100);
    rc = rm->readTuple(tableName, rid, returnedData1);
    assert(rc != success);
    printTuple(returnedData1, tupleSize);
    
    if(memcmp(tuple, returnedData1, tupleSize) != 0)
    {
        cout << "****Test case 5 passed****" << endl << endl;
    }
    else
    {
        cout << "****Test case 5 failed****" << endl << endl;
    }
       
    free(tuple);
    free(returnedData);
    free(returnedData1);
    return;
}


void secA_6(const string &tableName, const int nameLength, const string &name, const int age, const float height, const int salary)
{
    // Functions Tested
    // 0. Insert tuple;
    // 1. Read Tuple
    // 2. Delete Table **
    // 3. Read Tuple
    cout << "****In Test Case 6****" << endl;
   
    RID rid; 
    int tupleSize = 0;
    void *tuple = malloc(100);
    void *returnedData = malloc(100);
    void *returnedData1 = malloc(100);
   
    // Test Insert Tuple
    prepareTuple(nameLength, name, age, height, salary, tuple, &tupleSize);
    RC rc = rm->insertTuple(tableName, tuple, rid);
    assert(rc == success);

    // Test Read Tuple 
    rc = rm->readTuple(tableName, rid, returnedData);
    assert(rc == success);

    // Test Delete Table
    rc = rm->deleteTable(tableName);
    assert(rc == success);
    cout << "After deletion!" << endl;
    
    // Test Read Tuple 
    memset((char*)returnedData1, 0, 100);
    rc = rm->readTuple(tableName, rid, returnedData1);
    assert(rc != success);
    
    if(memcmp(returnedData, returnedData1, tupleSize) != 0)
    {
        cout << "****Test case 6 passed****" << endl << endl;
    }
    else
    {
        cout << "****Test case 6 failed****" << endl << endl;
    }
        
    free(tuple);
    free(returnedData);    
    free(returnedData1);
    return;
}


void secA_7(const string &tableName)
{
    // Functions Tested
    // 1. Reorganize Page **
    // Insert tuples into one page, reorganize that page, 
    // and use the same tids to read data. The results should 
    // be the same as before. Will check code as well.
    cout << "****In Test Case 7****" << endl;
   
    RID rid; 
    int tupleSize = 0;
    int numTuples = 5;
    void *tuple;
    void *returnedData = malloc(100);

    int sizes[numTuples];
    RID rids[numTuples];
    vector<char *> tuples;

    RC rc = 0;
    for(int i = 0; i < numTuples; i++)
    {
        tuple = malloc(100);

        // Test Insert Tuple
        float height = (float)i;
        prepareTuple(6, "Tester", 20+i, height, 123, tuple, &tupleSize);
        rc = rm->insertTuple(tableName, tuple, rid);
        assert(rc == success);

        tuples.push_back((char *)tuple);
        sizes[i] = tupleSize;
        rids[i] = rid;
        cout << rid.pageNum << endl;
    }
    cout << "After Insertion!" << endl;
    
    int pageid = 0; // Depends on which page the tuples are
    rc = rm->reorganizePage(tableName, pageid);
    assert(rc == success);

    // Print out the tuples one by one
    int i = 0;
    for (i = 0; i < numTuples; i++)
    {
        rc = rm->readTuple(tableName, rids[i], returnedData);
        assert(rc == success);
        printTuple(returnedData, tupleSize);

        //if any of the tuples are not the same as what we entered them to be ... there is a problem with the reorganization.
        if (memcmp(tuples[i], returnedData, sizes[i]) != 0)
        {      
            cout << "****Test case 7 failed****" << endl << endl;
            break;
        }
    }
    if(i == numTuples)
    {
        cout << "****Test case 7 passed****" << endl << endl;
    }
    
    // Delete Table    
    rc = rm->deleteTable(tableName);
    assert(rc == success);

    free(returnedData);
    for(i = 0; i < numTuples; i++)
    {
        free(tuples[i]);
    }
    return;
}


void secA_8(const string &tableName)
{
    // Functions Tested
    // 1. Simple scan **
    cout << "****In Test Case 8****" << endl;

    RID rid;    
    int tupleSize = 0;
    int numTuples = 5;
    void *tuple;
    void *returnedData = malloc(100);

    RID rids[numTuples];
    vector<char *> tuples;

    RC rc = 0;
    for(int i = 0; i < numTuples; i++)
    {
        tuple = malloc(100);

        // Insert Tuple
        float height = (float)i;
        prepareTuple(6, "Tester", 20+i, height, 123, tuple, &tupleSize);
        rc = rm->insertTuple(tableName, tuple, rid);
        assert(rc == success);

        tuples.push_back((char *)tuple);
        rids[i] = rid;
    }
    cout << "After Insertion!" << endl;

    // Set up the iterator
    RM_ScanIterator rmsi;
    string attr = "Age";
    vector<string> attributes;
    attributes.push_back(attr);
    rc = rm->scan(tableName, "", NO_OP, NULL, attributes, rmsi);
    assert(rc == success);

    cout << "Scanned Data:" << endl;
    
    while(rmsi.getNextTuple(rid, returnedData) != RM_EOF)
    {
        cout << "Age: " << *(int *)returnedData << endl;
    }
    rmsi.close();
    
    // Deleta Table
    rc = rm->deleteTable(tableName);
    assert(rc == success);

    free(returnedData);
    for(int i = 0; i < numTuples; i++)
    {
        free(tuples[i]);
    }
     cout << "****Test case 8 passed****" << endl << endl; 
    return;
}


void secA_9(const string &tableName, vector<RID> &rids, vector<int> &sizes)
{
    // Functions Tested:
    // 1. create table
    // 2. getAttributes
    // 3. insert tuple
    cout << "****In Test case 9****" << endl;

    RID rid; 
    void *tuple = malloc(1000);
    int numTuples = 2000;

    // GetAttributes
    vector<Attribute> attrs;
    RC rc = rm->getAttributes(tableName, attrs);
    assert(rc == success);

    for(unsigned i = 0; i < attrs.size(); i++)
    {
        cout << "Attribute Name: " << attrs[i].name << endl;
        cout << "Attribute Type: " << (AttrType)attrs[i].type << endl;
        cout << "Attribute Length: " << attrs[i].length << endl << endl;
    }

    // Insert 2000 tuples into table
    for(int i = 0; i < numTuples; i++)
    {
        // Test insert Tuple
        int size = 0;
        memset(tuple, 0, 1000);
        prepareLargeTuple(i, tuple, &size);

        rc = rm->insertTuple(tableName, tuple, rid);
        assert(rc == success);

        rids.push_back(rid);
        sizes.push_back(size);        
    }
    cout << "****Test case 9 passed****" << endl << endl;

    free(tuple);
}


void secA_10(const string &tableName, const vector<RID> &rids, const vector<int> &sizes)
{
    // Functions Tested:
    // 1. read tuple
    cout << "****In Test case 10****" << endl;

    int numTuples = 2000;
    void *tuple = malloc(1000);
    void *returnedData = malloc(1000);

    RC rc = 0;
    for(int i = 0; i < numTuples; i++)
    {
        memset(tuple, 0, 1000);
        memset(returnedData, 0, 1000);
        rc = rm->readTuple(tableName, rids[i], returnedData);
        assert(rc == success);

        int size = 0;
        prepareLargeTuple(i, tuple, &size);
        if(memcmp(returnedData, tuple, sizes[i]) != 0)
        {
            cout << "****Test case 10 failed****" << endl << endl;
            return;
        }
    }
    cout << "****Test case 10 passed****" << endl << endl;

    free(tuple);
    free(returnedData);
}


void secA_11(const string &tableName, vector<RID> &rids, vector<int> &sizes)
{
    // Functions Tested:
    // 1. update tuple
    // 2. read tuple
    cout << "****In Test case 11****" << endl;

    RC rc = 0;
    void *tuple = malloc(1000);
    void *returnedData = malloc(1000);

    // Update the first 1000 tuples
    int size = 0;
    for(int i = 0; i < 1000; i++)
    {
        memset(tuple, 0, 1000);
        RID rid = rids[i];

        prepareLargeTuple(i+10, tuple, &size);
        rc = rm->updateTuple(tableName, tuple, rid);
        assert(rc == success);

        sizes[i] = size;
        rids[i] = rid;
    }
    cout << "Updated!" << endl;

    // Read the recrods out and check integrity
    for(int i = 0; i < 1000; i++)
    {
        memset(tuple, 0, 1000);
        memset(returnedData, 0, 1000);
        prepareLargeTuple(i+10, tuple, &size);
        rc = rm->readTuple(tableName, rids[i], returnedData);
        assert(rc == success);

        if(memcmp(returnedData, tuple, sizes[i]) != 0)
        {
            cout << "****Test case 11 failed****" << endl << endl;
            return;
        }
    }
    cout << "****Test case 11 passed****" << endl << endl;

    free(tuple);
    free(returnedData);
}


void secA_12(const string &tableName, const vector<RID> &rids)
{
    // Functions Tested
    // 1. delete tuple
    // 2. read tuple
    cout << "****In Test case 12****" << endl;

    RC rc = 0;
    void * returnedData = malloc(1000);

    // Delete the first 1000 tuples
    for(int i = 0; i < 1000; i++)
    {
        rc = rm->deleteTuple(tableName, rids[i]);
        assert(rc == success);

        rc = rm->readTuple(tableName, rids[i], returnedData);
        assert(rc != success);
    }
    cout << "After deletion!" << endl;

    for(int i = 1000; i < 2000; i++)
    {
        rc = rm->readTuple(tableName, rids[i], returnedData);
        assert(rc == success);
    }
    cout << "****Test case 12 passed****" << endl << endl;

    free(returnedData);
}


void secA_13(const string &tableName)
{
    // Functions Tested
    // 1. scan
    cout << "****In Test case 13****" << endl;

    RM_ScanIterator rmsi;
    vector<string> attrs;
    attrs.push_back("attr5");
    attrs.push_back("attr12");
    attrs.push_back("attr28");
   
    RC rc = rm->scan(tableName, "", NO_OP, NULL, attrs, rmsi); 
    assert(rc == success);

    RID rid;
    int j = 0;
    void *returnedData = malloc(1000);

    while(rmsi.getNextTuple(rid, returnedData) != RM_EOF)
    {
        if(j % 200 == 0)
        {
            int offset = 0;

            cout << "Real Value: " << *(float *)(returnedData) << endl;
            offset += 4;
        
            int size = *(int *)((char *)returnedData + offset);
            cout << "String size: " << size << endl;
            offset += 4;

            char *buffer = (char *)malloc(size + 1);
            memcpy(buffer, (char *)returnedData + offset, size);
            buffer[size] = 0;
            offset += size;
    
            cout << "Char Value: " << buffer << endl;

            cout << "Integer Value: " << *(int *)((char *)returnedData + offset ) << endl << endl;
            offset += 4;

            free(buffer);
        }
        j++;
        memset(returnedData, 0, 1000);
    }
    rmsi.close();
    cout << "Total number of tuples: " << j << endl << endl;

    cout << "****Test case 13 passed****" << endl << endl;
    free(returnedData);
}


void secA_14(const string &tableName, const vector<RID> &rids)
{
    // Functions Tested
    // 1. reorganize page
    // 2. delete tuples
    // 3. delete table
    cout << "****In Test case 14****" << endl;

    RC rc;
    rc = rm->reorganizePage(tableName, rids[1000].pageNum);
    assert(rc == success);

    rc = rm->deleteTuples(tableName);
    assert(rc == success);

    rc = rm->deleteTable(tableName);
    assert(rc == success);

    cout << "****Test case 14 passed****" << endl << endl;
}


void secA_15(const string &tableName) {

    cout << "****In Test case 15****" << endl;
    
    RID rid;    
    int tupleSize = 0;
    int numTuples = 500;
    void *tuple;
    void *returnedData = malloc(100);
    int ageVal = 25;

    RID rids[numTuples];
    vector<char *> tuples;

    RC rc = 0;
    int age;
    for(int i = 0; i < numTuples; i++)
    {
        tuple = malloc(100);

        // Insert Tuple
        float height = (float)i;
        
        age = (rand()%20) + 15;
        
        prepareTuple(6, "Tester", age, height, 123, tuple, &tupleSize);
        rc = rm->insertTuple(tableName, tuple, rid);
        assert(rc == success);

        tuples.push_back((char *)tuple);
        rids[i] = rid;
    }
    cout << "After Insertion!" << endl;

    // Set up the iterator
    RM_ScanIterator rmsi;
    string attr = "Age";
    vector<string> attributes;
    attributes.push_back(attr); 
    rc = rm->scan(tableName, attr, GT_OP, &ageVal, attributes, rmsi);
    assert(rc == success); 

    cout << "Scanned Data:" << endl;
    
    while(rmsi.getNextTuple(rid, returnedData) != RM_EOF)
    {
        cout << "Age: " << *(int *)returnedData << endl;
        assert ( (*(int *) returnedData) > ageVal );
    }
    rmsi.close();
    
    // Deleta Table
    rc = rm->deleteTable(tableName);
    assert(rc == success);

    free(returnedData);
    for(int i = 0; i < numTuples; i++)
    {
        free(tuples[i]);
    }
    
    cout << "****Test case 15 passed****" << endl << endl;
}


void Tests()
{
    // GetAttributes
    secA_0("tbl_employee");

    // Insert/Read Tuple
    secA_1("tbl_employee", 6, "Peters", 24, 170.1, 5000);

    // Delete Tuple
    secA_2("tbl_employee", 6, "Victor", 22, 180.2, 6000);

    // Update Tuple
    secA_3("tbl_employee", 6, "Thomas", 28, 187.3, 4000);

    // Read Attributes
    secA_4("tbl_employee", 6, "Veekay", 27, 171.4, 9000);

    // Delete Tuples
    secA_5("tbl_employee", 6, "Dillon", 29, 172.5, 7000);

    // Delete Table
    secA_6("tbl_employee", 6, "Martin", 26, 173.6, 8000);
   
    memProfile();
 
    // Reorganize Page
    createTable("tbl_employee2");
    secA_7("tbl_employee2");

    // Simple Scan
    createTable("tbl_employee3");
    secA_8("tbl_employee3");

    memProfile();
	
    // Pressure Test
    createLargeTable("tbl_employee4");

    vector<RID> rids;
    vector<int> sizes;

    // Insert Tuple
    secA_9("tbl_employee4", rids, sizes);
    // Read Tuple
    secA_10("tbl_employee4", rids, sizes);

    // Update Tuple
    secA_11("tbl_employee4", rids, sizes);

    // Delete Tuple
    secA_12("tbl_employee4", rids);

    memProfile();

    // Scan
    secA_13("tbl_employee4");

    // DeleteTuples/Table
    secA_14("tbl_employee4", rids);
    
    // Scan with conditions
    createTable("tbl_b_employee4");  
    secA_15("tbl_b_employee4");
    
    memProfile();
    return;
}

int main()
{
    // Basic Functions
    cout << endl << "Test Basic Functions..." << endl;

    // Create Table
    createTable("tbl_employee");

    Tests();

    return 0;
}

