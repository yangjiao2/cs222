
#ifndef _rbfm_h_
#define _rbfm_h_

#include <string>
#include <vector>
#include <sstream>
#include <cstring>

#include "../rbf/pfm.h"

using namespace std;


// Record ID
typedef struct
{
    unsigned pageNum;
    unsigned slotNum;
} RID;


// Attribute
typedef enum { TypeInt = 0, TypeReal, TypeVarChar, TypeNone } AttrType;

typedef unsigned AttrLength;

struct Attribute {
    string   name;     // attribute name
    AttrType type;     // attribute type
    AttrLength length; // attribute length
    Attribute(){}
    Attribute(string _name, AttrType _type, AttrLength _len):
    name(_name), type(_type), length(_len){}
};


// Comparison Operator (NOT needed for part 1 of the project)
typedef enum { EQ_OP = 0,  // =
               LT_OP,      // <
               GT_OP,      // >
               LE_OP,      // <=
               GE_OP,      // >=
               NE_OP,      // !=
               NO_OP       // no condition
             } CompOp;


class AttrValue{
public:
    AttrType _type;
    int _len;
    string _sv;
    int _iv;
    float _fv;
    AttrValue(): _len(0), _sv(""), _iv(0), _fv(0){} //intialize to same value, only different field value matter
    int readFromData(AttrType type, char *data); //return length of the content
    int writeToData(char *data); //return length of content
    static bool compareValue(AttrValue v, AttrValue cmp, CompOp op);
    void printSelf(void);
};

inline bool operator==(const AttrValue& lhs, const AttrValue& rhs){ return lhs._fv == rhs._fv &&
    lhs._iv == rhs._iv && lhs._fv == rhs._fv;}
inline bool operator!=(const AttrValue& lhs, const AttrValue& rhs){return !operator==(lhs,rhs);}
inline bool operator< (const AttrValue& lhs, const AttrValue& rhs){return lhs._fv < rhs._fv ||
    lhs._iv < rhs._iv || lhs._sv < rhs._sv;}
inline bool operator> (const AttrValue& lhs, const AttrValue& rhs){return  operator< (rhs,lhs);}
inline bool operator<=(const AttrValue& lhs, const AttrValue& rhs){return !operator> (lhs,rhs);}
inline bool operator>=(const AttrValue& lhs, const AttrValue& rhs){return !operator< (lhs,rhs);}


/****************************************************************************
The scan iterator is NOT required to be implemented for part 1 of the project
*****************************************************************************/

# define RBFM_EOF (-1)  // end of a scan operator

// RBFM_ScanIterator is an iteratr to go through records
// The way to use it is like the following:
//  RBFM_ScanIterator rbfmScanIterator;
//  rbfm.open(..., rbfmScanIterator);
//  while (rbfmScanIterator(rid, data) != RBFM_EOF) {
//    process the data;
//  }
//  rbfmScanIterator.close();


class RBFM_ScanIterator {
    friend class RecordBasedFileManager;
public:
    RBFM_ScanIterator() {}
    ~RBFM_ScanIterator() {}

    // "data" follows the same format as RecordBasedFileManager::insertRecord()
    RC getNextRecord(RID &rid, void *data);
    RC close() {
        return -1;
    }
    AttrType getAttrType(string name);
private:
    RID _rid;
    FileHandle _fh;
    vector<Attribute> _descriptor;
    vector<string> _projected;
    CompOp _opr;
    string _condAttr;
    char *_value;
};


class RecordBasedFileManager
{
    friend class RBFM_ScanIterator;
public:
    static RecordBasedFileManager* instance();
    
    static int getAttrsLength(vector<Attribute> atrs);

    RC createFile(const string &fileName);

    RC destroyFile(const string &fileName);

    RC openFile(const string &fileName, FileHandle &fileHandle);

    RC closeFile(FileHandle &fileHandle);

    //  Format of the data passed into the function is the following:
    //  1) data is a concatenation of values of the attributes
    //  2) For int and real: use 4 bytes to store the value;
    //     For varchar: use 4 bytes to store the length of characters, then store the actual characters.
    //  !!!The same format is used for updateRecord(), the returned data of readRecord(), and readAttribute()
    RC insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid);

    RC readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data);

    // This method will be mainly used for debugging/testing
    RC printRecord(const vector<Attribute> &recordDescriptor, const void *data);

    /**************************************************************************************************************************************************************
    ***************************************************************************************************************************************************************
    IMPORTANT, PLEASE READ: All methods below this comment (other than the constructor and destructor) are NOT required to be implemented for part 1 of the project
    ***************************************************************************************************************************************************************
    ***************************************************************************************************************************************************************/
    RC deleteRecords(FileHandle &fileHandle);

    RC deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid);

    // Assume the rid does not change after update
    RC updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid);

    RC readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string attributeName, void *data);

    RC reorganizePage(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const unsigned pageNumber);

    // scan returns an iterator to allow the caller to go through the results one by one.
    RC scan(FileHandle &fileHandle,
            const vector<Attribute> &recordDescriptor,
            const string &conditionAttribute,
            const CompOp compOp,                  // comparision type such as "<" and "="
            const void *value,                    // used in the comparison
            const vector<string> &attributeNames, // a list of projected attributes
            RBFM_ScanIterator &rbfm_ScanIterator);
    
    RID locateRecordRID(FileHandle &fh, const RID &rid);
    
private:
    RC getNextRecord(RBFM_ScanIterator &rs, void *data);
    RC meetRequirement(string v, string cmp, CompOp op);
    RC meetRequirement(int v, int cmp, CompOp op);
    RC meetRequirement(float v, float cmp, CompOp op);


// Extra credit for part 2 of the project, please ignore for part 1 of the project
public:

    RC reorganizeFile(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor);


protected:
    RecordBasedFileManager();
    ~RecordBasedFileManager();

private:
    static RecordBasedFileManager *_rbf_manager;
	PagedFileManager *pfm;
};


#endif
