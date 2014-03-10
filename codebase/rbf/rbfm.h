
#ifndef _rbfm_h_
#define _rbfm_h_

#include <string>
#include <vector>
#include <sstream>
#include <cstring>
#include <cassert>

#include "../rbf/pfm.h"



// Record ID
typedef struct
{
    unsigned pageNum;
    unsigned slotNum;
} RID;

inline bool operator==(const RID& lhs, const RID& rhs){
    return lhs.pageNum == rhs.pageNum && lhs.slotNum == rhs.slotNum;
}

inline bool operator!=(const RID& lhs, const RID& rhs){
    return !(lhs == rhs);
}

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
    int _len; //lenght occupied in memory
    string _sv;
    int _iv;
    float _fv;
    AttrValue(int v): _type(TypeInt), _len(sizeof(int)), _sv(""), _iv(v), _fv(0) {}
    AttrValue(float v): _type(TypeReal), _len(sizeof(float)), _sv(""), _iv(0), _fv(v)  {}
    AttrValue(string v): _type(TypeVarChar), _len(sizeof(int) + (int)v.length()), _sv(v), _iv(0), _fv(0)  {}
    AttrValue(): _len(0), _sv(""), _iv(0), _fv(0){} //intialize to same value, only different field value matter
    int readFromData(AttrType type, char *data); //return length of the content
    int writeToData(char *data); //return length of content
    static bool compareValue(AttrValue v, AttrValue cmp, CompOp op);
    void printSelf(void) const;
};

inline bool operator==(const AttrValue& lhs, const AttrValue& rhs){
    return lhs._fv == rhs._fv &&
    lhs._iv == rhs._iv && lhs._sv == rhs._sv;}
inline bool operator!=(const AttrValue& lhs, const AttrValue& rhs){return !operator==(lhs,rhs);}
inline bool operator< (const AttrValue& lhs, const AttrValue& rhs){return lhs._fv < rhs._fv ||
    lhs._iv < rhs._iv || lhs._sv < rhs._sv || (lhs._len == 0 && rhs._len != 0); } //this is for -infinity, _len = 0 means -inifinity
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


//TODO: *_value may should have copy, otherwise it's affected by caller's changing of *value
//not high priority
class RBFM_ScanIterator {
    friend class RecordBasedFileManager;
public:
    RBFM_ScanIterator() {}
    ~RBFM_ScanIterator() {}
    
    // "data" follows the same format as RecordBasedFileManager::insertRecord()
    RC getNextRecord(RID &rid, void *data);
    RC close() {
        PagedFileManager *pfm = PagedFileManager::instance();
        return pfm->closeFile(_fh);
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
    
    RC locateRecordRID(FileHandle &fh, const RID &rid, RID &tid);
    
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


class ValueStream{
public:
    ValueStream(void *data = NULL): _data((char *)data), _offset(0){}
    ~ValueStream(){
    }
    ValueStream& operator<<(int v){
        memcpy(get_pointer(), &v, sizeof(int));
        _offset += sizeof(int);
        return *this;
    }
    ValueStream& operator<<(float v){
        memcpy(get_pointer(), &v, sizeof(float));
        _offset += sizeof(float);
        return *this;
    }
    ValueStream& operator<<(double v){
        return (*this)<<((float)v);
    }
    ValueStream& operator<<(string s){
        int len = (int)s.length();
        (*this)<<len;
        memcpy(get_pointer(), s.c_str(), len);
        _offset += len;
        return *this;
    }
    ValueStream& operator>>(int &v){
        int tmp;
        memcpy(&tmp, get_pointer(), sizeof(int));
        v = tmp;
        _offset += sizeof(int);
        return *this;
    }
    ValueStream& operator>>(float &v){
        float tmp;
        memcpy(&tmp, get_pointer(), sizeof(int));
        v = tmp;
        _offset += sizeof(float);
        return *this;
    }
    ValueStream& operator>>(string &s){
        int len;
        (*this)>>len;
        char str[100] = {'\0'};
        memcpy(str, get_pointer(), len);
        s = string(str);
        _offset += len;
        return *this;
    }
    
    AttrValue read(const Attribute &attr){
        return read(attr.type);
    }
    
    AttrValue read(AttrType type){
        int v;
        float f;
        string s;
        switch (type) {
            case TypeInt:
                *this>>v;
                return AttrValue(v);
            case TypeReal:
                *this>>f;
                return AttrValue(f);
            case TypeVarChar:
                *this>>s;
                return AttrValue(s);
            default:
                assert(false); //should not reach here
                return AttrValue();
        }
    }
    
    ValueStream& write(const AttrValue av){
        switch (av._type) {
            case TypeInt:
                *this<<av._iv;
                break;
            case TypeReal:
                *this<<av._fv;
                break;
            case TypeVarChar:
                *this<<av._sv;
                break;
            default:
                assert(false);
                break;
        }
        return *this;
    }
    
    bool search(const vector<Attribute> &attrs, const string name, AttrValue &av){
        for (int i = 0; i < (int)attrs.size(); read(attrs[i]), i++)
            if (attrs[i].name == name){
                av = read(attrs[i]);
                return true;
            }
        return false;
    }
    
    ValueStream& set_offset(int off){
        _offset = off;
        return *this;
    }
    char *get_pointer(){
        return _data + _offset;
    }
private:
    char *_data;
    int _offset;
};

#endif
