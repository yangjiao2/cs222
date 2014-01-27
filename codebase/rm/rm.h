
#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>

#include "../rbf/rbfm.h"



#define RM_CATALOG_NAME "catalog"
#define RM_ATTRIBUTES_NAME "attributes"
#define RM_TABLE_FILENAME(x) (x) //table file name is same as table name



# define RM_EOF (-1)  // end of a scan operator

// RM_ScanIterator is an iteratr to go through tuples
// The way to use it is like the following:
//  RM_ScanIterator rmScanIterator;
//  rm.open(..., rmScanIterator);
//  while (rmScanIterator(rid, data) != RM_EOF) {
//    process the data;
//  }
//  rmScanIterator.close();

class RM_ScanIterator {
public:
    RM_ScanIterator() {};
    ~RM_ScanIterator() {};
    
    // "data" follows the same format as RelationManager::insertTuple()
    RC getNextTuple(RID &rid, void *data) { return RM_EOF; };
    RC close() { return -1; };
};


// Relation Manager
class RelationManager
{
public:
    static RelationManager* instance();
    
    RC createTable(const string &tableName, const vector<Attribute> &attrs);
    
    RC deleteTable(const string &tableName);
    
    RC getAttributes(const string &tableName, vector<Attribute> &attrs);
    
    RC insertTuple(const string &tableName, const void *data, RID &rid);
    
    RC deleteTuples(const string &tableName);
    
    RC deleteTuple(const string &tableName, const RID &rid);
    
    // Assume the rid does not change after update
    RC updateTuple(const string &tableName, const void *data, const RID &rid);
    
    RC readTuple(const string &tableName, const RID &rid, void *data);
    
    RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);
    
    RC reorganizePage(const string &tableName, const unsigned pageNumber);
    
    // scan returns an iterator to allow the caller to go through the results one by one.
    RC scan(const string &tableName,
            const string &conditionAttribute,
            const CompOp compOp,                  // comparision type such as "<" and "="
            const void *value,                    // used in the comparison
            const vector<string> &attributeNames, // a list of projected attributes
            RM_ScanIterator &rm_ScanIterator);
    
    
private:
    vector<Attribute> prepareCatalogDescriptor(void);
    vector<Attribute> prepareAttrDescriptor(void);
    
    int prepareCatalogData(string tableName, char *data);
    int prepareAttrData(string tableName, Attribute attr, char *data);
    void RetrieveMetaInfo(void);
    
    
    
    // Extra credit
public:
    RC dropAttribute(const string &tableName, const string &attributeName);
    
    RC addAttribute(const string &tableName, const Attribute &attr);
    
    RC reorganizeTable(const string &tableName);
    
    
protected:
    RelationManager();
    ~RelationManager();
    
private:
    static RelationManager *_rm;
    RecordBasedFileManager *rbfm;
    FileHandle catalog_fh;
    FileHandle attr_fh;
    std::map<string, int> tbname_to_id;//table name to id
    std::map<int, string> tid_to_tbname; //id to tablename
    std::map<string, vector<Attribute> > tbname_to_desp; //table name to descriptor
    int _new_tbid;
};

#endif
