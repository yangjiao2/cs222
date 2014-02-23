
#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rbf/rbfm.h"
#include "../ix/btree.h"

# define IX_EOF (-1)  // end of the index scan

class IX_ScanIterator;

class IndexManager {
public:
    static IndexManager* instance();
    
    RC createFile(const string &fileName);
    
    RC destroyFile(const string &fileName);
    
    RC openFile(const string &fileName, FileHandle &fileHandle);
    
    RC closeFile(FileHandle &fileHandle);
    
    RC checkRootMap(FileHandle &fh, const Attribute &attr);
    
    // The following two functions are using the following format for the passed key value.
    //  1) data is a concatenation of values of the attributes
    //  2) For int and real: use 4 bytes to store the value;
    //     For varchar: use 4 bytes to store the length of characters, then store the actual characters.
    RC insertEntry(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid);  // Insert new index entry
    RC deleteEntry(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid);  // Delete index entry
    
    // scan() returns an iterator to allow the caller to go through the results
    // one by one in the range(lowKey, highKey).
    // For the format of "lowKey" and "highKey", please see insertEntry()
    // If lowKeyInclusive (or highKeyInclusive) is true, then lowKey (or highKey)
    // should be included in the scan
    // If lowKey is null, then the range is -infinity to highKey
    // If highKey is null, then the range is lowKey to +infinity
    RC scan(FileHandle &fileHandle,
            const Attribute &attribute,
            const void        *lowKey,
            const void        *highKey,
            bool        lowKeyInclusive,
            bool        highKeyInclusive,
            IX_ScanIterator &ix_ScanIterator);
    RC getNextEntry(IX_ScanIterator &ix, void *key, RID &rid);
    
protected:
    IndexManager   ();                            // Constructor
    ~IndexManager  ();                            // Destructor
    
private:
    static IndexManager *_index_manager;
    map<string, BTreeNode *> rootsMap;
};

class IX_ScanIterator {
    friend class IndexManager;
    friend class BTreeNode;
public:
    IX_ScanIterator();  							// Constructor
    ~IX_ScanIterator(); 							// Destructor
    
    RC getNextEntry(RID &rid, void *key);  		// Get next matching entry
    RC close();             						// Terminate index scan
private:
    bool isOK(const AttrValue &val){
        bool ok = true;
        if (_low._len != 0)
            ok &= _inc_low ? val >= _low : val > _low;
        if (_high._len != 0)
            ok &= _inc_high ? val <= _high : val < _high;
        return ok;
    }
    
private:
    Attribute _attr;
    //using _len == 0 indicates is infinity
    AttrValue _low;
    AttrValue _high;
    bool _inc_low;
    bool _inc_high;
    FileHandle _fh;
    AttrValue _last_key;
    RID _rid;
};

// print out the error message for a given return code
void IX_PrintError (RC rc);


#endif
