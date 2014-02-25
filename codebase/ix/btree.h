#ifndef cs222_btree_h
#define cs222_btree_h
#include "../rbf/pfm.h"
#include "../rbf/rbfm.h"
#include <cassert>
#include <algorithm>

#define EMPTY_NODE (-1)

typedef enum {
    BT_SUCCESS = 0,
    BT_FAIL,
    BT_SPLIT,
    BT_REDIST,
    BT_MERGE
} BTResult;


class IX_ScanIterator;

class BTreeNode{
    friend class LeafNode;
public:
    BTreeNode(FileHandle &fh, int pageNum);
    BTreeNode(FileHandle &fh, AttrType _type, int dep);
    AttrType getType(){
        return _type;
    }
    ~BTreeNode();
    bool find(AttrValue key, RID rid);
    RC rootInsert(const void *key, RID rid, BTreeNode * &newroot);
    RC rootDelete(const void *key, RID rid, BTreeNode * &newroot);
    
    RC getNextEntry(IX_ScanIterator &ix);
    
private:
    void dump();
    void split();
    int sizeOnDisk();
    bool shouldSplit();
    bool shouldMerge();
    AttrValue pushupFirstKey();
    
    //returning whether split, (key, rid) no duplicates
    bool insert(AttrValue key, RID rid);
    
    //just leave it there
    void Delete(AttrValue key, RID rid);
    
    bool redistribute(vector<AttrValue>::iterator par_key);
    bool merge(vector<AttrValue>::iterator par_key);
    bool isSibling(int pgid, int &sib_size);
    
private:
    int _leftID; //no need to load neighbourings in advance, we have id => BTreeNode constructor
    int _rightID;
    vector<AttrValue> _keys;
    vector<int> _childs;
    int _depth;
    AttrType _type;
    
    char _buffer[PAGE_SIZE];
    int _pgid;
    FileHandle _fh;
};

class LeafNode{
    friend class BTreeNode;
public:
    LeafNode(FileHandle &fh, int pageNum);
    LeafNode(FileHandle &fh, AttrType type); // new a new leaf node
    
private:
    void dump();
    bool find(AttrValue key, RID rid);
    bool insert(AttrValue key, RID rid);
    AttrValue firstKeyValue();
    void split();
    int sizeOnDisk();
    bool shouldSplit();
    //just leave it there
    void Delete(AttrValue av, RID rid);

    void spill(AttrValue key, RID rid);
    //get whole entries, including its overflow page
    void get_whole_entries(vector<AttrValue> &keys, vector<RID> &rids);
    
private:
    int _leftID;
    int _rightID;
    int _nextID; //overflow page id
    AttrType _type;
    int _pgid;
    
    vector<AttrValue> _keys;
    vector<RID> _rids;
    
    char _buffer[PAGE_SIZE];
    FileHandle _fh;
};



#endif

