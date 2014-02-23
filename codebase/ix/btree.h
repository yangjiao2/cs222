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
    friend class BTreeNode;
    friend class LeafNode;
public:
    BTreeNode(FileHandle &fh, int pageNum);
    BTreeNode(FileHandle &fh, AttrType _type, int dep, int parent);
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
    void claimChilds();
    
    //returning whether split, (key, rid) no duplicates
    bool insert(AttrValue key, RID rid);
    
    //par_key is parent's key value next to pointer, pointing to this node
    //return whether merge
    bool Delete(AttrValue key, RID rid, vector<AttrValue>::iterator par_key);
    
    bool redistribute(vector<AttrValue>::iterator par_key);
    bool merge(vector<AttrValue>::iterator par_key);
    bool isSibling(int pgid, int &sib_size);
    
private:
    int _leftID; //no need to load neighbourings in advance, we have id => BTreeNode constructor
    int _rightID;
    int _parentID;
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
    LeafNode(BTreeNode *parent); // new a new leaf node
    ~LeafNode();
    
private:
    void dump();
    bool find(AttrValue key, RID rid);
    bool insert(AttrValue key, RID rid);
    AttrValue firstKeyValue();
    void split();
    int sizeOnDisk();
    bool shouldSplit();
    bool shouldMerge();
    bool isSibling(int pgid, int &size);
    
    //return whether this has merged, deciding right-merge or left-merge is parent's business:
    //make sure, merge always merge into current leaf node, not sibling's node, then
    //parent just need to see whether pkey <= _key[0] of child
    bool Delete(AttrValue av, RID rid, vector<AttrValue>::iterator par_key);
    
    bool redistribute(vector<AttrValue>::iterator par_key);
    bool merge();
    
private:
    int _leftID;
    int _rightID;
    int _parentID;
    AttrType _type;
    int _pgid;
    
    vector<AttrValue> _keys;
    vector<RID> _rids;
    
    char _buffer[PAGE_SIZE];
    FileHandle _fh;
};



#endif

