#ifndef cs222_btree_h
#define cs222_btree_h
#include "../rbf/pfm.h"
#include "../rbf/rbfm.h"
#include <cassert>

#define EMPTY_NODE (-1)

class BTreeNode{
    friend class BTreeNode;
    friend class LeafNode;
public:
    BTreeNode(FileHandle &fh, int pageNum);
    BTreeNode(FileHandle &fh, AttrType _type, int dep, int parent);
    bool find(void *key, RID rid);
    bool insert(void *key, RID rid); //returning whether split, (key, rid) no duplicates
    
private:
    void dump();
    int sizeOnDisk();
    bool shouldSplit();
    bool shouldMerge();
    
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
    void dump();
    bool find(void *key, RID rid);
    bool insert(void *key, RID rid);

private:
    int sizeOnDisk();
    bool shouldSplit();
    bool shouldMerge();
    
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

