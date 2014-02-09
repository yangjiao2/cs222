#ifndef cs222_btree_h
#define cs222_btree_h
#include "../rbf/pfm.h"
#include "../rbf/rbfm.h"

class BTreeNode{
public:
    
private:
    BTreeNode *left;
    BTreeNode *right;
    BTreeNode *parent;
    AttrType type;
    vector<AttrValue> keys;
    vector<BTreeNode *> childs;
    
    char _buffer[PAGE_SIZE];
    int _pgid;
    FileHandle fh;
};

#endif

