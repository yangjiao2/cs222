
#include "btree.h"

static PagedFileManager *pfm = PagedFileManager::instance();

BTreeNode::BTreeNode(FileHandle &fh, int pageNum){
    fh.readPage(pageNum, _buffer);
    pfm->openFile(fh._fh_name.c_str(), _fh);
    int N, arr[6], offset = PAGE_SIZE - 4;
    
    for (int i = 0; i < 6; offset -= 4, i++)
        memcpy(&arr[i], _buffer + offset, sizeof(int));
    N = arr[0];
    _leftID = arr[1];
    _rightID = arr[2];
    _parentID = arr[3];
    _type = (AttrType)arr[4];
    _depth = arr[5];
    _childs.resize(N + 1);
    _keys.resize(N);
    _pgid = pageNum;
    vector<int> pos(N+1);
    for (int i = 0; i <= N; i++, offset -= 4){
        memcpy(&pos[i], _buffer + offset, sizeof(int));
        memcpy(&_childs[i], _buffer + pos[i], sizeof(int));
    }
    AttrValue av;
    for (int i = 0; i < N; i++) {
        av.readFromData(_type, _buffer + pos[i] + sizeof(int));
        _keys[i] = av;
    }
}

BTreeNode::BTreeNode(FileHandle &fh, AttrType type, int dep, int parent){
    pfm->openFile(fh._fh_name.c_str(), _fh);
    memset(_buffer, 0, sizeof(_buffer));
    _parentID = parent;
    _depth = dep;
    _type = type;
    _leftID = _rightID = EMPTY_NODE;
    _fh.appendPage(_buffer);
    _pgid =  _fh.getNumberOfPages() - 1;
    _childs = vector<int>(1, EMPTY_NODE);
    dump();
}

void BTreeNode::dump(){
    int N = (int) _keys.size(), arr[6];
    arr[0] = N;
    arr[1] = _leftID;
    arr[2] = _rightID;
    arr[3] = _parentID;
    arr[4] = (int) _type;
    arr[5] = _depth;
    int offset = PAGE_SIZE - 4;
    for (int i = 0; i < 6; offset -= 4, i++)
        memcpy(_buffer + offset, &arr[i], sizeof(int));
    int pointer = 0;
    for (int i = 0; i <= N; i++, offset -= 4) {
        memcpy(_buffer + offset, &pointer, sizeof(int));
        memcpy(_buffer + pointer, &_childs[i], sizeof(int));
        if (i == N) continue;
        pointer += sizeof(int);
        pointer += _keys[i].writeToData(_buffer + pointer);
    }
    _fh.writePage(_pgid, _buffer);
}

int BTreeNode::freeSpace(){
    int N = (int)_keys.size(), offset;
    memcpy(&offset, _buffer + PAGE_SIZE - (N + 7) * sizeof(int), sizeof(int));
    //reserve 2 for future inserting slot
    return PAGE_SIZE - ((N + 10) * sizeof(int) + offset);
}

bool BTreeNode::moreThanHalf(){
    return freeSpace() <= PAGE_SIZE / 2;
}

bool BTreeNode::canAccept(void *data){
    AttrValue av;
    return av.readFromData(_type, (char *)data) < freeSpace();
}

//TODO: change to binary search later
bool BTreeNode::find(void *key, RID rid){
    AttrValue av;
    av.readFromData(_type, (char *)key);
    int N = (int)_keys.size(), next, i = 0;
    for (; i < N; i++)
        if (AttrValue::compareValue(_keys[i], av, GT_OP))
            break;
    if (i == N)
        next = _childs[N];
    else if (AttrValue::compareValue(_keys[i], av, LE_OP))
        next = _childs[i+1];
    else
        next = _childs[i];
    if (next == EMPTY_NODE)
        return false;
    if (_depth == 1) {
        LeafNode leaf(_fh, next);
        return leaf.find(key, rid);
    }
    BTreeNode inner(_fh, next);
    return inner.find(key, rid);
}


LeafNode::LeafNode(FileHandle &fh, int pageNum){
    fh.readPage(pageNum, _buffer);
    pfm->openFile(fh._fh_name.c_str(), _fh);
    int N, arr[5];
    int offset = PAGE_SIZE - 4;
    for (int i = 0; i < 5; i++, offset -= 4)
        memcpy(&arr[i], _buffer + offset, sizeof(int));
    N = arr[0];
    _leftID = arr[1];
    _rightID = arr[2];
    _parentID = arr[3];
    _type = (AttrType) arr[4];
    _pgid = pageNum;
    _rids.resize(N);
    _keys.resize(N);
    
    offset = 0;
    for (int i = 0; i < N; i++) {
        offset += _keys[i].readFromData(_type, _buffer + offset);
        memcpy(&_rids[i].pageNum, _buffer + offset, sizeof(int));
        memcpy(&_rids[i].slotNum, _buffer + offset + 4, sizeof(int));
        offset += 8;
    }
}

LeafNode::LeafNode(BTreeNode *parent){
    assert(parent);
    pfm->openFile(parent->_fh._fh_name.c_str(), _fh);
    _parentID = parent->_pgid;
    _type = parent->_type;
    _leftID = _rightID = EMPTY_NODE;
    _fh.appendPage(_buffer);
    _pgid = _fh.getNumberOfPages() - 1;
    dump();
}

void LeafNode::dump(){
    int N = (int)_keys.size(), arr[4];
    arr[0] = N;
    arr[1] = _leftID;
    arr[2] = _rightID;
    arr[3] = _parentID;
    int offset = PAGE_SIZE - 4;
    for (int i = 0; i < 4; i++, offset -= 4)
        memcpy(_buffer + offset, &arr[i], sizeof(int));
    offset = 0;
    for (int i = 0; i < N; i++) {
        offset += _keys[i].writeToData(_buffer + offset);
        memcpy(_buffer + offset, &_rids[i].pageNum, sizeof(int));
        memcpy(_buffer + offset + 4, &_rids[i].slotNum, sizeof(int));
        offset += 8;
    }
    _fh.writePage(_pgid, _buffer);
}

int LeafNode::freeSpace(){
    int N = (int)_keys.size(), offset;
    int total =(N + 5) * sizeof(int) + 3 * sizeof(int); //2 reserve for RID, one for slot
    if (N > 0){
        memcpy(&offset, _buffer + PAGE_SIZE - 4 * (N + 5), sizeof(int));
        total += _keys[N-1]._len + offset + sizeof(int) * 2;
    }
    return PAGE_SIZE - total;
}

bool LeafNode::moreThanHalf(){
    return freeSpace() <= PAGE_SIZE / 2;
}

bool LeafNode::canAccept(void *data){
    AttrValue av;
    return freeSpace() >= av.readFromData(_type, (char *)data);
}

//TODO: change to binary search later
bool LeafNode::find(void *key, RID rid){
    AttrValue av;
    av.readFromData(_type, (char *)key);
    int i, N = (int)_keys.size();
    for (i = 0; i < N; i++)
        if (_rids[i] == rid && AttrValue::compareValue(av, _keys[i], EQ_OP))
            return true;
    return false;
}





