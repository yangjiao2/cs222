
#include "btree.h"

#define UNIT_SIZE (sizeof(int))

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
    _pgid = pageNum;
    
    AttrValue av;
    offset = 0;
    _childs.resize(N + 1);
    _keys.resize(N);
    for (int i = 0; i < N; i++){
        memcpy(&_childs[i], _buffer + offset, UNIT_SIZE);
        offset += UNIT_SIZE;
        offset += av.readFromData(_type, _buffer + offset);
        _keys[i] = av;
    }
    memcpy(&_childs[N], _buffer + offset, UNIT_SIZE);
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
    _childs.clear();
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
    offset = 0;
    for (int i = 0; i < N; i++){
        memcpy(_buffer + offset, &_childs[i], UNIT_SIZE);
        offset += UNIT_SIZE;
        offset += _keys[i].writeToData(_buffer + offset);
    }
    memcpy(_buffer + offset, &_childs[N], UNIT_SIZE);
    _fh.writePage(_pgid, _buffer);
}

/*
 int BTreeNode::freeSpace(){
 int N = (int)_keys.size(), offset;
 memcpy(&offset, _buffer + PAGE_SIZE - (N + 7) * sizeof(int), sizeof(int));
 //reserve 2 for future inserting slot
 return PAGE_SIZE - ((N + 10) * sizeof(int) + offset);
 }
 */

/*
 bool BTreeNode::canAcceptNewEntry(void *data){
 AttrValue av;
 // key entry size + child entry size
 return av.readFromData(_type, (char *)data) + sizeof(int) < freeSpace();
 }*/

int BTreeNode::sizeOnDisk(){
    int N = (int) _keys.size();
    int size = UNIT_SIZE * (6 + N + 1 + N + 1);
    for (int i = 0; i < N; i++)
        size += _keys[i]._len;
    return size;
}

bool BTreeNode::shouldMerge(){
    return sizeOnDisk() < PAGE_SIZE / 2;
}

bool BTreeNode::shouldSplit(){
    return sizeOnDisk() >= PAGE_SIZE;
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
    int offset = PAGE_SIZE - UNIT_SIZE;
    for (int i = 0; i < 5; i++, offset -= UNIT_SIZE)
        memcpy(&arr[i], _buffer + offset, UNIT_SIZE);
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
        memcpy(&_rids[i].pageNum, _buffer + offset, UNIT_SIZE);
        memcpy(&_rids[i].slotNum, _buffer + offset + UNIT_SIZE, UNIT_SIZE);
        offset += 2 * UNIT_SIZE;
    }
}


//Note: calling
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
    int N = (int)_keys.size(), arr[5];
    arr[0] = N;
    arr[1] = _leftID;
    arr[2] = _rightID;
    arr[3] = _parentID;
    arr[4] = _type;
    int offset = PAGE_SIZE - 4;
    for (int i = 0; i < 5; i++, offset -= 4)
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

/*
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

bool LeafNode::canAcceptNewEntry(void *keyData){
    AttrValue av;
    //key entry size + rid entry size
    return freeSpace() >= av.readFromData(_type, (char *)keyData) + 2 * sizeof(int);
}*/


int LeafNode::sizeOnDisk(){
    int size = 0;
    return size;
}

bool LeafNode::shouldMerge(){
    return sizeOnDisk() < PAGE_SIZE / 2;
}

bool LeafNode::shouldSplit(){
    return sizeOnDisk() >= PAGE_SIZE / 2;
}
AttrValue LeafNode::firstKeyValue(){
    assert(!_keys.empty());
    return _keys[0];
}

//TODO: change to binary search later
bool LeafNode::find(void *key, RID rid){
    AttrValue av;
    av.readFromData(_type, (char *)key);
    int i, N = (int)_keys.size();
    for (i = 0; i < N; i++)
        if (_rids[i] == rid && av == _keys[i])
            return true;
    return false;
}


void LeafNode::split(){
    assert(shouldSplit());
    int N = (int) _keys.size();
    int range = N / 2;
    BTreeNode parent(_fh, _parentID);
    LeafNode nbr(&parent);
    nbr._leftID = _pgid;
    nbr._rightID = _rightID;
    _rightID = nbr._pgid;
    nbr._keys.resize(N - range);
    nbr._rids.resize(N - range);
    copy(_keys.begin() + range, _keys.end(), nbr._keys.begin());
    copy(_rids.begin() + range, _rids.end(), nbr._rids.begin());
    _keys.erase(_keys.begin() + range, _keys.end());
    _rids.erase(_rids.begin() + range, _rids.end());
    nbr.dump();
}

//return whether splitted
bool LeafNode::insert(void *key, RID rid){
    AttrValue av;
    av.readFromData(_type, (char *)key);
    vector<AttrValue>::iterator ind = lower_bound(_keys.begin(), _keys.end(), av);
    assert(*ind != av); //assume no same key multiple entries case.
    int i = (int)(ind - _keys.begin());
    _keys.insert(_keys.begin() + i, av);
    _rids.insert(_rids.begin() + i, rid);
    bool splitted = shouldSplit();
    if (splitted)
        split();
    dump();
    return splitted;
}

AttrValue BTreeNode::pushupFirstKey(){
    assert(!_keys.empty());
    assert(_childs.size() == _keys.size() + 1);
    AttrValue av = _keys[0];
    _keys.erase(_keys.begin());
    _childs.erase(_childs.begin());
    dump();
    return av;
}

/*
 current node   =>      _keys: [0, range),      _childs: [0, range] = [0, range + 1)
 new splitted   =>      _keys: [range, end)     _childs: [range, end')
 note: _childs[range] would be in both nodes, but
 splitted node's _keys[0], _childs[0] would be removed in pushup
 */
void BTreeNode::split(){
    assert(shouldSplit());
    int N = (int)_keys.size();
    int range = N / 2;
    BTreeNode nbr(_fh,_type, _depth, _parentID);
    nbr._keys.resize(N - range);
    nbr._childs.resize(N - range + 1);
    nbr._leftID = _pgid;
    nbr._rightID = _rightID;
    _rightID = nbr._pgid;
    copy(_keys.begin() + range, _keys.end(), nbr._keys.begin());
    copy(_childs.begin() + range, _childs.end(), nbr._childs.begin());
    _keys.erase(_keys.begin() + range, _keys.end());
    _childs.erase(_childs.begin() + range + 1, _childs.end());
    nbr.dump();
}

//TODO: write some print BTreeNode function to debug

bool BTreeNode::insert(void *key, RID rid){
    AttrValue av;
    av.readFromData(_type, (char *)key);
    //root, first time, empty, special handle
    if (_keys.empty()){
        _keys.resize(1, av);
        _childs.resize(2, EMPTY_NODE);
    }
    //index: next level's _childs[index] goes down to
    int index = (int)(upper_bound(_keys.begin(), _keys.end(), av) - _keys.begin());
    int &next = _childs[index];
    if (_depth == 1) {
        if (next == EMPTY_NODE){
            LeafNode leaf(this);
            leaf.insert(key, rid);
            next = leaf._pgid;
            return false;
        }
        LeafNode leaf(_fh, next);
        bool childsplit = leaf.insert(key, rid);
        if (childsplit){
            LeafNode nbr(_fh, leaf._rightID);
            _keys.insert(_keys.begin() + index, nbr.firstKeyValue());
            _childs.insert(_childs.begin() + index + 1, nbr._pgid);
        }
    }
    else{
        BTreeNode bnode(_fh, next);
        bool childsplit = bnode.insert(key, rid);
        if (childsplit) {
            BTreeNode bnbr(_fh, bnode._rightID);
            _keys.insert(_keys.begin() + index, bnbr.pushupFirstKey()); // TODO: maybe should also remove _childs[0];
            _childs.insert(_childs.begin() + index + 1, bnbr._pgid);
        }
    }
    bool splitted = shouldSplit();
    if (splitted)
        split();
    dump();
    return splitted;
}


