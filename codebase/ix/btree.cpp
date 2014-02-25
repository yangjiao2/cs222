#include "btree.h"
#include "ix.h"

#define UNIT_SIZE (sizeof(int))
#define pfm (PagedFileManager::instance())


//[begin, end) subvector of vector x
template <typename T>
vector<T> subvector(vector<T> &x, int start, int end){
    return vector<T>(x.begin() + start, x.begin() + end);
}

template <typename T>
vector<T> concate_vectors(vector<T> x, vector<T> y) {
    vector<T> z(x);
    z.insert(z.end(), y.begin(), y.end());
    return z;
}

void BTreeNode::setRootPageID(FileHandle &fh, int v){
    char buffer[PAGE_SIZE];
    memset(buffer, 0, sizeof(buffer));
    memcpy(buffer, &v, UNIT_SIZE);
    fh.writePage(0, buffer);
}

int BTreeNode::getRootPageID(FileHandle &fh){
    char buffer[PAGE_SIZE];
    fh.readPage(0, buffer);
    int v;
    memcpy(&v, buffer, UNIT_SIZE);
    return v;
}


BTreeNode::~BTreeNode(){
    dump();
    pfm->closeFile(_fh);
}

BTreeNode::BTreeNode(FileHandle &fh, int pageNum){
    fh.readPage(pageNum, _buffer);
    pfm->openFile(fh._fh_name.c_str(), _fh);
    int N, arr[5], offset = PAGE_SIZE - 4;
    
    for (int i = 0; i < 6; offset -= 4, i++)
        memcpy(&arr[i], _buffer + offset, sizeof(int));
    N = arr[0];
    _leftID = arr[1];
    _rightID = arr[2];
    _type = (AttrType)arr[3];
    _depth = arr[4];
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

BTreeNode::BTreeNode(FileHandle &fh, AttrType type, int dep){
    pfm->openFile(fh._fh_name.c_str(), _fh);
    memset(_buffer, 0, sizeof(_buffer));
    _depth = dep;
    _type = type;
    _leftID = _rightID = EMPTY_NODE;
    _fh.appendPage(_buffer);
    _pgid =  _fh.getNumberOfPages() - 1;
    _childs = vector<int>(1, EMPTY_NODE);
}

void BTreeNode::dump(){
    int N = (int) _keys.size(), arr[6];
    arr[0] = N;
    arr[1] = _leftID;
    arr[2] = _rightID;
    arr[3] = (int) _type;
    arr[4] = _depth;
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


bool BTreeNode::find(AttrValue key, RID rid){
    int index = (int)(upper_bound(_keys.begin(), _keys.end(), key) - _keys.begin());
    int next = _childs[index];
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
    _nextID = arr[3];
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

LeafNode::LeafNode(FileHandle &fh, AttrType type){
    pfm->openFile(fh._fh_name.c_str(), _fh);
    _nextID = EMPTY_NODE;
    _type = type;
    _leftID = _rightID = EMPTY_NODE;
    _fh.appendPage(_buffer);
    _pgid = _fh.getNumberOfPages() - 1;
}

//Note: dump() should not be eagerly called! As its size may greater than PAGE_SIZE, causing overwrite
//_fh's field (see comments in LeafNode::inset())not easy to spot, best way to debug is using your mind... :)
void LeafNode::dump(){
    assert(!shouldSplit());
    int N = (int)_keys.size(), arr[5];
    arr[0] = N;
    arr[1] = _leftID;
    arr[2] = _rightID;
    arr[3] = _nextID;
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
        assert(offset < PAGE_SIZE);
    }
    _fh.writePage(_pgid, _buffer);
}


int LeafNode::sizeOnDisk(){
    int size = UNIT_SIZE * 5;
    for (int i = 0; i < (int) _keys.size(); i++) {
        size += _keys[i]._len;
        size += UNIT_SIZE * 2;
    }
    return size;
}

bool LeafNode::shouldSplit(){
    return sizeOnDisk() >= PAGE_SIZE;
}
AttrValue LeafNode::firstKeyValue(){
    assert(!_keys.empty());
    return _keys[0];
}

bool LeafNode::find(AttrValue key, RID rid){
    vector<AttrValue>::iterator ind = lower_bound(_keys.begin(), _keys.end(), key);
    if (ind == _keys.end() || *ind != key)
        return false;
    int index = (int) (ind - _keys.begin());
    for (; index < (int)_keys.size() && _keys[index] == key; index++)
        if (_rids[index].pageNum == rid.pageNum && _rids[index].slotNum == rid.slotNum)
            return true;
    return false;
}


void LeafNode::split(){
    assert(shouldSplit());
    int N = (int) _keys.size();
    int range = max(N / 2, (int)
                    (upper_bound(_keys.begin(), _keys.end(), _keys[0]) - _keys.begin()));
    assert(range < (int) _keys.size());
    LeafNode nbr(_fh, _type);
    nbr._leftID = _pgid;
    nbr._rightID = _rightID;
    _rightID = nbr._pgid;
    nbr._keys.resize(N - range);
    nbr._rids.resize(N - range);
    copy(_keys.begin() + range, _keys.end(), nbr._keys.begin());
    copy(_rids.begin() + range, _rids.end(), nbr._rids.begin());
    _keys.erase(_keys.begin() + range, _keys.end());
    _rids.erase(_rids.begin() + range, _rids.end());
}

//return whether splitted
bool LeafNode::insert(AttrValue key, RID rid){
    vector<AttrValue>::iterator ind = lower_bound(_keys.begin(), _keys.end(), key);
    int i = (int)(ind - _keys.begin());
    for (; ind != _keys.end() && *ind == key; ind++){
        RID tmp =_rids[ind-_keys.begin()];
        assert( tmp.pageNum != rid.pageNum || tmp.slotNum != rid.slotNum);
    }
    _keys.insert(_keys.begin() + i, key);
    _rids.insert(_rids.begin() + i, rid);
    if (!shouldSplit())
        return false;
    //try to spill
    if (_keys[0] == _keys.back()){
        spill(_keys.back(), _rids.back());
        _keys.pop_back();
        _rids.pop_back();
    }
    if (!shouldSplit())
        return false;
    split();
    return true;
}

AttrValue BTreeNode::pushupFirstKey(){
    assert(!_keys.empty());
    assert(_childs.size() == _keys.size() + 1);
    AttrValue av = _keys[0];
    _keys.erase(_keys.begin());
    _childs.erase(_childs.begin());
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
    BTreeNode nbr(_fh,_type, _depth);
    nbr._keys.resize(N - range);
    nbr._childs.resize(N - range + 1);
    nbr._leftID = _pgid;
    nbr._rightID = _rightID;
    _rightID = nbr._pgid;
    copy(_keys.begin() + range, _keys.end(), nbr._keys.begin());
    copy(_childs.begin() + range, _childs.end(), nbr._childs.begin());
    _keys.erase(_keys.begin() + range, _keys.end());
    _childs.erase(_childs.begin() + range + 1, _childs.end());
}

bool BTreeNode::insert(AttrValue key, RID rid){
    //Root Node first time insert: no need for special case, leave _keys empty, 1 entry of _childs.
    //it would split automatically after only child spills.
    
    //index: next level's _childs[index] goes down to
    int index = (int)(upper_bound(_keys.begin(), _keys.end(), key) - _keys.begin());
    int &next = _childs[index];
    if (_depth == 1) {
        if (next == EMPTY_NODE){
            LeafNode leaf(_fh, _type);
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
            _keys.insert(_keys.begin() + index, bnbr.pushupFirstKey()); // pushupFirstKey() also remove _childs[0];
            _childs.insert(_childs.begin() + index + 1, bnbr._pgid);
        }
    }
    bool splitted = shouldSplit();
    if (splitted)
        split();
    return splitted;
}

RC BTreeNode::rootInsert(const void *key, RID rid, BTreeNode * &newroot){
    AttrValue vkey;
    vkey.readFromData(_type, (char *)key);
    if (find(vkey, rid))
        return -1;
    bool split = insert(vkey, rid);
    if (!split){
        newroot = this;
    }
    else{
        newroot = new BTreeNode(_fh,  _type, _depth + 1);
        BTreeNode nbr(_fh, _rightID);
        newroot->_keys.resize(1, nbr.pushupFirstKey());
        newroot->_childs.resize(2);
        newroot->_childs[0] = _pgid;
        newroot->_childs[1] = nbr._pgid;
        setRootPageID(_fh, newroot->_pgid);
    }
    newroot->dump();
    return 0;
}

void LeafNode::Delete(AttrValue av, RID rid){
    int index = (int)(lower_bound(_keys.begin(), _keys.end(), av) - _keys.begin());
    for (; index < _keys.size() && _keys[index] == av; index++)
        if (_rids[index] == rid)
            break;
    assert(index < (int) _keys.size() &&
           _keys[index] == av && _rids[index] == rid);
    _keys.erase(_keys.begin() + index);
    _rids.erase(_rids.begin() + index);
}


void BTreeNode::Delete(AttrValue key, RID rid){
    //index points to less or equal key entry compare to key
    int index = (int)(upper_bound(_keys.begin(), _keys.end(), key) - _keys.begin()) - 1;
    assert(index + 1 < (int)_childs.size());
    int next = _childs[index+1];
    assert(next != EMPTY_NODE);
    if (_depth == 1){
        LeafNode leaf(_fh, next);
        leaf.Delete(key, rid);
    }
    else{
        BTreeNode bnode(_fh, next);
        bnode.Delete(key, rid);
    }
}


RC BTreeNode::rootDelete(const void *key, RID rid, BTreeNode * &newroot){
    AttrValue vkey;
    vkey.readFromData(_type, (char*)key);
    if (!find(vkey, rid))
        return -1;
    Delete(vkey, rid);
    newroot = this;
    newroot->dump();
    return 0;
}

RC BTreeNode::getNextEntry(IX_ScanIterator &ix){
    AttrValue last = ix._last_key, found;
    int depth = _depth, next = _pgid, index, begin;
    do{
        BTreeNode bnode(_fh, next);
        index = (int)(upper_bound(bnode._keys.begin(), bnode._keys.end(), last) - bnode._keys.begin());
        next = _childs[index];
        depth = bnode._depth;
    }while (depth > 1);

    while (next != EMPTY_NODE){
        LeafNode leaf(_fh, next);
        next = leaf._rightID;
        vector<AttrValue> keys;
        vector<RID> rids;
        leaf.get_whole_entries(keys, rids);
        bool found = false;
        begin = index = (int)(lower_bound(keys.begin(), keys.end(), last) - keys.begin());
        if (index >= (int) keys.size())
            continue;
        for (; index < (int)keys.size() && keys[index] == last; index++)
            if (rids[index] == ix._rid){
                found = true;
                break;
            }
        index = found ? index + 1 : begin;
        if (index >= (int) keys.size())
            continue;
        if (ix.isOK(keys[index])){
            ix._last_key = keys[index];
            ix._rid = rids[index];
            return 0;
        }
    }
    return -1;
}

void LeafNode::spill(AttrValue key, RID rid){
    if (_nextID == EMPTY_NODE){
        LeafNode over(_fh, _type);
        _nextID = over._pgid;
    }
    LeafNode over(_fh, _nextID);
    int space = (int) PAGE_SIZE - over.sizeOnDisk();
    if (space < key._len + (int) sizeof(RID))
        over.spill(key, rid);
    else{
        over._keys.push_back(key);
        over._rids.push_back(rid);
    }
}

void LeafNode::get_whole_entries(vector<AttrValue> &keys, vector<RID> &rids){
    keys = concate_vectors(_keys, keys);
    rids = concate_vectors(_rids, rids);
    if (_nextID != EMPTY_NODE){
        LeafNode over(_fh, _nextID);
        over.get_whole_entries(keys, rids);
    }
}

