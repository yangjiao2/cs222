/*
 TODO:
 X  1) ROOT_EMPTY, left and right node should be connected. other connected is achieved during split
 ANS: no need for special case, intialize root to empty _keys, 1 _childs. it would split automatically
 
 X 2) BootNode Split: should reassign parent id to new splitted child, could factor into a method
 3) For Debug: output and check consistency: parent_pgid == page[child[i]]'s parent id, print out
 X 4) rootInsert(): should return failure, if duplicates
 
 5) Delete(): ... trikcy, if merge, parent node design merge-with-right or merge-with left.
 */


#include "btree.h"

#define UNIT_SIZE (sizeof(int))

static PagedFileManager *pfm = PagedFileManager::instance();


BTreeNode::~BTreeNode(){
    dump();
    pfm->closeFile(_fh);
}

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


//TODO: change to binary search later
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


LeafNode::~LeafNode(){
    pfm->closeFile(_fh);
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


int LeafNode::sizeOnDisk(){
    int size = UNIT_SIZE * 5;
    for (int i = 0; i < (int) _keys.size(); i++) {
        size += _keys[i]._len;
        size += UNIT_SIZE * 2;
    }
    return size;
}

bool LeafNode::shouldMerge(){
    return sizeOnDisk() < PAGE_SIZE / 2;
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
    return true;
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
bool LeafNode::insert(AttrValue key, RID rid){
    cout<<"leaf node on page "<<_pgid<<endl;
    vector<AttrValue>::iterator ind = lower_bound(_keys.begin(), _keys.end(), key);
    assert(ind == _keys.end() || *ind != key); //assume no same key multiple entries case.
    int i = (int)(ind - _keys.begin());
    _keys.insert(_keys.begin() + i, key);
    _rids.insert(_rids.begin() + i, rid);
    bool splitted = shouldSplit();
    if (splitted){
        cout<<"leaf splitted"<<endl;
        split();
    }
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


void BTreeNode::claimChilds(){
    int N = (int) _childs.size();
    char buffer[PAGE_SIZE];
    for (int i = 0; i < N; i++)
        if (_childs[i] != EMPTY_NODE){
            _fh.readPage(_childs[i], buffer);
            memcpy(buffer + PAGE_SIZE - 4 * UNIT_SIZE, &_pgid, UNIT_SIZE); //offset works no matter LeafNode or BTreeNode
            _fh.writePage(_childs[i], buffer);
        }
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
    nbr.claimChilds();
    nbr.dump();
}

bool BTreeNode::insert(AttrValue key, RID rid){
    //Root Node first time insert: no need for special case, leave _keys empty, 1 entry of _childs.
    //it would split automatically after only child spills.
    
    //index: next level's _childs[index] goes down to
    int index = (int)(upper_bound(_keys.begin(), _keys.end(), key) - _keys.begin());
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
        newroot = new BTreeNode(_fh,  _type, _depth + 1, EMPTY_NODE);
        BTreeNode nbr(_fh, _rightID);
        newroot->_keys.resize(1, nbr.pushupFirstKey());
        newroot->_childs.resize(2);
        newroot->_childs[0] = _pgid;
        newroot->_childs[1] = nbr._pgid;
        newroot->claimChilds();
        newroot->dump();
    }
    return 0;
}

bool LeafNode::isSibling(int pgid, int &size){
    if (pgid == EMPTY_NODE)
        return false;
    LeafNode sib(_fh, pgid);
    size = sib.sizeOnDisk();
    return sib._parentID == _parentID;
}


template <typename T>
vector<T> concate_vectors(vector<T> &x, vector<T> &y) {
    vector<T> z(x.size() + y.size());
    z.insert(z.end(), x.begin(), x.end());
    z.insert(z.end(), y.begin(), y.end());
    return z;
}

//par_key pionts to current leafNode's corresponding parent's key entry
bool LeafNode::redistribute(vector<AttrValue>::iterator par_key){
    int sib_size = 0, sib_id = EMPTY_NODE, size = sizeOnDisk();
    if (isSibling(_leftID, sib_size) && size + sib_size > PAGE_SIZE)
        sib_id = _leftID;
    else if (isSibling(_rightID, sib_size) && size + sib_size > PAGE_SIZE)
        sib_id = _rightID;
    if (sib_id == EMPTY_NODE)
        return false;
    
    LeafNode sib(_fh, sib_id);
    if (sib_id == _rightID){
        _keys.push_back(sib._keys[0]);
        _rids.push_back(sib._rids[0]);
        sib._keys.erase(sib._keys.begin());
        sib._rids.erase(sib._rids.begin());
        *(par_key+1) = sib._keys[0];
    }
    else{
        int index = (int)sib._keys.size() - 1;
        _keys.insert(_keys.begin(), sib._keys[index]);
        _rids.insert(_rids.begin(), sib._rids[index]);
        sib._keys.erase(sib._keys.begin() + index);
        sib._rids.erase(sib._rids.begin() + index);
        *par_key = _keys[0];
    }
    return true;
}


//merge into current_child
bool LeafNode::merge(){
    int sib_size = 0, sib_id = EMPTY_NODE, size = sizeOnDisk();
    if (isSibling(_leftID, sib_size) && sib_size + size <= PAGE_SIZE)
        sib_id = _leftID;
    else if (isSibling(_rightID, sib_size) && sib_size + size <= PAGE_SIZE)
        sib_id = _rightID;
    if (sib_id == EMPTY_NODE)
        return false;
    LeafNode sib(_fh, sib_id);
    if (sib_id == _rightID){
        _keys = concate_vectors<AttrValue>(_keys, sib._keys);
        _rids = concate_vectors<RID>(_rids, sib._rids);
    }
    else{
        _keys = concate_vectors<AttrValue>(sib._keys, _keys);
        _rids = concate_vectors<RID>(sib._rids, _rids);
    }
    return true;
}


bool LeafNode::Delete(AttrValue av, RID rid, vector<AttrValue>::iterator pkey){
    int index = (int)(lower_bound(_keys.begin(), _keys.end(), av) - _keys.begin());
    assert(_keys[index] == av);
    _keys.erase(_keys.begin() + index);
    _rids.erase(_rids.begin() + index);
    if (!shouldMerge())
        return false;
    if (redistribute(pkey))
        return false;
    if (merge())
        return true;
    //unable to redistribute or merge, just return, may happen due to lack of entries
    return false;
}

bool BTreeNode::isSibling(int pgid, int &sib_size){
    if (pgid == EMPTY_NODE)
        return false;
    BTreeNode bnode(_fh, pgid);
    sib_size = bnode.sizeOnDisk();
    return bnode._parentID == _parentID;
}


bool BTreeNode::redistribute(vector<AttrValue>::iterator par_key){
    int sib_size = 0, sib_id = EMPTY_NODE, size = sizeOnDisk();
    if (isSibling(_leftID, sib_size) && size + sib_size > PAGE_SIZE)
        sib_id = _leftID;
    else if (isSibling(_rightID, sib_size) && size + sib_size > PAGE_SIZE)
        sib_id = _rightID;
    if (sib_id == EMPTY_NODE)
        return false;
    BTreeNode bnode(_fh, sib_id);
    if (sib_id == _leftID){
        _keys.insert(_keys.begin(), *par_key);
        _childs.insert(_childs.begin(), bnode._childs.back());
        *par_key = bnode._keys.back();
        bnode._childs.pop_back();
        bnode._keys.pop_back();
    }
    else{
        _keys.push_back(*(par_key + 1));
        _childs.push_back(bnode._childs[0]);
        *(par_key + 1) = bnode._keys[0];
        bnode._childs.pop_back();
        bnode._keys.pop_back();
    }
    return true;
}


//merge into current node
bool BTreeNode::merge(vector<AttrValue>::iterator par_key){
    int sib_size = 0, sib_id = EMPTY_NODE, size = sizeOnDisk();
    if (isSibling(_leftID, sib_size) && size + sib_size <= PAGE_SIZE)
        sib_id = _leftID;
    else if (isSibling(_rightID, sib_size) && size + sib_size <= PAGE_SIZE)
        sib_id = _rightID;
    if (sib_id == EMPTY_NODE)
        return false;
    BTreeNode bnode(_fh, sib_id);
    if (sib_id == _leftID){
        _keys.insert(_keys.begin(), *par_key);
        _keys = concate_vectors<AttrValue>(bnode._keys, _keys);
        _childs = concate_vectors<int>(bnode._childs, _childs);
    }
    else{
        _keys.insert(_keys.begin(), *(par_key+1));
        bnode._keys = concate_vectors<AttrValue>(_keys, bnode._keys);
        bnode._childs = concate_vectors<int>(_childs, bnode._childs);
    }
    return true;
}

bool BTreeNode::Delete(AttrValue key, RID rid,vector<AttrValue>::iterator par_key){
    int index = (int)(lower_bound(_keys.begin(), _keys.end(), key) - _keys.begin());
    assert(index < (int)_keys.size());
    int &next = _childs[index + 1];
    assert(next != EMPTY_NODE);
    bool merge_with_right = false;
    if (_depth == 1){
        LeafNode leaf(_fh, next);
        if (leaf.Delete(key, rid, _keys.begin() + index))
            merge_with_right = (_keys[index] <= leaf._keys[0]);
    }
    else{
        BTreeNode bnode(_fh, next);
        if (bnode.Delete(key, rid, _keys.begin() + index))
            merge_with_right = _keys[index] <= bnode._keys[0];
    }
    if (merge_with_right){
        _keys.erase(_keys.begin() + index + 1);
        _childs.erase(_childs.begin() + index + 2);
    }
    else{
        //remove previous _childs pointer, make current child pinter comes into effect
        _childs.erase(_childs.begin() + index);
        _keys.erase(_keys.begin() + index);
    }
    if (!shouldMerge())
        return false;
    if (redistribute(par_key))
        return false;
    if (merge(par_key))
        return true;
    return false;
}


RC BTreeNode::rootDelete(const void *key, RID rid, BTreeNode * &newroot){
    AttrValue vkey;
    vkey.readFromData(_type, (char*)key);
    if (!find(vkey, rid))
        return -1;
    //modify newroot: only more than one layer, and key is empty
    int index = (int)(lower_bound(_keys.begin(), _keys.end(), vkey) - _keys.begin());
    int next = _childs[index + 1];
    if (_depth > 1 && _keys.empty())
        newroot = new BTreeNode(_fh, next);
    else
        newroot = this;
    return 0;
}

