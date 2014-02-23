
#include "ix.h"

#define UNIT_SIZE (sizeof(int))

IndexManager* IndexManager::_index_manager = 0;
//static PagedFileManager *pfm;

#define pfm (PagedFileManager::instance())
#define idm (IndexManager::instance())


IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();
    
    return _index_manager;
}

IndexManager::IndexManager()
{
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName)
{
    return pfm->createFile(fileName.c_str());
}

RC IndexManager::destroyFile(const string &fileName)
{
    if (rootsMap.find(fileName) != rootsMap.end()){
        BTreeNode *root = rootsMap[fileName];
        rootsMap.erase(fileName);
        delete root;
    }
    return pfm->destroyFile(fileName.c_str());
}

RC IndexManager::openFile(const string &fileName, FileHandle &fileHandle)
{
    return pfm->openFile(fileName.c_str(), fileHandle);
}

RC IndexManager::closeFile(FileHandle &fileHandle)
{
    return pfm->closeFile(fileHandle);
}


RC IndexManager::checkRootMap(FileHandle &fh, const Attribute &attribute){
    char buffer[PAGE_SIZE];
    memset(buffer, 0, sizeof(buffer));
    string fname = fh._fh_name;
    int rootID = 1;
    assert(fname != "");
    
    if (rootsMap.find(fname) == rootsMap.end()){
        if (fh.getNumberOfPages() != 0){
            fh.readPage(0, buffer);
            memcpy(&rootID, buffer, UNIT_SIZE);
            rootsMap[fname] = new BTreeNode(fh, rootID);
        }
        //intialize: page0 root pointer, page 1 root page.
        else{
            memcpy(buffer, &rootID, UNIT_SIZE);
            fh.appendPage(buffer);
            rootsMap[fname] = new BTreeNode(fh, attribute.type, rootID, EMPTY_NODE);
        }
    }
    return rootsMap[fname]->getType() == attribute.type ? 0 : -1;
}

//TODO: return false if insert fail, could be duplicates: same key, multiple rids
//only the first time insert, we know what type of BTreeNode, ft...

RC IndexManager::insertEntry(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    if (checkRootMap(fileHandle, attribute) != 0)
        return -1;
    BTreeNode *root = rootsMap[fileHandle._fh_name], *newroot;
    if (root->rootInsert(key, rid, newroot) != 0)
        return -1;
    if (root != newroot){
        rootsMap[fileHandle._fh_name] = newroot;
        delete root; //easy to crash, i guess :-(
    }
    return 0;
}

RC IndexManager::deleteEntry(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    string fn = fileHandle._fh_name;
    if (rootsMap.find(fn) == rootsMap.end())
        return -1;
    BTreeNode *root = rootsMap[fn], *newroot;
    if (root->rootDelete(key, rid, newroot) != 0)
        return -1;
    if (newroot != root){
        rootsMap[fn] = newroot;
        delete root;
    }
    return 0;
}

RC IndexManager::scan(FileHandle &fileHandle,
                      const Attribute &attribute,
                      const void      *lowKey,
                      const void      *highKey,
                      bool			lowKeyInclusive,
                      bool        	highKeyInclusive,
                      IX_ScanIterator &ix_ScanIterator){
    if (lowKey)
        ix_ScanIterator._low.readFromData(attribute.type, (char *)lowKey);
    if (highKey)
        ix_ScanIterator._high.readFromData(attribute.type, (char *)highKey);
    ix_ScanIterator._attr = attribute;
    ix_ScanIterator._inc_low = lowKeyInclusive;
    ix_ScanIterator._inc_high = highKeyInclusive;
    if (pfm->openFile(fileHandle._fh_name.c_str(), ix_ScanIterator._fh) != 0)
        return -1;
    ix_ScanIterator._last_key = ix_ScanIterator._low;
    ix_ScanIterator._rid.pageNum = ix_ScanIterator._rid.slotNum = 0;
    return 0;
}

RC IndexManager::getNextEntry(IX_ScanIterator &ix, void *key, RID &rid){
    if (checkRootMap(ix._fh, ix._attr) == -1)
        return -1;
    string fn = ix._fh._fh_name;
    if (rootsMap[fn]->getNextEntry(ix) == -1)
        return -1;
    rid = ix._rid;
    ix._last_key.writeToData((char *)key);
    return 0;
}


IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    return idm->getNextEntry(*this, key, rid);
}

RC IX_ScanIterator::close()
{
    idm->closeFile(_fh);
    return 0;
}

void IX_PrintError (RC rc)
{
}
