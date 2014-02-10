
#include "ix.h"

IndexManager* IndexManager::_index_manager = 0;
static PagedFileManager *pfm;


IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();
    
    return _index_manager;
}

IndexManager::IndexManager()
{
    pfm = PagedFileManager::instance();
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

RC IndexManager::insertEntry(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    if (fileHandle.getNumberOfPages() == 0)
        rootsMap[fileHandle._fh_name] =
        new BTreeNode(fileHandle, attribute.type, 1, EMPTY_NODE);
	return -1;
}

RC IndexManager::deleteEntry(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	return -1;
}

RC IndexManager::scan(FileHandle &fileHandle,
                      const Attribute &attribute,
                      const void      *lowKey,
                      const void      *highKey,
                      bool			lowKeyInclusive,
                      bool        	highKeyInclusive,
                      IX_ScanIterator &ix_ScanIterator)
{
	return -1;
}

IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
	return -1;
}

RC IX_ScanIterator::close()
{
	return -1;
}

void IX_PrintError (RC rc)
{
}
