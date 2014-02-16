#include "pfm.h"
#include <cassert>

PagedFileManager* PagedFileManager::_pf_manager = 0;

#define pfm (PagedFileManager::instance())

PagedFileManager* PagedFileManager::instance()
{
	if(!_pf_manager)
		_pf_manager = new PagedFileManager();
	return _pf_manager;
}


PagedFileManager::PagedFileManager()
{
}



bool PagedFileManager::isFileExit(const char *filename)
{
	fstream f(filename);
	if (f.good()){
		f.close();
		return true;
	}
	return false;
}

PagedFileManager::~PagedFileManager()
{
    for (map<string, fstream *>::iterator itr = _pf_open_files.begin();
         itr != _pf_open_files.end(); itr++)
        itr->second->close();
}


fstream *PagedFileManager::get_fstream(string name){
    assert(_pf_open_files.find(name) != _pf_open_files.end());
    return _pf_open_files[name];
}

//the allocation of head page should be business of PageManager
RC PagedFileManager::createFile(const char *fileName)
{
	if (isFileExit(fileName))
		return -1;
	fstream f(fileName, fstream::out);
	f.close();
	return 0;
}


RC PagedFileManager::destroyFile(const char *fileName)
{
	if (!isFileExit(fileName))
		return -1;
	int ret = remove(fileName);
	return ret;
}

RC PagedFileManager::openFile(const char *fileName, FileHandle &fileHandle)
{
	if (!isFileExit(fileName))
		return -1;
	if (fileHandle.isAttached())
		return -1;
	string fn(fileName);
	if (_pf_open_count.find(fn) == _pf_open_count.end()){
		_pf_open_count[fn] = 1;
        _pf_open_files[fn] = new fstream(fileName,fstream::in | fstream::out);
    }
	else
		_pf_open_count[fn]++;
	fileHandle._fh_name = fileName;
	return 0;
}


//only close file when open count of file decreases to 0
RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
    if (!fileHandle.isAttached())
        return -1;
	string fn(fileHandle._fh_name);
	_pf_open_count[fn]--;
	if (_pf_open_count[fn] == 0){
        _pf_open_files[fn]->close();
		_pf_open_count.erase(fn);
        _pf_open_files.erase(fn);
	}
	fileHandle._fh_name = "";
	return 0;
}


FileHandle::FileHandle()
{
	_fh_name = "";
}

bool FileHandle::isAttached(){
    return _fh_name != "";
}


FileHandle::~FileHandle()
{
	if (isAttached())
		pfm->closeFile(*this);
}


//pagenum start from 0
RC FileHandle::readPage(PageNum pageNum, void *data)
{
    fstream *fs = pfm->get_fstream(_fh_name);
	fs->seekg(pageNum * PAGE_SIZE);
	fs->read((char *)data, PAGE_SIZE);
	return 0;
}


//TODO: writePage should fail if pageNum not exists
RC FileHandle::writePage(PageNum pageNum, const void *data)
{
    fstream *fs = pfm->get_fstream(_fh_name);
    assert(fs);
	fs->seekp(pageNum * PAGE_SIZE);
	fs->write((char *)data, PAGE_SIZE);
	return 0;
}

//seekp(fstream::end); should not use this!
//seekp(0, fstream::end); instread. otherwise would write 2 more bytes!
RC FileHandle::appendPage(const void *data)
{
	unsigned n = getNumberOfPages();
	writePage(n, data);
	return 0;
}


unsigned FileHandle::getNumberOfPages()
{
    fstream *fs = pfm->get_fstream(_fh_name);
	fs->seekg(0, fstream::end);
	return (unsigned)fs->tellg() / PAGE_SIZE;
}


