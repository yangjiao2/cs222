#include "pfm.h"

PagedFileManager* PagedFileManager::_pf_manager = 0;

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

//TODO: close all open files, no fstream pointer
PagedFileManager::~PagedFileManager()
{
}


//the allocation of head page should be business of PageManager
RC PagedFileManager::createFile(const char *fileName)
{
	if (isFileExit(fileName))
		return -1;
	fstream f;
	f.open(fileName);
	if (!f.good())
		return -1;
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
	if (fileHandle._fh_file != NULL)
		return -1;
	fstream f(fileName);
	fileHandle._fh_file = &f;
	fileHandle._fh_name = fileName;
	string fn(fileName);
	if (_pf_open_count.find(fn) == _pf_open_count.end())
		_pf_open_count[fn] = 1;
	else
		_pf_open_count[fn]++;
	return 0;
}


//only close file when open count of file decreases to 0
RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
	fstream *fs = fileHandle._fh_file;
	string fn(fileHandle._fh_name);
	fs->flush();
	_pf_open_count[fn]--;
	if (_pf_open_count[fn] == 0){
		fs->close();
		_pf_open_count.erase(fn);
	}
	fileHandle._fh_file = NULL;
	fileHandle._fh_name = "";
	return 0;
}


FileHandle::FileHandle()
{
	_fh_name = "";
	_fh_file = NULL;
}


FileHandle::~FileHandle()
{
	if (_fh_file){
		PagedFileManager *pf = PagedFileManager::instance();
		pf->closeFile(*this);
	}
}


//pagenum start from 0
RC FileHandle::readPage(PageNum pageNum, void *data)
{
	_fh_file->seekg(pageNum * PAGE_SIZE);
	_fh_file->read((char *)data, PAGE_SIZE);
	return -1;
}

RC FileHandle::writePage(PageNum pageNum, const void *data)
{
	_fh_file->seekp(pageNum * PAGE_SIZE);
	_fh_file->write((char *)data, PAGE_SIZE);
	return -1;
}


RC FileHandle::appendPage(const void *data)
{
	_fh_file->seekp(ios::end);
	_fh_file->write((char *)data, PAGE_SIZE);
	return -1;
}


unsigned FileHandle::getNumberOfPages()
{
	_fh_file->seekg(ios::end);
	return _fh_file->tellg() / PAGE_SIZE;
}


