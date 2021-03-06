#ifndef _pfm_h_
#define _pfm_h_

typedef int RC;
typedef unsigned PageNum;

#include <iostream>
#include <fstream>
#include <map>
#include <string>
#include <cstdio>
using namespace std;

#define PAGE_SIZE 4096
//#define PFM (PagedFileManager::instance())



class FileHandle;



class PagedFileManager
{
    friend class FileHandle;
public:
    static PagedFileManager* instance();                     // Access to the _pf_manager instance

    RC createFile    (const char *fileName);                         // Create a new file
    RC destroyFile   (const char *fileName);                         // Destroy a file
    RC openFile      (const char *fileName, FileHandle &fileHandle); // Open a file
    RC closeFile     (FileHandle &fileHandle);                       // Close a file

private:
	bool isFileExit(const char *filename);
    fstream *get_fstream(string name);

protected:
    PagedFileManager();                                   // Constructor
    ~PagedFileManager();                                  // Destructor

private:
    static PagedFileManager *_pf_manager;
	map<string, int> _pf_open_count; //record the open count of certain file
    map<string, fstream *> _pf_open_files; //record the open count of certain file
};


class FileHandle
{
public:
    FileHandle();                                                    // Default constructor
    ~FileHandle();                                                   // Destructor

    RC readPage(PageNum pageNum, void *data);                           // Get a specific page
    RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
    RC appendPage(const void *data);                                    // Append a specific page
    unsigned getNumberOfPages();                                        // Get the number of pages in the file
    bool isAttached();

	//associated file
	string _fh_name;

private:

};

#endif
