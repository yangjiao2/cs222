
#include "rbfm.h"
#include "adt.h"

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
	pfm = PagedFileManager::instance();
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}


RC RecordBasedFileManager::createFile(const string &fileName) {
    FileHandle fh;
    pfm->createFile(fileName.c_str());
    pfm->openFile(fileName.c_str(), fh);
    char pgd_buffer[PAGE_SIZE];
    memset(pgd_buffer, 0, sizeof(pgd_buffer));
    fh.writePage(0, pgd_buffer);
    return 0;
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
	return pfm->destroyFile(fileName.c_str());
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
	return pfm->openFile(fileName.c_str(), fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
	return pfm->closeFile(fileHandle);
}



//TODO: FreeLen还要改...写回directoryEntry, 所以根据FP原理, 应该让之独立开来.
//RecordPage, PageDirectory自己保存数据
RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle,
                                        const vector<Attribute> &recordDescriptor,
                                        const void *data, RID &rid) {
    int length = RecordHeader::getRecordLength(recordDescriptor);
    int pgid, slotID;
    char direct_buff[PAGE_SIZE], rp_buff[PAGE_SIZE];
    
    fileHandle.readPage(0, direct_buff);
    PageDirectory pdt(direct_buff);
    pgid = pdt.firstPageWithFreelen(fileHandle, length);
    if (pgid == -1)
        pgid = pdt.allocRecordPage(fileHandle);
    if (pgid == -1){
        cout<<"fail to allocate an availiable page"<<endl;
        return -1;
    }
    fileHandle.readPage(pgid, rp_buff);
    RecordPage rp(rp_buff);
    RecordHeader rh = rp.allocRecordHeader(length, slotID);

    
    rh.writeRecord(recordDescriptor, (char *)data);
    pdt.set_pgfree_by_id(pgid, rp.getFreelen());
    fileHandle.writePage(pgid, rp_buff);
    fileHandle.writePage(pdt.in_the_page, direct_buff);
    rid.pageNum = (unsigned int)pgid;
    rid.slotNum = (unsigned int)slotID;
    return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle,
                                      const vector<Attribute> &recordDescriptor,
                                      const RID &rid, void *data) {
    char rp_buffer[PAGE_SIZE], *content = NULL;
    fileHandle.readPage(rid.pageNum, rp_buffer);
    RecordPage rp(rp_buffer);
    RecordHeader rh = rp.getRecordHeaderAt(rid.slotNum - 1);
    content = rh.getContentPointer();
    memcpy(data, content, RecordHeader::getRecordContentLength(recordDescriptor));
    return 0;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor,
                                       const void *data) {
    int a, offset = 0;
    float b;
    char str[50];
    for (int i = 0; i < recordDescriptor.size(); i++) {
        switch (recordDescriptor[i].type) {
            //TypeVarChar: |length_of_string|string_content
            case TypeVarChar:
                memcpy(&a, (char *)data + offset, sizeof(int));
                offset += sizeof(int);
                memcpy(str, (char*)data + offset, a);
                str[a] = '\0';
                offset += a;
                cout<<string(str)<<" | ";
                break;
            case TypeInt:
                memcpy((char*)&a,(char *)data + offset, sizeof(int));
                offset += sizeof(int);
                cout<<"Int: "<<a<<" | ";
                break;
            case TypeReal:
                memcpy((char*)&b ,(char *)data + offset, sizeof(float));
                offset += sizeof(float);
                cout<<"Real: "<<b<<" | ";
                break;
        }
    }
    cout<<endl;
    return 0;
}
