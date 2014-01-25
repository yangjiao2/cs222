
#include "rbfm.h"
#include "adt.h"
#include <cassert>


int AttrValue::readFromData(AttrType type, char *data){
    int len = 0;
    char s[PAGE_SIZE];
    _type = type;
    switch (_type) {
        case TypeVarChar:
            memcpy(&len, data, sizeof(int));
            memcpy(s, data + sizeof(int), len);
            s[len] = '\0';
            _sv = string(s);
            _len = sizeof(int) + len;
            break;
        case TypeInt:
            memcpy(&_iv, data, sizeof(int));
            _len = sizeof(int);
            break;
        case TypeReal:
            memcpy(&_fv, data, sizeof(float));
            _len = sizeof(float);
            break;
        default:
            break;
    }
    return _len;
}
int AttrValue::writeToData(char *data){
    switch (_type) {
        case TypeVarChar:
            memcpy(data, &_len, sizeof(int));
            memcpy(data + sizeof(int), _sv.c_str(), _sv.length());
            return 4;
        case TypeInt:
            memcpy(data, &_iv, 4);
            return 4;
        case TypeReal:
            memcpy(data, &_fv, 4);
            return 4;
        default:
            return 0;
    }
}

void AttrValue::printSelf(){
    switch (_type) {
        case TypeInt:
            cout<<"Int "<<_iv;
            break;
        case TypeReal:
            cout<<"Real "<<_fv;
            break;
        case TypeVarChar:
            cout<<"String "<<_sv;
        default:
            break;
    }
    cout<<" | "<<endl;
}

bool AttrValue::compareValue(AttrValue v, AttrValue cmp, CompOp op){
    assert(v._type == cmp._type);
    switch (op) {
        case EQ_OP:
            return v == cmp;
        case NE_OP:
            return v != cmp;
        case LT_OP:
            return v < cmp;
        case LE_OP:
            return v <= cmp;
        case GT_OP:
            return v > cmp;
        case GE_OP:
            return v >= cmp;
        default:
            return true;
    }
}



AttrType RBFM_ScanIterator::getAttrType(string name){
    for (int i = 0; i < _descriptor.size(); i++)
        if (_descriptor[i].name == name)
            return _descriptor[i].type;
    return TypeNone;
}

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

RC RecordBasedFileManager::scan(FileHandle &fileHandle,
        const vector<Attribute> &recordDescriptor,
        const string &conditionAttribute,
        const CompOp compOp,                  // comparision type such as "<" and "="
        const void *value,                    // used in the comparison
        const vector<string> &attributeNames, // a list of projected attributes
        RBFM_ScanIterator &rbfm_ScanIterator){
    rbfm_ScanIterator._condAttr = conditionAttribute;
    rbfm_ScanIterator._descriptor = recordDescriptor;
    rbfm_ScanIterator._opr = compOp;
    rbfm_ScanIterator._projected = attributeNames;
    rbfm_ScanIterator._value = (char *)value;
    pfm->openFile(fileHandle._fh_name.c_str(), rbfm_ScanIterator._fh);
    rbfm_ScanIterator._rid.pageNum = rbfm_ScanIterator._rid.slotNum = 0;
    return 0;
}

RC RecordBasedFileManager::getNextRecord(RBFM_ScanIterator &rs, void *data){
    FileHandle &fh = rs._fh;
    RID rid = rs._rid;
    int offset = 0;
    char buffer[PAGE_SIZE], val[PAGE_SIZE];
    fh.readPage(0, buffer);
    PageDirectory pdt(buffer);
    while (pdt.nextRecord(fh, rid) != -1) {
        if (readAttribute(fh, rs._descriptor, rid, rs._condAttr, val) == -1)
            return -1;
        AttrType type = rs.getAttrType(rs._condAttr);
        AttrValue rv, cv;
        rv.readFromData(type, val);
        cv.readFromData(type, rs._value);
        if (AttrValue::compareValue(rv, cv, rs._opr)){
            rs._rid = rid;
            for (int i = 0; i < rs._projected.size(); i++) {
                readAttribute(rs._fh, rs._descriptor, rid, rs._projected[i],
                              (char *)data + offset);
                offset += rv.readFromData(rs.getAttrType(rs._projected[i]), (char *)data + offset);
            }
            return  0; //retrieve projected
        }
    }
    return -1;
}


RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data){
    return RBFM_EOF;
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
    int offset = 0;
    AttrValue av;
    for (int i = 0; i < recordDescriptor.size(); i++) {
        av.readFromData(recordDescriptor[i].type, (char *)data + offset);
        offset += av._len;
        av.printSelf();
    }
    cout<<endl;
    return 0;
}


//delete all records, just close, destroy, create, reopen file.
RC RecordBasedFileManager::deleteRecords(FileHandle &fileHandle){
    closeFile(fileHandle);
    destroyFile(fileHandle._fh_name);
    createFile(fileHandle._fh_name);
    openFile(fileHandle._fh_name, fileHandle);
    return 0;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor,
                const RID &rid){
    char buffer[PAGE_SIZE];
    fileHandle.readPage(rid.pageNum, buffer);
    RecordPage rp(buffer);
    rp.set_offset(rid.slotNum - 1, -1);
    return 0;
}


// Assume the rid does not change after update
// TODO: if records is growing too long, should move it to other place, leave tombstone
// this would affect readRecord's code, keep forwarding until find a none tombstone.
RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor,
                const void *data, const RID &rid){
    char buffer[PAGE_SIZE];
    fileHandle.readPage(rid.pageNum, buffer);
    RecordHeader rh = RecordPage(buffer).getRecordHeaderAt(rid.slotNum - 1);
    rh.writeRecord(recordDescriptor, (char *)data);
    fileHandle.writePage(rid.pageNum, buffer);
    return 0;
}


RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor,
                 const RID &rid, const string attributeName, void *data){
    char buffer[PAGE_SIZE];
    readRecord(fileHandle, recordDescriptor, rid, buffer);
    int offset = 0;
    AttrValue av;
    for (int i = 0; i < recordDescriptor.size(); i++) {
        offset += av.readFromData(recordDescriptor[i].type, buffer + offset);
        if (recordDescriptor[i].name == attributeName) {
            av.writeToData((char *)data);
            return 0;
        }
    }
    return -1;
}

RC RecordBasedFileManager::reorganizePage(FileHandle &fileHandle, const vector<Attribute>
                                          &recordDescriptor, const unsigned pageNumber){
    char buffer[PAGE_SIZE];
    int offset = 0;
    fileHandle.readPage(pageNumber, buffer);
    RecordPage rp(buffer);
    int newFreePointer = 0;
    for (int i = 0; i < rp.get_rcdnum(); i++)
        if ((offset = rp.get_offset(i)) != -1){
            //safe to use memove, even may overlap
            memmove(rp.data + newFreePointer, rp.data + offset, rp.get_rcdlen(i));
            rp.set_offset(i, newFreePointer);
            newFreePointer += rp.get_rcdlen(i);
        }
    return 0;
}
