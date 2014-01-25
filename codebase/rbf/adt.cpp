#include "adt.h"
#include "rbfm.h"
#include <cassert>

#define UNIT_SIZE (sizeof(int))


int PageDirectory::MaximunEntryNum(void){
    return (PAGE_SIZE - UNIT_SIZE * 2) / (UNIT_SIZE * 2) - 1;
}

int PageDirectory::set_pgfree_by_id(int pageID, int newlen){
    int pgnum = get_pgnum();
    for (int i = 0; i < pgnum; i++)
        if (get_pgid(i) == pageID){
            set_pgfreelen(i, newlen);
            return 0;
        }
    return -1;
}

int PageDirectory::moveToNext(FileHandle &fh){
    assert(get_next() != 0);
    assert(get_pgnum() == PageDirectory::MaximunEntryNum());
    fh.writePage(in_the_page, data);
    in_the_page = get_next();
    fh.readPage(in_the_page, data);
    return in_the_page;
}

int PageDirectory::firstPageWithFreelen(FileHandle& fh, int len){
    while(1){
        int num = get_pgnum();
        for (int i = 0; i < num; i++)
            if (get_pgfreelen(i) > len)
                return get_pgid(i);
        if(get_next() == 0)
            return -1;
        moveToNext(fh);
    }
    return -1;
}

int PageDirectory::nextRecordPageID(FileHandle &fh, int pageid){
    int pgentry = -1;
    if (pageid == 0)
        while (get_next() != 0) {
            moveToNext(fh);
            if (get_pgnum() != 0)
                return get_pgid(0);
        }
    //locate
    while (1) {
        for (int i = 0; i < get_pgnum(); i++)
            if (get_pgid(i) == pageid) {
                pgentry = i;
                break;
            }
        if (pgentry != -1)
            break;
        if (get_next() == 0)
            return -1;
        moveToNext(fh);
    }
    if (pgentry < get_pgnum() - 1)
        return get_pgid(pgentry + 1);
    while (get_next() != 0) {
        moveToNext(fh);
        if (get_pgnum() != 0)
            return get_pgid(0);
    }
    return -1;
}

//TODO: refactoring
int PageDirectory::nextRecord(FileHandle &fh, RID &rid){
    char buffer[PAGE_SIZE];
    int pgid = rid.pageNum, slotid = -1;
    RecordPage rp;
    if (rid.pageNum != 0) {
        fh.readPage(rid.pageNum, buffer);
        rp = RecordPage(buffer);
        if ((slotid = rp.nextRecord(rid.slotNum + 1)) != -1) {
            rid.slotNum = slotid;
            return 0;
        }
    }
    while (1) {
        if ((pgid = nextRecordPageID(fh, pgid)) == -1)
            return -1;
        fh.readPage(pgid, buffer);
        rp= RecordPage(buffer);
        if ((slotid = rp.nextRecord(0)) != -1) {
            rid.pageNum = pgid;
            rid.slotNum = slotid;
            return 0;
        }
    }
    return -1;
}

int PageDirectory::allocRecordPage(FileHandle &fh){
    int allocated = -1;
    while (get_pgnum() == PageDirectory::MaximunEntryNum() &&
           get_next() != 0)
        moveToNext(fh);
    
    char newrp[PAGE_SIZE], newpd_buffer[PAGE_SIZE];
    memset(newrp, 0, sizeof(newrp)); //all zeros is ok
    memset(newpd_buffer, 0, sizeof(newpd_buffer)); //all zeros is ok
    RecordPage rp(newrp);
    fh.appendPage(newrp);
    allocated = (int) (fh.getNumberOfPages() - 1);
    
    if (get_next() == 0 && get_pgnum() == PageDirectory::MaximunEntryNum()) {
        fh.appendPage(newpd_buffer);
        set_next(fh.getNumberOfPages() - 1);
        moveToNext(fh);
    }
    set_pgid(get_pgnum(), allocated);
    set_pgfreelen(get_pgnum(), rp.getFreelen());
    set_pgnum(get_pgnum() + 1);
    return allocated;
}





int RecordPage::getFreelen(void){
    return PAGE_SIZE - get_freeptr() - 2 * UNIT_SIZE * (get_rcdnum() + 1);
}


RecordHeader RecordPage::getRecordHeaderAt(int index){
    return RecordHeader(data + get_offset(index));
}

int RecordPage::nextRecord(int start_from_slot){
    for (int i = start_from_slot; i < get_rcdnum(); i++)
        if (get_offset(i) != -1)
            return i;
    return -1;
}



int RecordHeader::getRecordLength(vector<Attribute> descriptor){
    int len = UNIT_SIZE * (int)(1 + descriptor.size()) +
    RecordHeader::getRecordContentLength(descriptor);
    return len;
}

int RecordHeader::getRecordContentLength(vector<Attribute> descriptor){
    int len = 0;
    for (int i = 0; i < (int)descriptor.size(); i++){
        len += descriptor[i].length;
        if (descriptor[i].type == TypeVarChar)
            len += UNIT_SIZE;
    }
    return len;
}

//len = len(RecordHeader + RecordContent)
RecordHeader RecordPage::allocRecordHeader(int len, int& slotID){
    slotID = -1;
    if (getFreelen() < len){
        cout<<"not enough space for RecordHeader";
        return RecordHeader();
    }
    for (int i = 0; i < get_rcdnum(); i++)
        if (get_offset(i) == -1) {
            slotID = i + 1;
            break;
        }
    if (slotID == -1){
        set_rcdnum(get_rcdnum() + 1);
        slotID = get_rcdnum();
    }
    set_offset(slotID - 1, get_freeptr());
    set_rcdlen(slotID - 1, len);
    set_freeptr(get_freeptr() + len);
    return RecordHeader(data + get_offset(slotID - 1));
}


int RecordHeader::writeRecord(vector<Attribute> recordDescriptor, char *bytes){
    int fld = (int)recordDescriptor.size(), vlen = 0, offset = 0;
    set_fieldnum(fld);
    int base = UNIT_SIZE * (2 + fld);//fld + 1 offset in the header
    for (int i = 0; i < fld; i++){
        set_fieldOffset(i, offset + base);
        if (recordDescriptor[i].type == TypeVarChar) {
            memcpy(&vlen, data + offset, sizeof(int));
            offset += sizeof(int) + vlen;
        }
        else
            offset += recordDescriptor[i].length;
    }
    set_fieldOffset(fld, offset + base);//last offset points to the end of record
    memcpy(data + UNIT_SIZE * (2 + get_fieldnum()), bytes,
           RecordHeader::getRecordContentLength(recordDescriptor));
    return 0;
}


char *RecordHeader::getContentPointer(void){
    return data + UNIT_SIZE * (2 + get_fieldnum());
}

//index start from 0
char *RecordHeader::getAttributePointer(int index){
    return data + get_fieldOffset(index);
}


//finally finds memcpy...
int writeInt(char *data, int val){
    memcpy(data, &val, sizeof(val));
    return 0;
}

int readInt(char *data){
    int val;
    memcpy(&val, data, sizeof(int));
    return val;
}

int PageDirectory::get_pgnum(void){
    return readInt(data);
}

int PageDirectory::set_pgnum(int v){
    return writeInt(data, v);
}

int PageDirectory::get_next(void){
    return readInt(data + UNIT_SIZE);
}

int PageDirectory::set_next(int v){
    return writeInt(data + UNIT_SIZE, v);
}

int PageDirectory::get_pgid(int index){
    return readInt(data + UNIT_SIZE * (2 + 2 * index));
}

int PageDirectory::set_pgid(int index, int val){
    return writeInt(data + UNIT_SIZE * (2 + 2 * index), val);
}

int PageDirectory::get_pgfreelen(int index){
    return readInt(data + UNIT_SIZE * (3 + 2 * index));
}

int PageDirectory::set_pgfreelen(int index, int val){
    return writeInt(data + UNIT_SIZE * (3 + 2 *index), val);
}

int RecordPage::get_rcdnum(void){
    return readInt(data + PAGE_SIZE - UNIT_SIZE);
}

int RecordPage::set_rcdnum(int v){
    return writeInt(data + PAGE_SIZE - UNIT_SIZE, v);
}

int RecordPage::get_freeptr(void){
    return readInt(data + PAGE_SIZE - 2 * UNIT_SIZE);
}

int RecordPage::set_freeptr(int v){
    return writeInt(data + PAGE_SIZE - 2 * UNIT_SIZE, v);
}

//index also starts from 0, slotID = index + 1
int RecordPage::get_offset(int index){
    return readInt(data + PAGE_SIZE - UNIT_SIZE * (4 + 2 * index));
}
int RecordPage::set_offset(int index, int v){
    return writeInt(data + PAGE_SIZE - UNIT_SIZE * (4 + 2 * index), v);
}

int RecordPage::get_rcdlen(int index){
    return readInt(data + PAGE_SIZE - UNIT_SIZE * (3 + 2 * index));
}

int RecordPage::set_rcdlen(int index, int v){
    return writeInt(data + PAGE_SIZE - UNIT_SIZE * (3 + 2 * index),v);
}

int RecordHeader::get_fieldnum(void){
    return readInt(data);
}

int RecordHeader::set_fieldnum(int v){
    return writeInt(data,v);
}

//index starts from 0
int RecordHeader::get_fieldOffset(int index){
    return readInt(data + UNIT_SIZE * (1 + index));
}

int RecordHeader::set_fieldOffset(int index, int v){
    return writeInt(data + UNIT_SIZE * (1 + index), v);
}