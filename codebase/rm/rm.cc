
#include "rm.h"
#include "../ix/ix.h"
#include <cassert>

char const *catalog_attr_name[] = {"tableID", "tableName", "fileName"};
const AttrType catalog_attr_type[] = {TypeInt, TypeVarChar, TypeVarChar};
const int catalog_attr_len[] = {4, 30, 30};

char const *attrs_attr_name[] = {"tableID", "colum_name", "colum_type", "colum_len"};
const AttrType attrs_attr_type[4] = {TypeInt, TypeVarChar, TypeInt, TypeInt};
const int attrs_attr_len[4] = {4, 30, 4, 4};


char const *index_attr_name[] = {"tableName", "attrName", "indexName"};
const AttrType index_attr_type[] = {TypeVarChar, TypeVarChar, TypeVarChar};
const int index_attr_len[] = {30,30,30};



#define TABLE_EXIST_CHECK(x) \
if (tbname_to_desp.find(x) == tbname_to_desp.end()) return -1;

#define ixm (IndexManager::instance())



RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();
    
    return _rm;
}

string RelationManager::tid_to_tbname(int tid){
    for (map<string, int>::iterator itr = tbname_to_id.begin() ;
         itr != tbname_to_id.end(); itr++)
        if (itr->second == tid)
            return itr->first;
    assert(0);
    return "";
}


//(tableID, tablename, fileName)
vector<Attribute> RelationManager::prepareCatalogDescriptor(void){
    vector<Attribute> dp;
    for (int i = 0; i < 3; i++)
        dp.push_back(Attribute(catalog_attr_name[i], catalog_attr_type[i],
                               catalog_attr_len[i]));
    return dp;
}

//(tableID, colum name, column type, column length)
vector<Attribute> RelationManager::prepareAttrDescriptor(void){
    vector<Attribute> dp;
    for (int i = 0; i < 4; i++)
        dp.push_back(Attribute(attrs_attr_name[i], attrs_attr_type[i],
                               attrs_attr_len[i]));
    return dp;
}

vector<Attribute> RelationManager::prepareIndexDescriptor(){
    vector<Attribute> dp;
    for (int i = 0; i < 3; i++)
        dp.push_back(Attribute(index_attr_name[i], index_attr_type[i],
                               index_attr_len[i]));
    return dp;
}

//caller is responsible for allocating tableid
int RelationManager::prepareCatalogData(string tableName, char *data){
    assert(tbname_to_id.find(tableName) != tbname_to_id.end());
    int id = tbname_to_id[tableName];
    string fileName = RM_TABLE_FILENAME(tableName);
    ValueStream vs(data);
    vs<<id<<tableName<<fileName;
    return 0;
}

int RelationManager::prepareAttrData(string tableName, Attribute attr, char *data){
    assert(tbname_to_id.find(tableName) != tbname_to_id.end());
    int id = tbname_to_id[tableName];
    string column_name = attr.name;
    int column_type = (int)attr.type;
    int column_len = attr.length;
    ValueStream vs(data);
    vs<<id<<column_name<<column_type<<column_len;
    return 0;
}

int RelationManager::prepareIndexData(string tablename, string attribute, char *data){
    TABLE_EXIST_CHECK(tablename);
    ValueStream vs(data);
    vs<<tablename<<attribute<<RM_INDEX_FILENAME(tablename, attribute);
    return 0;
}


//retrieve all meta info for all table, fill in map data structre
//scan the whole file, set _new_tbid
void RelationManager::RetrieveMetaInfo(){
    RBFM_ScanIterator rsi;
    RID rid;
    char data[PAGE_SIZE];
    vector<string> projected;
    projected.assign(catalog_attr_name, catalog_attr_name + 3);
    rbfm->scan(catalog_fh, prepareCatalogDescriptor(), "", NO_OP, NULL, projected, rsi);
    AttrValue av;
    while (rsi.getNextRecord(rid, data) != -1) {
        ValueStream vs(data);
        string tname;
        int tid;
        vs>>tid>>tname;
        tbname_to_id[tname] = tid;
        _new_tbid = max(_new_tbid, tid + 1);
    }
    rsi.close();
    
    //fill in tbname_to_desp from attribute table
    projected.assign(attrs_attr_name, attrs_attr_name + 4);
    rbfm->scan(attr_fh, prepareAttrDescriptor(), "", NO_OP, NULL, projected, rsi);
    while (rsi.getNextRecord(rid, data) != -1) {
        int tid, coltype, collen;
        string colname;
        ValueStream vs(data);
        vs>>tid>>colname>>coltype>>collen;
        string tname = tid_to_tbname(tid);
        Attribute attr(colname, (AttrType)coltype, (AttrLength)collen);
        vector<Attribute> desp;
        if (tbname_to_desp.find(tname) != tbname_to_desp.end())
            desp = tbname_to_desp[tname];
        desp.push_back(attr);
        tbname_to_desp[tname] = desp;
    }
    rsi.close();
    
    //fill in tbname_to_indices from index_table file
    projected.assign(index_attr_name, index_attr_name+3);
    rbfm->scan(index_fh, prepareIndexDescriptor(), "", NO_OP, NULL, projected, rsi);
    while (rsi.getNextRecord(rid, data) != -1) {
        int offset = 0;
        offset += av.readFromData(TypeVarChar, data);
        string tname = av._sv;
        offset += av.readFromData(TypeVarChar, data + offset);
        string attribute = av._sv;
        tbname_to_indices[tname].insert(attribute);
    }
    rsi.close();
}

RelationManager::RelationManager()
{
    rbfm = RecordBasedFileManager::instance();
    _new_tbid = 1;
    //only first time run db would call this, so no need to RetriveMetaInfo
    if (-1 == rbfm->openFile(RM_CATALOG_NAME, catalog_fh)){
        rbfm->createFile(RM_CATALOG_NAME);
        rbfm->createFile(RM_ATTRIBUTES_NAME);
        rbfm->createFile(RM_INDEX_NAME);
        
        rbfm->openFile(RM_CATALOG_NAME, catalog_fh);
        rbfm->openFile(RM_ATTRIBUTES_NAME, attr_fh);
        rbfm->openFile(RM_INDEX_NAME, index_fh);
        
        createTable(RM_CATALOG_NAME, prepareCatalogDescriptor());
        createTable(RM_ATTRIBUTES_NAME, prepareAttrDescriptor());
        createTable(RM_INDEX_NAME, prepareIndexDescriptor());
    }
    else{
        rbfm->openFile(RM_CATALOG_NAME, catalog_fh);
        rbfm->openFile(RM_ATTRIBUTES_NAME, attr_fh);
        rbfm->openFile(RM_INDEX_NAME, index_fh);
        RetrieveMetaInfo();
    }
}

RelationManager::~RelationManager()
{
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    if (tbname_to_desp.find(tableName) != tbname_to_desp.end())
        return -1;
    char data[PAGE_SIZE];
    string tb_fn = RM_TABLE_FILENAME(tableName);
    RID rid;
    if (tableName != RM_ATTRIBUTES_NAME && tableName != RM_CATALOG_NAME && tableName != RM_INDEX_NAME)
        rbfm->createFile(tb_fn);
    tbname_to_id[tableName] = _new_tbid++;
    tbname_to_desp[tableName] = attrs;
    prepareCatalogData(tableName, data);
    rbfm->insertRecord(catalog_fh, prepareCatalogDescriptor(), data, rid);
    for (int i = 0; i < attrs.size(); i++) {
        prepareAttrData(tableName, attrs[i], data);
        rbfm->insertRecord(attr_fh, prepareAttrDescriptor(), data, rid);//why using insertTuple would fail
    }
    return 0;
}


RC RelationManager::deleteTable(const string &tableName)
{
    TABLE_EXIST_CHECK(tableName);
    rbfm->destroyFile(RM_TABLE_FILENAME(tableName));
    RID rid;
    RM_ScanIterator rsi;
    char data[PAGE_SIZE], value[100];
    
    ValueStream(value)<<tableName;
    assert(scan(RM_CATALOG_NAME, "tableName", EQ_OP, value, vector<string>(), rsi) != -1);
    while (-1 != rsi.getNextTuple(rid, data))
        deleteTuple(RM_CATALOG_NAME, rid);
    rsi.close();
    
    assert(scan(RM_ATTRIBUTES_NAME, "tableID", EQ_OP, &tbname_to_id[tableName], vector<string>(), rsi) != -1);
    while (-1 != rsi.getNextTuple(rid, data))
        deleteTuple(RM_ATTRIBUTES_NAME, rid);
    rsi.close();
    
    //delete associate indices
    assert(scan(RM_INDEX_NAME, "tableName", EQ_OP, value, vector<string>(), rsi) != -1);
    while (-1 != rsi.getNextTuple(rid, data))
        deleteTuple(RM_INDEX_NAME, rid);
    rsi.close();

    tbname_to_id.erase(tableName);
    tbname_to_desp.erase(tableName);
    tbname_to_indices.erase(tableName);
    return 0;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    TABLE_EXIST_CHECK(tableName);
    attrs = tbname_to_desp[tableName];
    return 0;
}


Attribute RelationManager::getAttribute(const string tb, const string attrname){
    assert(tbname_to_desp.find(tb) != tbname_to_desp.end());
    vector<Attribute> &entry = tbname_to_desp[tb];
    for (vector<Attribute>::iterator itr = entry.begin(); itr != entry.end(); itr++)
        if (itr->name == attrname)
            return *itr;
    return Attribute();
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    TABLE_EXIST_CHECK(tableName);
    FileHandle fh;
    rbfm->openFile(RM_TABLE_FILENAME(tableName), fh);
    if (rbfm->insertRecord(fh, tbname_to_desp[tableName], data, rid) == -1)
        return -1;

    //coordinate indices insertion
    vector<Attribute> &desriptor = tbname_to_desp[tableName];
    ValueStream vs((char *)data);
    for (vector<Attribute>::iterator itr = desriptor.begin(); itr != desriptor.end(); itr++){
        if (containIndex(tableName, itr->name))
            insertIndexEntry(tableName, itr->name, vs.get_pointer(), rid);
        vs.read(*itr);
    }
    return 0;
}

RC RelationManager::deleteTuples(const string &tableName)
{
    TABLE_EXIST_CHECK(tableName);
    FileHandle fh;
    rbfm->openFile(RM_TABLE_FILENAME(tableName), fh);
    assert(rbfm->deleteRecords(fh) == 0);
    set<string> indices = tbname_to_indices[tableName];
    for (set<string>::iterator itr = indices.begin(); itr != indices.end(); itr++){
        assert(0 == destroyIndex(tableName, *itr));
        assert(0 == createIndex(tableName, *itr));
    }
    return 0;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    TABLE_EXIST_CHECK(tableName);
    FileHandle fh;
    char data[PAGE_SIZE];
    vector<Attribute> &descriptor = tbname_to_desp[tableName];
    if (0 == readTuple(tableName, rid, data)){
        ValueStream vs(data);
        for (vector<Attribute>::iterator itr = descriptor.begin();
             itr != descriptor.end(); itr++){
            if (containIndex(tableName, itr->name))
                deleteIndexEntry(tableName, itr->name, vs.get_pointer(), rid);
            vs.read(*itr);
        }
    }
    assert(rbfm->openFile(RM_TABLE_FILENAME(tableName), fh) == 0);
    if (rbfm->deleteRecord(fh, tbname_to_desp[tableName], rid) == -1)
        return -1;
    return 0;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    TABLE_EXIST_CHECK(tableName);
    FileHandle fh;
    rbfm->openFile(RM_TABLE_FILENAME(tableName), fh);
    return rbfm->updateRecord(fh, tbname_to_desp[tableName], data, rid);
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    TABLE_EXIST_CHECK(tableName);
    FileHandle fh;
    if (rbfm->openFile(RM_TABLE_FILENAME(tableName), fh) != 0)
        return -1;
    return rbfm->readRecord(fh, tbname_to_desp[tableName], rid, data);
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    TABLE_EXIST_CHECK(tableName);
    FileHandle fh;
    rbfm->openFile(RM_TABLE_FILENAME(tableName), fh);
    return rbfm->readAttribute(fh, tbname_to_desp[tableName], rid, attributeName, data);
}

RC RelationManager::reorganizePage(const string &tableName, const unsigned pageNumber)
{
    TABLE_EXIST_CHECK(tableName);
    FileHandle fh;
    rbfm->openFile(tableName, fh);
    return rbfm->reorganizePage(fh, tbname_to_desp[tableName], pageNumber);
}


RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
    return _rmsi.getNextRecord(rid, data);
}

RC RelationManager::scan(const string &tableName,
                         const string &conditionAttribute,
                         const CompOp compOp,
                         const void *value,
                         const vector<string> &attributeNames,
                         RM_ScanIterator &rm_ScanIterator)
{
    TABLE_EXIST_CHECK(tableName);
    FileHandle fh;
    rbfm->openFile(RM_TABLE_FILENAME(tableName), fh);
    return rbfm->scan(fh, tbname_to_desp[tableName], conditionAttribute, compOp, value,
                      attributeNames, rm_ScanIterator._rmsi);
}

// Extra credit
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
    return -1;
}

// Extra credit
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
    return -1;
}

// Extra credit
RC RelationManager::reorganizeTable(const string &tableName)
{
    return -1;
}


//========================
// Index Creation
//========================


bool RelationManager::containIndex(string tbn, string attr){
//    assert(tbname_to_indices.find(tbn) != tbname_to_indices.end());
    return tbname_to_indices[tbn].find(attr) != tbname_to_indices[tbn].end();
}

RC RelationManager::insertIndexEntry(string tbn, string attr, void *data, const RID &rid){
    assert(containIndex(tbn, attr));
    FileHandle ixfh;
    assert(rbfm->openFile(RM_INDEX_FILENAME(tbn, attr), ixfh) == 0);
    assert(ixm->insertEntry(ixfh, getAttribute(tbn, attr), data, rid) == 0);
    return 0;
}

RC RelationManager::deleteIndexEntry(string tbn, string attr, void *data, const RID &rid){
    assert(containIndex(tbn, attr));
    FileHandle ixfh;
    assert(rbfm->openFile(RM_INDEX_FILENAME(tbn, attr), ixfh) == 0);
    assert(ixm->deleteEntry(ixfh, getAttribute(tbn, attr), data, rid) == 0);
    return 0;
}




RC RelationManager::createIndex(const string &tableName, const string &attributeName){
    TABLE_EXIST_CHECK(tableName);
    set<string> &iset = tbname_to_indices[tableName];
    if (iset.find(attributeName) != iset.end())
        return -1;
    char data[PAGE_SIZE];
    RID rid;
    //insert into index_table
    prepareIndexData(tableName, attributeName, data);
    assert(insertTuple(RM_INDEX_NAME, data, rid) == 0);
    iset.insert(attributeName);
    
    string ixfn = RM_INDEX_FILENAME(tableName, attributeName);
    FileHandle ixfh;
    ixm->createFile(ixfn);
    ixm->openFile(ixfn, ixfh);
    RM_ScanIterator rmsi;
    assert(scan(tableName, "", NO_OP, NULL, vector<string>(1,attributeName), rmsi) == 0);
    while (rmsi.getNextTuple(rid, data) != -1)
        assert(ixm->insertEntry(ixfh, getAttribute(tableName, attributeName), data, rid) == 0);
    rmsi.close();
    return 0;
}

RC RelationManager::destroyIndex(const string &tableName, const string &attributeName){
    TABLE_EXIST_CHECK(tableName);
    if (!containIndex(tableName, attributeName))
        return -1;
    RID rid;
    tbname_to_indices[tableName].erase(attributeName);
    assert(0 == deleteTuple(RM_INDEX_NAME, rid));
    assert(0 == ixm->destroyFile(RM_INDEX_FILENAME(tableName, attributeName)));
    return 0;
}

// indexScan returns an iterator to allow the caller to go through qualified entries in index
RC RelationManager::indexScan(const string &tableName,
                              const string &attributeName,
                              const void *lowKey,
                              const void *highKey,
                              bool lowKeyInclusive,
                              bool highKeyInclusive,
                              RM_IndexScanIterator &rm_IndexScanIterator){
    TABLE_EXIST_CHECK(tableName);
    FileHandle fh;
    assert(rbfm->openFile(RM_INDEX_FILENAME(tableName, attributeName), fh) != -1);
    return ixm->scan(fh, getAttribute(tableName, attributeName), lowKey, highKey,
                     lowKeyInclusive, highKeyInclusive, rm_IndexScanIterator._ixsi);
}

RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key){
    return _ixsi.getNextEntry(rid, key);
}


