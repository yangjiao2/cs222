
#include "rm.h"
#include <cassert>

char const *catalog_attr_name[] = {"tableID", "tableName", "fileName"};
const AttrType catalog_attr_type[] = {TypeInt, TypeVarChar, TypeVarChar};
const int catalog_attr_len[] = {4, 30, 30};

char const *attrs_attr_name[] = {"tableID", "colum_name", "colum_type", "colum_len"};
const AttrType attrs_attr_type[4] = {TypeInt, TypeVarChar, TypeInt, TypeInt};
const int attrs_attr_len[4] = {4, 30, 4, 4};

#define TABLE_EXIST_CHECK(x) \
if (tbname_to_desp.find(x) == tbname_to_desp.end()) return -1;


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

//caller is responsible for allocating tableid
int RelationManager::prepareCatalogData(string tableName, char *data){
    assert(tbname_to_id.find(tableName) != tbname_to_id.end());
    int id = tbname_to_id[tableName], offset = 0, len = 0;
    string fileName = RM_TABLE_FILENAME(tableName);
    
    memcpy(data, &id, sizeof(int));
    offset += sizeof(int);
    
    len = (int)tableName.length();
    memcpy(data + offset, &len, sizeof(int));
    offset += sizeof(int);
    memcpy(data + offset, tableName.c_str(), len);
    offset += len;
    
    len = (int)fileName.length();
    memcpy(data + offset, &len, sizeof(int));
    offset += sizeof(int);
    memcpy(data + offset, fileName.c_str(), len);
    offset += len;
    
    return 0;
}

int RelationManager::prepareAttrData(string tableName, Attribute attr, char *data){
    assert(tbname_to_id.find(tableName) != tbname_to_id.end());
    int id = tbname_to_id[tableName];
    string column_name = attr.name;
    int column_type = (int)attr.type;
    int column_len = attr.length;
    int offset = 0, len = 0;
    
    memcpy(data, &id, sizeof(int));
    offset += sizeof(int);
    
    len = (int)column_name.length();
    memcpy(data + offset, &len, sizeof(int));
    offset += sizeof(int);
    memcpy(data + offset, column_name.c_str(), len);
    offset += len;

    
    memcpy(data + offset, &column_type, sizeof(int));
    offset += sizeof(int);
    
    memcpy(data + offset, &column_len, sizeof(int));
    offset += sizeof(int);
    
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
        int offset = 0;
        offset += av.readFromData(TypeInt, data);
        int tid = av._iv;
        offset += av.readFromData(TypeVarChar, data + offset);
        string tname = av._sv;
        tbname_to_id[tname] = tid;
        _new_tbid = max(_new_tbid, tid + 1);
    }
    rsi.close();
    projected.assign(attrs_attr_name, attrs_attr_name + 4);
    rbfm->scan(attr_fh, prepareAttrDescriptor(), "", NO_OP, NULL, projected, rsi);
    while (rsi.getNextRecord(rid, data) != -1) {
        int offset = 0;
        offset += av.readFromData(TypeInt, data + offset);
        int tid = av._iv;
        offset += av.readFromData(TypeVarChar, data + offset);
        string colname = av._sv;
        offset += av.readFromData(TypeInt, data + offset);
        int coltype = av._iv;
        offset += av.readFromData(TypeInt, data + offset);
        int collen = av._iv;
        string tname = tid_to_tbname(tid);
        if (tname == RM_CATALOG_NAME || tname == RM_ATTRIBUTES_NAME)
            continue;
        Attribute attr(colname, (AttrType)coltype, (AttrLength)collen);
        vector<Attribute> desp;
        if (tbname_to_desp.find(tname) != tbname_to_desp.end())
            desp = tbname_to_desp[tname];
        desp.push_back(attr);
        tbname_to_desp[tname] = desp;
    }
}

RelationManager::RelationManager()
{
    rbfm = RecordBasedFileManager::instance();
    _new_tbid = 1;
    int nofiles = 0;
    if (-1 == rbfm->openFile(RM_CATALOG_NAME, catalog_fh)){
        rbfm->createFile(RM_CATALOG_NAME);
        rbfm->openFile(RM_CATALOG_NAME, catalog_fh);
        nofiles++;
    }
    if (-1 == rbfm->openFile(RM_ATTRIBUTES_NAME, attr_fh)){
        rbfm->createFile(RM_ATTRIBUTES_NAME);
        rbfm->openFile(RM_ATTRIBUTES_NAME, attr_fh);
        nofiles++;
    }
    assert(nofiles == 0 || nofiles == 2);
    if (nofiles == 2){
        createTable(RM_CATALOG_NAME, prepareCatalogDescriptor());
        createTable(RM_ATTRIBUTES_NAME, prepareAttrDescriptor());
    }
    tbname_to_desp[RM_CATALOG_NAME] = prepareCatalogDescriptor();
    tbname_to_desp[RM_ATTRIBUTES_NAME] = prepareAttrDescriptor();
    RetrieveMetaInfo();
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
    if (tableName != RM_ATTRIBUTES_NAME && tableName != RM_CATALOG_NAME)
        rbfm->createFile(tb_fn);
    tbname_to_id[tableName] = _new_tbid++;
    tbname_to_desp[tableName] = attrs;
    prepareCatalogData(tableName, data);
    rbfm->insertRecord(catalog_fh, prepareCatalogDescriptor(), data, rid);
    for (int i = 0; i < attrs.size(); i++) {
        prepareAttrData(tableName, attrs[i], data);
        rbfm->insertRecord(attr_fh, prepareAttrDescriptor(), data, rid);
    }
    return 0;
}


//TODO: refectoring, using AttrValue to dump bytes
RC RelationManager::deleteTable(const string &tableName)
{
    TABLE_EXIST_CHECK(tableName);
    rbfm->destroyFile(RM_TABLE_FILENAME(tableName));
    RID rid;
    RM_ScanIterator rsi;
    char data[PAGE_SIZE], value[100];
    int len = (int)tableName.length();
    vector<string> attrNames(1);
    attrNames[0] = tableName;
    memcpy(value, &len, sizeof(int));
    memcpy(value + sizeof(int), tableName.c_str(), len);
    cout<<"scanned result is  "<<scan(RM_CATALOG_NAME, "tableName", EQ_OP, value, attrNames, rsi)<<endl;
    rsi.getNextTuple(rid, data);
    deleteTuple(RM_CATALOG_NAME, rid);
    attrNames[0] = "tableID";
    rsi.close();
    scan(RM_ATTRIBUTES_NAME, "tableID", EQ_OP, &tbname_to_id[tableName], attrNames, rsi);
    while (-1 != rsi.getNextTuple(rid, data))
        deleteTuple(RM_ATTRIBUTES_NAME, rid);
    tbname_to_id.erase(tableName);
    tbname_to_desp.erase(tableName);
    return 0;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    TABLE_EXIST_CHECK(tableName);
    attrs = tbname_to_desp[tableName];
    return 0;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    TABLE_EXIST_CHECK(tableName);
    FileHandle fh;
    rbfm->openFile(RM_TABLE_FILENAME(tableName), fh);
    return rbfm->insertRecord(fh, tbname_to_desp[tableName], data, rid);
}

RC RelationManager::deleteTuples(const string &tableName)
{
    if (tbname_to_desp.find(RM_CATALOG_NAME) == tbname_to_desp.end())
        cout<<"not find catalog in deleteTuples"<<endl;
    TABLE_EXIST_CHECK(tableName);
    FileHandle fh;
    rbfm->openFile(RM_TABLE_FILENAME(tableName), fh);
    return rbfm->deleteRecords(fh);
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    TABLE_EXIST_CHECK(tableName);
    FileHandle fh;
    rbfm->openFile(RM_TABLE_FILENAME(tableName), fh);
    return rbfm->deleteRecord(fh, tbname_to_desp[tableName], rid);
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
    rbfm->openFile(RM_TABLE_FILENAME(tableName), fh);
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
    if (tbname_to_desp.find(RM_CATALOG_NAME) == tbname_to_desp.end())
        cout<<"not find catalog in reorganizePage"<<endl;
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
