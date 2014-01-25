
#include "rm.h"
#include <cassert>

const string catalog_attr_name[] = {"tableID", "tableName", "fileName"};
const AttrType catalog_attr_type[] = {TypeInt, TypeVarChar, TypeVarChar};
const int catalog_attr_len[] = {4, 30, 30};

const string attrs_attr_name[] = {"tableID", "colum_name", "colum_type", "colum_len"};
const AttrType attrs_attr_type[] = {TypeInt, TypeVarChar, TypeInt, TypeInt};
const int attrs_attr_len[] = {4, 30, 4, 4};

RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();
    
    return _rm;
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
    
    memcpy(data, &column_type, sizeof(int));
    offset += sizeof(int);
    
    memcpy(data, &column_len, sizeof(int));
    offset += sizeof(int);
    
    return 0;
}

RelationManager::RelationManager()
{
    rbfm = RecordBasedFileManager::instance();
    _new_tbid = 0;
    
    char data[PAGE_SIZE];
    int nofiles = 0;
    RID rid;
    
    if (-1 == rbfm->openFile(RM_CATALOG_NAME, catalog_fh)){
        rbfm->createFile(RM_CATALOG_NAME);
        rbfm->openFile(RM_CATALOG_NAME, catalog_fh);
        tbname_to_id[RM_CATALOG_NAME] = _new_tbid++;
        nofiles++;
    }
    if (-1 == rbfm->openFile(RM_ATTRIBUTES_NAME, attr_fh)){
        rbfm->createFile(RM_ATTRIBUTES_NAME);
        rbfm->openFile(RM_ATTRIBUTES_NAME, attr_fh);
        tbname_to_id[RM_ATTRIBUTES_NAME] = _new_tbid++;
        nofiles++;
    }
    assert(nofiles == 0 || nofiles == 2);
    if (nofiles == 2){
        createTable(RM_CATALOG_NAME, prepareCatalogDescriptor());
        createTable(RM_ATTRIBUTES_NAME, prepareAttrDescriptor());
    }
    //retrieve all meta info for all table, fill in map data structre
    //scan the whole file, set _new_tbid
}

RelationManager::~RelationManager()
{
}


RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    char data[PAGE_SIZE];
    string tb_fn = RM_TABLE_FILENAME(tableName);
    vector<Attribute> descriptor = prepareCatalogDescriptor();
    RID rid;
    if (tableName != RM_ATTRIBUTES_NAME && tableName != RM_CATALOG_NAME) {
        rbfm->createFile(tb_fn);
    }
    rbfm->insertRecord(catalog_fh, descriptor, data, rid);
    descriptor = prepareAttrDescriptor();
    for (int i = 0; i < attrs.size(); i++) {
        prepareAttrData(tableName, attrs[i], data);
        rbfm->insertRecord(attr_fh, descriptor, data, rid);
    }
    return 0;
}

RC RelationManager::deleteTable(const string &tableName)
{
    return -1;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    return -1;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    return -1;
}

RC RelationManager::deleteTuples(const string &tableName)
{
    return -1;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    return -1;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    return -1;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    return -1;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    return -1;
}

RC RelationManager::reorganizePage(const string &tableName, const unsigned pageNumber)
{
    return -1;
}

RC RelationManager::scan(const string &tableName,
                         const string &conditionAttribute,
                         const CompOp compOp,
                         const void *value,
                         const vector<string> &attributeNames,
                         RM_ScanIterator &rm_ScanIterator)
{
    return -1;
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
