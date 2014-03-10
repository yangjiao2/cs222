#ifndef _qe_h_
#define _qe_h_

#include <vector>

#include "../rbf/rbfm.h"
#include "../rm/rm.h"
#include "../ix/ix.h"

# define QE_EOF (-1)  // end of the index scan

using namespace std;

typedef enum{ MIN = 0, MAX, SUM, AVG, COUNT } AggregateOp;


// The following functions use  the following
// format for the passed data.
//    For int and real: use 4 bytes
//    For varchar: use 4 bytes for the length followed by
//                          the characters

struct Value {
    AttrType type;          // type of value
    void     *data;         // value
};


struct Condition {
    string lhsAttr;         // left-hand side attribute
    CompOp  op;             // comparison operator
    bool    bRhsIsAttr;     // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
    string rhsAttr;         // right-hand side attribute if bRhsIsAttr = TRUE
    Value   rhsValue;       // right-hand side value if bRhsIsAttr = FALSE
};


class Iterator {
    // All the relational operators and access methods are iterators.
public:
    virtual RC getNextTuple(void *data) = 0;
    //all getAttributes() returning attrs's name containg relation.attr
    virtual void getAttributes(vector<Attribute> &attrs) const = 0;
    virtual ~Iterator() {};
protected:
    string get_tbname();
};


class TableScan : public Iterator
{
    // A wrapper inheriting Iterator over RM_ScanIterator
public:
    RelationManager &rm;
    RM_ScanIterator *iter;
    string tableName;
    vector<Attribute> attrs;
    vector<string> attrNames;
    RID rid;
    
    TableScan(RelationManager &rm, const string &tableName, const char *alias = NULL):rm(rm)
    {
        //Set members
        this->tableName = tableName;
        
        // Get Attributes from RM
        rm.getAttributes(tableName, attrs);
        
        // Get Attribute Names from RM
        unsigned i;
        for(i = 0; i < attrs.size(); ++i)
        {
            // convert to char *
            attrNames.push_back(attrs[i].name);
        }
        
        // Call rm scan to get iterator
        iter = new RM_ScanIterator();
        rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);
        
        // Set alias
        if(alias) this->tableName = alias;
    };
    
    // Start a new iterator given the new compOp and value
    void setIterator()
    {
        iter->close();
        delete iter;
        iter = new RM_ScanIterator();
        rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);
    };
    
    RC getNextTuple(void *data)
    {
        return iter->getNextTuple(rid, data);
    };
    
    void getAttributes(vector<Attribute> &attrs) const
    {
        attrs.clear();
        attrs = this->attrs;
        unsigned i;
        
        // For attribute in vector<Attribute>, name it as rel.attr
        for(i = 0; i < attrs.size(); ++i)
        {
            string tmp = tableName;
            tmp += ".";
            tmp += attrs[i].name;
            attrs[i].name = tmp;
        }
    };
    
    ~TableScan()
    {
        iter->close();
    };
};


class IndexScan : public Iterator
{
    // A wrapper inheriting Iterator over IX_IndexScan
public:
    RelationManager &rm;
    RM_IndexScanIterator *iter;
    string tableName;
    string attrName;
    vector<Attribute> attrs;
    char key[PAGE_SIZE];
    RID rid;
    
    IndexScan(RelationManager &rm, const string &tableName, const string &attrName, const char *alias = NULL):rm(rm)
    {
        // Set members
        this->tableName = tableName;
        this->attrName = attrName;
        
        
        // Get Attributes from RM
        rm.getAttributes(tableName, attrs);
        
        // Call rm indexScan to get iterator
        iter = new RM_IndexScanIterator();
        rm.indexScan(tableName, attrName, NULL, NULL, true, true, *iter);
        
        // Set alias
        if(alias) this->tableName = alias;
    };
    
    // Start a new iterator given the new key range
    void setIterator(void* lowKey,
                     void* highKey,
                     bool lowKeyInclusive,
                     bool highKeyInclusive)
    {
        iter->close();
        delete iter;
        iter = new RM_IndexScanIterator();
        rm.indexScan(tableName, attrName, lowKey, highKey, lowKeyInclusive,
                     highKeyInclusive, *iter);
    };
    
    RC getNextTuple(void *data)
    {
        int rc = iter->getNextEntry(rid, key);
        if(rc == 0)
        {
            rc = rm.readTuple(tableName.c_str(), rid, data);
        }
        return rc;
    };
    
    void getAttributes(vector<Attribute> &attrs) const
    {
        attrs.clear();
        attrs = this->attrs;
        unsigned i;
        
        // For attribute in vector<Attribute>, name it as rel.attr
        for(i = 0; i < attrs.size(); ++i)
        {
            string tmp = tableName;
            tmp += ".";
            tmp += attrs[i].name;
            attrs[i].name = tmp;
        }
    };
    
    ~IndexScan()
    {
        iter->close();
    };
};


class Filter : public Iterator {
    // Filter operator
public:
    Filter(Iterator *input,                         // Iterator of input R
           const Condition &condition               // Selection condition
    ) : _iterator(input), _cond(condition) {} ;
    ~Filter(){};
    
    RC getNextTuple(void *data);
    // For attribute in vector<Attribute>, name it as rel.attr
    void getAttributes(vector<Attribute> &attrs) const;
private:
    Iterator* _iterator;
    Condition _cond;
};


class Project : public Iterator {
    // Projection operator
public:
    Project(Iterator *input, const vector<string> &attrNames) : _iteratr(input), _projected(attrNames){}
    ~Project(){};
    
    RC getNextTuple(void *data);
    // For attribute in vector<Attribute>, name it as rel.attr
    void getAttributes(vector<Attribute> &attrs) const;
private:
    Iterator* _iteratr;
    vector<string> _projected; //projected is also relation.attr schema
};


class NLJoin : public Iterator {
    // Nested-Loop join operator
public:
    NLJoin(Iterator *leftIn,                             // Iterator of input R
           TableScan *rightIn,                           // TableScan Iterator of input S
           const Condition &condition,                   // Join condition
           const unsigned numPages                       // Number of pages can be used to do join (decided by the optimizer)
    ): _left(leftIn), _right(rightIn), _cond(condition), _numPages(numPages){
//        _buff = new char[_numPages * PAGE_SIZE];
    };
    ~NLJoin(){
//        delete _buff;
    };
    
    RC getNextTuple(void *data);
    // For attribute in vector<Attribute>, name it as rel.attr
    void getAttributes(vector<Attribute> &attrs) const;
private:
    Iterator* _left;
    TableScan* _right;
    Condition _cond;
    unsigned _numPages;
    AttrValue _outer;
    char lbuf[PAGE_SIZE];
    char rbuf[PAGE_SIZE];
//    char* _buff;
};


class INLJoin : public Iterator {
    // Index Nested-Loop join operator
public:
    INLJoin(Iterator *leftIn,                               // Iterator of input R
            IndexScan *rightIn,                             // IndexScan Iterator of input S
            const Condition &condition,                     // Join condition
            const unsigned numPages                         // Number of pages can be used to do join (decided by the optimizer)
    ) : _left(leftIn), _right(rightIn), _cond(condition), _numPages(numPages)
    {}
    
    ~INLJoin(){};
    
    RC getNextTuple(void *data);
    // For attribute in vector<Attribute>, name it as rel.attr
    void getAttributes(vector<Attribute> &attrs) const;

private:
    Iterator* _left;
    IndexScan* _right;
    Condition _cond;
    unsigned _numPages;
    AttrValue _outer;
    char lbuf[PAGE_SIZE];
    char rbuf[PAGE_SIZE];
};


class Aggregate : public Iterator {
    // Aggregation operator
public:
    Aggregate(Iterator *input,                              // Iterator of input R
              Attribute aggAttr,                            // The attribute over which we are computing an aggregate
              AggregateOp op                                // Aggregate operation
    ) : _iterator(input), _aggAttr(aggAttr), _op(op)
    {processAll();};
    
    // Extra Credit
    Aggregate(Iterator *input,                              // Iterator of input R
              Attribute aggAttr,                            // The attribute over which we are computing an aggregate
              Attribute gAttr,                              // The attribute over which we are grouping the tuples
              AggregateOp op                                // Aggregate operation
    ) : _iterator(input), _aggAttr(aggAttr), _gAttr(gAttr), _op(op)
    {processAll();};
    
    ~Aggregate(){};
    
    RC getNextTuple(void *data);
    // Please name the output attribute as aggregateOp(aggAttr)
    // E.g. Relation=rel, attribute=attr, aggregateOp=MAX
    // output attrname = "MAX(rel.attr)"
    void getAttributes(vector<Attribute> &attrs) const;

private:
    bool isGroupBy() const{
        return _gAttr.length != 0;
    }
    void processAll();
    void accumulate(AttrValue &add, AttrValue &acc, AttrValue &count);
    void finalize(AttrValue &acc, AttrValue &count);
    
private:
    Iterator* _iterator;
    Attribute _aggAttr;
    Attribute _gAttr;
    AggregateOp _op;
    AttrValue _acc;
    AttrValue _count;
    map<AttrValue, AttrValue> _accMap;
    map<AttrValue, AttrValue> _countMap;
    AttrValue _last;
    char buff[PAGE_SIZE];
};

#endif
