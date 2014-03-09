
#include "qe.h"
#include "../rbf/rbfm.h"

#define rm (RelationManager::instance())

#define SCHEME_NAME(x,y) string(x+"."+y)

static AttrValue convert(Value v){
    return ValueStream((char*)v.data).read(v.type);
}

//return table_name, attribute_name
static pair<string, string> split_condition(string s){
    int pos = (int)s.find('.');
    return make_pair(s.substr(0, pos), s.substr(pos+1));
}

#define QE_TBNAME(s) (split_condition(s).first)
#define QE_ATNAME(s) (split_condition(s).second)

void Filter::getAttributes(vector<Attribute> &attrs) const{
    _iterator->getAttributes(attrs);
}

//assume Filter's cond is alwasys attr >/</= value
RC Filter::getNextTuple(void *data){
    vector<Attribute> attrs;
    getAttributes(attrs);
    AttrValue other = convert(_cond.rhsValue), av;
    while(_iterator->getNextTuple(data) != -1){
        assert(ValueStream(data).search(attrs, _cond.lhsAttr, av));
        if (AttrValue::compareValue(av, other, _cond.op))
            return 0;
    }
    return QE_EOF;
}


RC Project::getNextTuple(void *data){
    char buffer[PAGE_SIZE];
    vector<Attribute> attrs, tbAttrs;
    _iteratr->getAttributes(attrs); //it's _iterator's attrs
    AttrValue av;
    while (_iteratr->getNextTuple(buffer) != -1) {
        ValueStream output(data);
        for (int i = 0; i < (int)_projected.size(); i++) {
            ValueStream(buffer).search(attrs, _projected[i], av);
            output.write(av);
        }
        return 0;
    }
    return QE_EOF;
}

void Project::getAttributes(vector<Attribute> &attrs) const{
    vector<Attribute> descriptor;
    attrs.clear();
    _iteratr->getAttributes(descriptor);
    for (int i = 0; i < (int)_projected.size(); i++)
        for (int j = 0; j < (int)descriptor.size(); j++)
            if (_projected[i] == descriptor[j].name)
                attrs.push_back(descriptor[j]);
}



void NLJoin::getAttributes(vector<Attribute> &attrs) const{
    _left->getAttributes(attrs);
    vector<Attribute> tmp;
    _right->getAttributes(tmp);
    attrs.insert(attrs.end(), tmp.begin(), tmp.end());
}


static void output(void *lbuf, vector<Attribute> &lAttrs,
                    void *rbuf, vector<Attribute> &rAttrs, void *data){
    ValueStream output(data);
    AttrValue ov;
    for (int i = 0; i < (int)lAttrs.size(); i++){
        assert(ValueStream(lbuf).search(lAttrs, lAttrs[i].name, ov));
        output.write(ov);
    }
    for (int i = 0; i < (int)rAttrs.size(); i++){
        assert(ValueStream(rbuf).search(rAttrs, rAttrs[i].name, ov));
        output.write(ov);
    }
}

RC NLJoin::getNextTuple(void *data){
    vector<Attribute> lAttrs, rAttrs;
    _left->getAttributes(lAttrs);
    _right->getAttributes(rAttrs);
    AttrValue lv, rv, ov;
    
    //right remaining tuples
    while (_outer._len != 0 && _right->getNextTuple(rbuf) != QE_EOF){
        ValueStream(rbuf).search(rAttrs, _cond.rhsAttr, rv);
        if (AttrValue::compareValue(_outer, rv, _cond.op)){
            output(lbuf, lAttrs, rbuf, rAttrs, data);
            return 0;
        }
    }
    while (_left->getNextTuple(lbuf) != QE_EOF) {
        ValueStream(lbuf).search(lAttrs, _cond.lhsAttr, lv);
        _right->setIterator();
        while (_right->getNextTuple(rbuf) != QE_EOF) {
            ValueStream(rbuf).search(rAttrs, _cond.rhsAttr, rv);
            if (AttrValue::compareValue(lv, rv, _cond.op)){
                _outer = lv;
                output(lbuf, lAttrs, rbuf, rAttrs, data);
                return 0;
            }
        }
    }
    return QE_EOF;
}

void  INLJoin::getAttributes(vector<Attribute> &attrs) const{
    _left->getAttributes(attrs);
    vector<Attribute> tmp;
    _right->getAttributes(tmp);
    attrs.insert(attrs.end(), tmp.begin(), tmp.end());
}

RC INLJoin::getNextTuple(void *data){
    vector<Attribute> lAttrs, rAttrs;
    _left->getAttributes(lAttrs);
    _right->getAttributes(rAttrs);
    AttrValue lv, rv, ov;
    char keybuff[PAGE_SIZE];
    
    while (_outer._len != 0 && _right->getNextTuple(rbuf) != QE_EOF){
        ValueStream(rbuf).search(rAttrs, _cond.rhsAttr, rv);
        if (AttrValue::compareValue(_outer, rv, _cond.op)){
            output(lbuf, lAttrs, rbuf, rAttrs, data);
            return 0;
        }
    }
    while (_left->getNextTuple(lbuf) != QE_EOF) {
        ValueStream(lbuf).search(lAttrs, _cond.lhsAttr, lv);
        ValueStream(keybuff).write(lv);
        _right->setIterator(keybuff, keybuff, true, true);
        while (_right->getNextTuple(rbuf) != QE_EOF) {
            ValueStream(rbuf).search(rAttrs, _cond.rhsAttr, rv);
            if (AttrValue::compareValue(lv, rv, _cond.op)){
                _outer = lv;
                output(lbuf, lAttrs, rbuf, rAttrs, data);
                return 0;
            }
        }
    }
    return QE_EOF;
}










