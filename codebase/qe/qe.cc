
#include "qe.h"
#include "../rbf/rbfm.h"
#include <cfloat>

#define rm (RelationManager::instance())

#define SCHEME_NAME(x,y) string(x+"."+y)

static const char * aggOpName[] = {"MIN", "MAX", "SUM", "AVG", "COUNT"};

static AttrValue convert(Value v){
    return ValueStream((char*)v.data).read(v.type);
}



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


void Aggregate::getAttributes(vector<Attribute> &attrs) const{
    attrs.clear();
    if (isGroupBy())
        attrs.push_back(_gAttr);
    attrs.push_back(_aggAttr);
    attrs[1].name = string(aggOpName[_op]) + "(" + attrs[1].name + ")";
}

void Aggregate::accumulate(AttrValue &add, AttrValue &acc, AttrValue &count){
    //intialize
    if (acc._len == 0){
        acc._len = count._len = 4;
        acc._type = _aggAttr.type;
        count._type = TypeInt;
        if (acc._type == TypeInt && _op == MIN)
            acc._iv = INT_MAX;
        if (acc._type == TypeInt && _op == MAX)
            acc._iv = INT_MIN;
        if (acc._type == TypeReal && _op == MIN)
            acc._fv = FLT_MAX;
        if (acc._type == TypeInt && _op == MAX)
            acc._fv = FLT_MIN;
    }
    switch (_op) {
        case MIN:
            acc = min(acc, add);
            break;
        case MAX:
            acc = max(acc, add);
            break;
        case COUNT:
        case AVG:
        case SUM:
            count._iv++;
            acc._iv += add._iv;
            acc._fv += acc._fv;
            break;
    }
}

void Aggregate::finalize(AttrValue &acc, AttrValue &count){
    switch (_op) {
        case COUNT:
            acc = count;
            break;
        case AVG:
            if (acc._type == TypeInt){
                acc._type = TypeReal;
                acc._fv = (float)acc._iv / (float)count._iv;
            }
            else
                acc._fv = acc._fv / (float) count._iv;
        default:
            return;
    }
}

void Aggregate::processAll(){
    vector<Attribute> attrs;
    _iterator->getAttributes(attrs);
    AttrValue av, gv;
    while (_iterator->getNextTuple(buff) != QE_EOF) {
        ValueStream(buff).search(attrs, _aggAttr.name, av);
        if (isGroupBy()){
            ValueStream(buff).search(attrs, _gAttr.name, gv);
            accumulate(av, _accMap[gv], _countMap[gv]);
        }
        else
            accumulate(av, _acc, _count);
    }
    if (isGroupBy()){
        for (map<AttrValue, AttrValue>::iterator itr = _accMap.begin();
             itr != _accMap.end(); itr++)
            finalize(itr->second, _countMap[itr->first]);
    }
    else
        finalize(_acc, _count);
}

RC Aggregate::getNextTuple(void *data){
    ValueStream output(data);
    if (_last._len == 0){
        if (isGroupBy()){
            _last = (_accMap.begin())->first;
            output.write(_last);
            output.write((_accMap.begin())->second);
        }
        else{
            _last = _acc;
            output.write(_acc);
        }
        return 0;
    }
    if (isGroupBy()){
        map<AttrValue, AttrValue>::iterator itr = _accMap.find(_last);
        if (++itr == _accMap.end())
            return QE_EOF;
        _last = itr->first;
        output.write(_last);
        output.write(itr->second);
        return 0;
    }
    return QE_EOF;
}





