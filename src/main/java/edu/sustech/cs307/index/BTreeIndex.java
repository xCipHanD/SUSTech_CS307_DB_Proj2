package edu.sustech.cs307.index;

import edu.sustech.cs307.record.RID;
import edu.sustech.cs307.value.Value;


import java.util.Iterator;
import java.util.Map;



public class BTreeIndex implements Index {
    private final BTree<Value, RID> btree;

    public BTreeIndex(int t) {
        this.btree = new BTree<Value, RID>(t);
    }

    // 插入方法
    public void insert(Value key, RID rid) {
        btree.insert(key, rid);
    }

    @Override
    public RID EqualTo(Value value) {
        return btree.search(value);
    }

    // 下面的范围查找等可以用 TreeMap 或 BTree 的遍历实现
    @Override
    public Iterator<Map.Entry<Value, RID>> LessThan(Value value, boolean isEqual) {
        // 你可以实现一个遍历方法，返回小于 value 的所有键值对
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<Map.Entry<Value, RID>> MoreThan(Value value, boolean isEqual) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<Map.Entry<Value, RID>> Range(Value low, Value high, boolean leftEqual, boolean rightEqual) {
        throw new UnsupportedOperationException();
    }
}
