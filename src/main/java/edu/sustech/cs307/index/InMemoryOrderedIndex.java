package edu.sustech.cs307.index;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.record.RID;
import edu.sustech.cs307.value.Value;
import java.util.TreeMap;

import org.pmw.tinylog.Logger;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList; // Added for ArrayList
import java.util.Collections; // Added for Collections.singletonList and Collections.emptyList
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;

public class InMemoryOrderedIndex implements Index {

    private TreeMap<Value, RID> indexMap;
    private String persistPath; // Added to store persistPath
    private String tableName; // Added
    private String columnName; // Added

    public InMemoryOrderedIndex(String persistPath, String tableName, String columnName) { // Modified constructor
        this.persistPath = persistPath;
        this.tableName = tableName; // Initialize tableName
        this.columnName = columnName; // Initialize columnName
        // read from persistPath
        try {
            File file = new File(persistPath);
            if (file.exists()) {
                ObjectMapper objectMapper = new ObjectMapper();
                TypeReference<TreeMap<Value, RID>> typeRef = new TypeReference<>() {
                };
                this.indexMap = new TreeMap<>(objectMapper.readValue(file, typeRef));
            } else {
                this.indexMap = new TreeMap<>(); // Initialize if file doesn't exist
            }
        } catch (IOException e) {
            Logger.error("Error loading index data: " + e.getMessage());
            this.indexMap = new TreeMap<>(); // Initialize on error
        }
    }

    @Override
    public RID EqualTo(Value value) {
        // or throw an exception if preferred
        return indexMap.getOrDefault(value, null);
    }

    /**
     * 返回一个迭代器，该迭代器用于遍历所有严格小于给定值的条目。
     * 
     * @param value 要比较的值
     * @return 一个迭代器，按从大到小的顺序遍历所有严格小于给定值的条目
     */
    @Override
    public Iterator<Entry<Value, RID>> LessThan(Value value, boolean isEqual) {
        // 获取严格小于value的所有条目视图
        NavigableMap<Value, RID> subMap = indexMap.headMap(value, isEqual); // Modified to use isEqual

        // 使用descendingMap获取从大到小的迭代器
        return subMap.descendingMap().entrySet().iterator();
    }

    /**
     * 返回一个迭代器，遍历所有严格大于给定值的条目。
     *
     * @param value 要比较的值
     * @return 一个迭代器，包含所有严格大于指定值的条目
     */
    @Override
    public Iterator<Entry<Value, RID>> MoreThan(Value value, boolean isEqual) {
        // 获取严格大于value的所有条目视图
        NavigableMap<Value, RID> subMap = indexMap.tailMap(value, isEqual); // Modified to use isEqual
        return subMap.entrySet().iterator();
    }

    /**
     * 返回指定范围内的条目迭代器。
     * 
     * @param low        范围的下界
     * @param high       范围的上界
     * @param leftEqual  是否包含下界
     * @param rightEqual 是否包含上界
     * @return 指定范围内条目的迭代器
     */
    @Override
    public Iterator<Entry<Value, RID>> Range(Value low, Value high, boolean leftEqual, boolean rightEqual) {
        // 获取范围视图（左闭右开）
        NavigableMap<Value, RID> subMap = indexMap.subMap(
                low, leftEqual,
                high, rightEqual);

        return subMap.entrySet().iterator();
    }

    @Override
    public List<RID> search(Value key) throws DBException {
        // TODO Auto-generated method stub
        // throw new UnsupportedOperationException("Unimplemented method 'search'");
        if (indexMap.containsKey(key)) {
            return Collections.singletonList(indexMap.get(key));
        }
        return Collections.emptyList();
    }

    @Override
    public List<RID> searchRange(Value startKey, Value endKey, boolean startInclusive, boolean endInclusive)
            throws DBException {
        // TODO Auto-generated method stub
        // throw new UnsupportedOperationException("Unimplemented method
        // 'searchRange'");
        NavigableMap<Value, RID> subMap;
        if (startKey == null && endKey == null) {
            subMap = indexMap;
        } else if (startKey == null) {
            subMap = indexMap.headMap(endKey, endInclusive);
        } else if (endKey == null) {
            subMap = indexMap.tailMap(startKey, startInclusive);
        } else {
            subMap = indexMap.subMap(startKey, startInclusive, endKey, endInclusive);
        }
        return new ArrayList<>(subMap.values());
    }

    @Override
    public void insert(Value key, RID rid) throws DBException {
        if (indexMap.containsKey(key)) {
            // Handle duplicate key insertion if necessary.
            // For now, we might overwrite or throw an exception.
            // This implementation overwrites.
            Logger.warn("Key " + key + " already exists. Overwriting RID.");
        }
        indexMap.put(key, rid);
        // Persist changes
        persistIndex();
    }

    @Override
    public void delete(Value key, RID rid) throws DBException {
        // This simple delete removes the key regardless of the RID.
        // If multiple RIDs per key were supported, this would need to be more specific.
        if (indexMap.containsKey(key) && indexMap.get(key).equals(rid)) {
            indexMap.remove(key);
            // Persist changes
            persistIndex();
        } else {
            // Key not found or RID does not match
            Logger.warn("Key " + key + " with RID " + rid + " not found for deletion.");
        }
    }

    private void persistIndex() {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.writeValue(new File(persistPath), indexMap);
        } catch (IOException e) {
            Logger.error("Error persisting index data: " + e.getMessage());
        }
    }

    @Override
    public String getTableName() {
        return this.tableName;
    }

    @Override
    public String getColumnName() {
        return this.columnName;
    }

    public Iterator<Entry<Value, RID>> getFullIterator() {
        return indexMap.entrySet().iterator();
    }
}
