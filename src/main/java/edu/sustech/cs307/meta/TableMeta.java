package edu.sustech.cs307.meta;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.exception.ExceptionTypes;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TableMeta {
    public String tableName;
    public ArrayList<ColumnMeta> columns_list;

    @JsonIgnore
    public Map<String, ColumnMeta> columns; // 列名 -> 列的元数据

    private Map<String, IndexType> indexes; // 索引信息

    private Map<String, Integer> column_rank;

    public enum IndexType {
        BTREE
    }

    public TableMeta(String tableName) {
        this.tableName = tableName;
        this.columns = new HashMap<>();
        this.indexes = new HashMap<>();
    }

    public TableMeta(String tableName, ArrayList<ColumnMeta> columns) {
        this.tableName = tableName;
        this.columns_list = columns;
        this.columns = new HashMap<>();
        this.indexes = new HashMap<>();
        for (ColumnMeta column : columns) {
            this.columns.put(column.name, column);
        }
    }

    @JsonCreator
    public TableMeta(@JsonProperty("tableName") String tableName,
            @JsonProperty("columns_list") ArrayList<ColumnMeta> columns_list,
            @JsonProperty("indexes") Map<String, IndexType> indexes) {
        this.tableName = tableName;
        this.columns_list = columns_list;
        this.columns = new HashMap<>();
        this.indexes = indexes;
        for (var column : columns_list) {
            this.columns.put(column.name, column);
        }
    }

    public void addColumn(ColumnMeta column) throws DBException {
        String columnName = column.name;
        if (this.columns.containsKey(columnName)) {
            throw new DBException(ExceptionTypes.ColumnAlreadyExist(columnName));
        }
        this.columns.put(columnName, column);
        // 修复：同步更新columns_list
        if (this.columns_list == null) {
            this.columns_list = new ArrayList<>();
        }
        this.columns_list.add(column);
    }

    public void dropColumn(String columnName) throws DBException {
        if (!this.columns.containsKey(columnName)) {
            throw new DBException(ExceptionTypes.ColumnDoesNotExist(columnName));
        }
        this.columns.remove(columnName);
        // 修复：同步更新columns_list
        if (this.columns_list != null) {
            this.columns_list.removeIf(column -> column.name.equals(columnName));
        }
    }

    public ColumnMeta getColumnMeta(String columnName) {
        if (this.columns.containsKey(columnName)) {
            return this.columns.get(columnName);
        }
        return null;
    }

    public Map<String, ColumnMeta> getColumns() {
        return this.columns;
    }

    public void setColumns(Map<String, ColumnMeta> columns) {
        this.columns = columns;
    }

    public int columnCount() {
        return this.columns.size();
    }

    public boolean hasColumn(String columnName) {
        return this.columns.containsKey(columnName);
    }

    /**
     * 获取表的主键列名
     * 注：当前实现中，主键列通常是第一个建立索引的列
     * 
     * @return 主键列名，如果没有主键则返回null
     */
    public String getPrimaryKeyColumn() {
        if (indexes == null || indexes.isEmpty()) {
            return null;
        }
        // 返回第一个索引列作为主键列
        // 在当前实现中，主键列就是有索引的列
        return indexes.keySet().iterator().next();
    }

    /**
     * 检查指定列是否为主键列
     * 
     * @param columnName 列名
     * @return 如果是主键列返回true，否则返回false
     */
    public boolean isPrimaryKey(String columnName) {
        String primaryKeyColumn = getPrimaryKeyColumn();
        return primaryKeyColumn != null && primaryKeyColumn.equals(columnName);
    }

    public Map<String, IndexType> getIndexes() {
        return indexes;
    }

    public void setIndexes(Map<String, IndexType> indexes) {
        this.indexes = indexes;
    }
}
