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
import java.util.Set;
import java.util.HashSet;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TableMeta {
    public String tableName;
    public ArrayList<ColumnMeta> columns_list;

    @JsonIgnore
    public Map<String, ColumnMeta> columns; // 列名 -> 列的元数据

    private Map<String, IndexType> indexes; // 列名 -> 索引类型 (保持向后兼容)

    // 新增：索引名称到列名的映射
    private Map<String, String> indexNameToColumn; // 索引名 -> 列名

    // private Map<String, Integer> column_rank;

    public enum IndexType {
        BTREE
    }

    public TableMeta(String tableName) {
        this.tableName = tableName;
        this.columns = new HashMap<>();
        this.indexes = new HashMap<>();
        this.indexNameToColumn = new HashMap<>();
    }

    public TableMeta(String tableName, ArrayList<ColumnMeta> columns) {
        this.tableName = tableName;
        this.columns_list = columns;
        this.columns = new HashMap<>();
        this.indexes = new HashMap<>();
        this.indexNameToColumn = new HashMap<>();
        for (ColumnMeta column : columns) {
            this.columns.put(column.name, column);
        }
    }

    @JsonCreator
    public TableMeta(@JsonProperty("tableName") String tableName,
            @JsonProperty("columns_list") ArrayList<ColumnMeta> columns_list,
            @JsonProperty("indexes") Map<String, IndexType> indexes,
            @JsonProperty("indexNameToColumn") Map<String, String> indexNameToColumn) {
        this.tableName = tableName;
        this.columns_list = columns_list;
        this.columns = new HashMap<>();
        this.indexes = indexes != null ? indexes : new HashMap<>();
        this.indexNameToColumn = indexNameToColumn != null ? indexNameToColumn : new HashMap<>();

        if (columns_list != null) {
            for (var column : columns_list) {
                this.columns.put(column.name, column);
            }
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
    @JsonIgnore
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

    /**
     * 添加索引信息 (支持索引名称)
     * 
     * @param indexName  索引名称
     * @param columnName 列名
     * @param indexType  索引类型
     */
    public void addIndex(String indexName, String columnName, IndexType indexType) {
        if (this.indexes == null) {
            this.indexes = new HashMap<>();
        }
        if (this.indexNameToColumn == null) {
            this.indexNameToColumn = new HashMap<>();
        }

        this.indexes.put(columnName, indexType);
        this.indexNameToColumn.put(indexName, columnName);
    }

    /**
     * 根据索引名称删除索引
     * 
     * @param indexName 索引名称
     * @return 被删除索引对应的列名，如果索引不存在返回null
     */
    public String removeIndexByName(String indexName) {
        if (this.indexNameToColumn == null) {
            return null;
        }

        String columnName = this.indexNameToColumn.remove(indexName);
        if (columnName != null && this.indexes != null) {
            this.indexes.remove(columnName);
        }
        return columnName;
    }

    /**
     * 根据索引名称查找列名
     * 
     * @param indexName 索引名称
     * @return 对应的列名，如果不存在返回null
     */
    public String getColumnByIndexName(String indexName) {
        if (this.indexNameToColumn == null) {
            return null;
        }
        return this.indexNameToColumn.get(indexName);
    }

    /**
     * 检查索引名称是否存在
     * 
     * @param indexName 索引名称
     * @return 如果存在返回true，否则返回false
     */
    public boolean hasIndexName(String indexName) {
        return this.indexNameToColumn != null && this.indexNameToColumn.containsKey(indexName);
    }

    /**
     * 获取所有索引名称
     * 
     * @return 索引名称集合
     */
    @JsonIgnore
    public Set<String> getIndexNames() {
        if (this.indexNameToColumn == null) {
            return new HashSet<>();
        }
        return this.indexNameToColumn.keySet();
    }

    public Map<String, IndexType> getIndexes() {
        return indexes;
    }

    public void setIndexes(Map<String, IndexType> indexes) {
        this.indexes = indexes;
    }

    public Map<String, String> getIndexNameToColumn() {
        return indexNameToColumn;
    }

    public void setIndexNameToColumn(Map<String, String> indexNameToColumn) {
        this.indexNameToColumn = indexNameToColumn;
    }
}
