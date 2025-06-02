package edu.sustech.cs307.system;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.exception.ExceptionTypes;
import edu.sustech.cs307.index.BPlusTreeIndex;
import edu.sustech.cs307.index.Index;
import edu.sustech.cs307.meta.MetaManager;
import edu.sustech.cs307.meta.TableMeta;
import edu.sustech.cs307.meta.ColumnMeta;
import edu.sustech.cs307.record.RecordFileHandle;
import edu.sustech.cs307.record.RecordPageHandle;
import edu.sustech.cs307.record.Record;
import edu.sustech.cs307.record.RID;
import edu.sustech.cs307.record.BitMap;
import edu.sustech.cs307.value.Value;
import edu.sustech.cs307.value.ValueType;
import org.pmw.tinylog.Logger;

import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

public class IndexManager {
    // Structure: tableName -> columnName -> Index object
    private final Map<String, Map<String, Index>> indexes;
    private final MetaManager metaManager; // To get TableMeta for degree calculation etc.
    private final RecordManager recordManager; // 添加RecordManager引用

    // Default B+ Tree degree if not specified or found in metadata
    private static final int DEFAULT_BTREE_DEGREE = 10; // Example degree

    public IndexManager(MetaManager metaManager, RecordManager recordManager) {
        this.indexes = new ConcurrentHashMap<>();
        this.metaManager = metaManager;
        this.recordManager = recordManager;
    }

    /**
     * Creates a new index for a given table and column and populates it with
     * existing data.
     * If an index already exists for this combination, it might be overwritten or
     * an error thrown,
     * depending on desired behavior (currently overwrites).
     *
     * @param tableName  The name of the table.
     * @param columnName The name of the column to index.
     * @return The created Index object.
     * @throws DBException If the table or column does not exist, or index creation
     *                     fails.
     */
    public synchronized Index createIndex(String tableName, String columnName) throws DBException {

        Index existingIndex = getIndex(tableName, columnName);
        if (existingIndex != null) {
            Logger.info("Index for {}.{} already exists, returning existing index", tableName, columnName);
            return existingIndex;
        }

        TableMeta tableMeta = metaManager.getTable(tableName);
        if (tableMeta == null) {
            throw new DBException(ExceptionTypes.TableDoesNotExist(tableName));
        }
        ColumnMeta columnMeta = tableMeta.getColumnMeta(columnName);
        if (columnMeta == null) {
            throw new DBException(ExceptionTypes.ColumnDoesNotExist(columnName));
        }

        // For now, we only support BPlusTreeIndex.
        // The degree could be configurable, e.g., from table metadata or a global
        // setting.
        // int degree = tableMeta.getIndexDegree(columnName); // Hypothetical method
        int degree = DEFAULT_BTREE_DEGREE; // Use default for now

        Index index = new BPlusTreeIndex(tableName, columnName, degree);

        indexes.computeIfAbsent(tableName, k -> new ConcurrentHashMap<>()).put(columnName, index);

        Logger.info("Created B+Tree index for {}.{} with degree {}", tableName, columnName, degree);

        // 立即填充现有数据到索引中
        populateIndexWithExistingData(index, tableName, columnMeta);

        return index;
    }

    /**
     * 将表中的现有数据填充到索引中
     */
    private void populateIndexWithExistingData(Index index, String tableName, ColumnMeta columnMeta)
            throws DBException {
        try {
            // 打开表文件
            RecordFileHandle fileHandle = recordManager.OpenFile(tableName);
            Logger.info("Starting to populate index for {}.{} with existing data", tableName, columnMeta.name);

            int recordCount = 0;
            int totalPages = fileHandle.getFileHeader().getNumberOfPages();
            int recordsPerPage = fileHandle.getFileHeader().getNumberOfRecordsPrePage();

            // 遍历所有页面
            for (int pageNum = 1; pageNum <= totalPages; pageNum++) {
                try {
                    RecordPageHandle pageHandle = fileHandle.FetchPageHandle(pageNum);

                    // 遍历页面中的所有槽位
                    for (int slotNum = 0; slotNum < recordsPerPage; slotNum++) {
                        // 检查槽位是否有效（使用位图）
                        if (BitMap.isSet(pageHandle.bitmap, slotNum)) {
                            RID rid = new RID(pageNum, slotNum);
                            Record record = fileHandle.GetRecord(rid);

                            // 提取列值
                            io.netty.buffer.ByteBuf columnValueBuf = record.GetColumnValue(columnMeta.offset,
                                    columnMeta.len);
                            Value columnValue = convertByteBufToValue(columnValueBuf, columnMeta.type);

                            // 插入到索引中
                            index.insert(columnValue, rid);
                            recordCount++;
                        }
                    }
                } catch (DBException e) {
                    // 如果页面不存在，可能是因为页面数量不准确，继续下一页
                    Logger.warn("Failed to access page {} in table {}: {}", pageNum, tableName, e.getMessage());
                    continue;
                }
            }

            // 关闭文件
            recordManager.CloseFile(fileHandle);
            Logger.info("Successfully populated index for {}.{} with {} records", tableName, columnMeta.name,
                    recordCount);

        } catch (DBException e) {
            Logger.error("Failed to populate index for {}.{}: {}", tableName, columnMeta.name, e.getMessage());
            throw e;
        }
    }

    /**
     * 将ByteBuf转换为Value对象
     */
    private Value convertByteBufToValue(io.netty.buffer.ByteBuf buf, ValueType type) throws DBException {
        try {
            switch (type) {
                case INTEGER:
                    return new Value(buf.readLong());
                case FLOAT:
                    return new Value(buf.readFloat());
                case DOUBLE:
                    return new Value(buf.readDouble());
                case CHAR:
                    byte[] bytes = new byte[buf.readableBytes()];
                    buf.readBytes(bytes);
                    // 移除trailing null bytes
                    String str = new String(bytes).trim().replaceAll("\0", "");
                    return new Value(str);
                default:
                    throw new DBException(ExceptionTypes
                            .UnsupportedValueType("Unsupported column type: " + type, type));
            }
        } finally {
            buf.release(); // 释放ByteBuf资源
        }
    }

    /**
     * Retrieves an existing index.
     *
     * @param tableName  The name of the table.
     * @param columnName The name of the column.
     * @return The Index object, or null if no index exists for this combination.
     */
    public Index getIndex(String tableName, String columnName) {
        Map<String, Index> tableIndexes = indexes.get(tableName);
        if (tableIndexes != null) {
            return tableIndexes.get(columnName);
        }
        return null;
    }

    /**
     * Removes an index.
     *
     * @param tableName  The name of the table.
     * @param columnName The name of the column.
     * @return True if the index was removed, false otherwise.
     */
    public synchronized boolean dropIndex(String tableName, String columnName) {
        Map<String, Index> tableIndexes = indexes.get(tableName);
        if (tableIndexes != null && tableIndexes.containsKey(columnName)) {
            // 获取要删除的索引实例
            Index indexToRemove = tableIndexes.get(columnName);

            // 完整的 B+Tree 生命周期管理 - 清理索引资源
            try {
                if (indexToRemove instanceof BPlusTreeIndex) {
                    BPlusTreeIndex btreeIndex = (BPlusTreeIndex) indexToRemove;
                    // 清理 B+Tree 的内部结构和缓存
                    btreeIndex.clear(); // 假设 BPlusTreeIndex 有 clear 方法
                    Logger.debug("Cleared B+Tree internal structures for {}.{}", tableName, columnName);
                }
            } catch (Exception e) {
                Logger.warn("Failed to clear B+Tree structures for {}.{}: {}", tableName, columnName, e.getMessage());
                // 继续删除操作，不因清理失败而停止
            }

            // 从内存中移除索引
            tableIndexes.remove(columnName);
            if (tableIndexes.isEmpty()) {
                indexes.remove(tableName);
            }

            // 更新元数据
            try {
                TableMeta tableMeta = metaManager.getTable(tableName);
                if (tableMeta != null && tableMeta.getIndexes() != null) {
                    tableMeta.getIndexes().remove(columnName);
                    metaManager.saveToJson();
                    Logger.debug("Updated metadata after dropping index for {}.{}", tableName, columnName);
                }
            } catch (DBException e) {
                Logger.warn("Failed to update metadata after dropping index: {}", e.getMessage());
                // 不抛出异常，因为索引已经从内存中删除
            }

            Logger.info("Successfully dropped B+Tree index for {}.{}", tableName, columnName);
            return true;
        }
        Logger.warn("Attempted to drop non-existent index for {}.{}", tableName, columnName);
        return false;
    }

    /**
     * 删除指定表的所有索引 - 完整的生命周期管理
     *
     * @param tableName 表名
     * @return 删除的索引数量
     */
    public synchronized int dropAllIndexesForTable(String tableName) {
        Map<String, Index> tableIndexes = indexes.get(tableName);
        if (tableIndexes != null) {
            int indexCount = tableIndexes.size();

            // 逐个清理每个索引的资源
            for (Map.Entry<String, Index> entry : tableIndexes.entrySet()) {
                String columnName = entry.getKey();
                Index index = entry.getValue();

                try {
                    if (index instanceof BPlusTreeIndex) {
                        BPlusTreeIndex btreeIndex = (BPlusTreeIndex) index;
                        btreeIndex.clear(); // 清理 B+Tree 内部结构
                        Logger.debug("Cleared B+Tree structures for {}.{}", tableName, columnName);
                    }
                } catch (Exception e) {
                    Logger.warn("Failed to clear B+Tree structures for {}.{}: {}",
                            tableName, columnName, e.getMessage());
                }
            }

            // 清空并移除表的所有索引
            tableIndexes.clear();
            indexes.remove(tableName);

            // 更新元数据
            try {
                TableMeta tableMeta = metaManager.getTable(tableName);
                if (tableMeta != null && tableMeta.getIndexes() != null) {
                    tableMeta.getIndexes().clear();
                    metaManager.saveToJson();
                    Logger.debug("Updated metadata after dropping all indexes for table {}", tableName);
                }
            } catch (DBException e) {
                Logger.warn("Failed to update metadata after dropping all indexes for table {}: {}",
                        tableName, e.getMessage());
            }

            Logger.info("Successfully dropped all {} B+Tree indexes for table {}", indexCount, tableName);
            return indexCount;
        }
        Logger.info("No indexes found for table {}", tableName);
        return 0;
    }

    /**
     * Loads all indexes defined in the metadata.
     * This would typically be called at DB startup.
     */
    public void loadAllIndexes() throws DBException {
        Logger.info("Loading all indexes from metadata...");

        Set<String> tableNames = metaManager.getTableNames();
        int totalIndexesLoaded = 0;

        for (String tableName : tableNames) {
            try {
                TableMeta tableMeta = metaManager.getTable(tableName);
                if (tableMeta != null && tableMeta.getIndexes() != null) {
                    Map<String, TableMeta.IndexType> tableIndexes = tableMeta.getIndexes();

                    for (String columnName : tableIndexes.keySet()) {
                        if (getIndex(tableName, columnName) == null) {
                            try {
                                Index index = createIndex(tableName, columnName);
                                Logger.info("Auto-loaded index for {}.{} based on metadata", tableName, columnName);
                                totalIndexesLoaded++;
                            } catch (DBException e) {
                                Logger.error("Failed to load index for {}.{}: {}", tableName, columnName,
                                        e.getMessage());
                            }
                        } else {
                            Logger.debug("Index for {}.{} already exists, skipping", tableName, columnName);
                        }
                    }
                }
            } catch (DBException e) {
                Logger.error("Failed to load indexes for table {}: {}", tableName, e.getMessage());
            }
        }

        Logger.info("Completed loading indexes. Total loaded: {}", totalIndexesLoaded);
    }
}
