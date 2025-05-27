package edu.sustech.cs307.system;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.index.Index;
import edu.sustech.cs307.meta.TableMeta;
import edu.sustech.cs307.meta.MetaManager;
import edu.sustech.cs307.record.RID;
import edu.sustech.cs307.record.Record;
import edu.sustech.cs307.value.Value;
import edu.sustech.cs307.meta.ColumnMeta;
import org.pmw.tinylog.Logger;

/**
 * 索引同步管理器：负责在数据变更时同步更新相关索引
 */
public class IndexSynchronizer {
    private final IndexManager indexManager;
    private final MetaManager metaManager;

    public IndexSynchronizer(IndexManager indexManager, MetaManager metaManager) {
        this.indexManager = indexManager;
        this.metaManager = metaManager;
    }

    /**
     * 在插入记录后同步更新索引
     */
    public void onRecordInserted(String tableName, Record record, RID rid) throws DBException {
        TableMeta tableMeta = metaManager.getTable(tableName);
        if (tableMeta == null || tableMeta.getIndexes() == null) {
            return;
        }

        for (String columnName : tableMeta.getIndexes().keySet()) {
            Index index = indexManager.getIndex(tableName, columnName);
            if (index != null) {
                try {
                    ColumnMeta columnMeta = tableMeta.getColumnMeta(columnName);
                    if (columnMeta != null) {
                        // 使用offset和len从记录中获取列值
                        io.netty.buffer.ByteBuf columnValueBuf = record.GetColumnValue(columnMeta.offset,
                                columnMeta.len);
                        Value columnValue = convertByteBufToValue(columnValueBuf, columnMeta.type);
                        index.insert(columnValue, rid);
                        Logger.debug("Updated index for {}.{} with value {} and RID {}",
                                tableName, columnName, columnValue, rid);
                    }
                } catch (DBException e) {
                    Logger.error("Failed to update index for {}.{} after insert: {}",
                            tableName, columnName, e.getMessage());
                    // 继续处理其他索引，不因为一个索引失败而停止
                }
            }
        }
    }

    /**
     * 在删除记录后同步更新索引
     */
    public void onRecordDeleted(String tableName, Record record, RID rid) throws DBException {
        TableMeta tableMeta = metaManager.getTable(tableName);
        if (tableMeta == null || tableMeta.getIndexes() == null) {
            return;
        }

        for (String columnName : tableMeta.getIndexes().keySet()) {
            Index index = indexManager.getIndex(tableName, columnName);
            if (index != null) {
                try {
                    ColumnMeta columnMeta = tableMeta.getColumnMeta(columnName);
                    if (columnMeta != null) {
                        io.netty.buffer.ByteBuf columnValueBuf = record.GetColumnValue(columnMeta.offset,
                                columnMeta.len);
                        Value columnValue = convertByteBufToValue(columnValueBuf, columnMeta.type);
                        index.delete(columnValue, rid);
                        Logger.debug("Removed from index for {}.{} with value {} and RID {}",
                                tableName, columnName, columnValue, rid);
                    }
                } catch (DBException e) {
                    Logger.error("Failed to update index for {}.{} after delete: {}",
                            tableName, columnName, e.getMessage());
                    // 继续处理其他索引
                }
            }
        }
    }

    /**
     * 在更新记录后同步更新索引
     */
    public void onRecordUpdated(String tableName, Record oldRecord, Record newRecord, RID rid) throws DBException {
        TableMeta tableMeta = metaManager.getTable(tableName);
        if (tableMeta == null || tableMeta.getIndexes() == null) {
            return;
        }

        for (String columnName : tableMeta.getIndexes().keySet()) {
            Index index = indexManager.getIndex(tableName, columnName);
            if (index != null) {
                try {
                    ColumnMeta columnMeta = tableMeta.getColumnMeta(columnName);
                    if (columnMeta != null) {
                        io.netty.buffer.ByteBuf oldValueBuf = oldRecord.GetColumnValue(columnMeta.offset,
                                columnMeta.len);
                        io.netty.buffer.ByteBuf newValueBuf = newRecord.GetColumnValue(columnMeta.offset,
                                columnMeta.len);
                        Value oldValue = convertByteBufToValue(oldValueBuf, columnMeta.type);
                        Value newValue = convertByteBufToValue(newValueBuf, columnMeta.type);

                        // 只有当列值发生变化时才需要更新索引
                        if (!oldValue.equals(newValue)) {
                            index.delete(oldValue, rid);
                            index.insert(newValue, rid);
                            Logger.debug("Updated index for {}.{} changed from {} to {} for RID {}",
                                    tableName, columnName, oldValue, newValue, rid);
                        }
                    }
                } catch (DBException e) {
                    Logger.error("Failed to update index for {}.{} after update: {}",
                            tableName, columnName, e.getMessage());
                    // 继续处理其他索引
                }
            }
        }
    }

    /**
     * 批量重建表的所有索引
     */
    public void rebuildIndexesForTable(String tableName) throws DBException {
        TableMeta tableMeta = metaManager.getTable(tableName);
        if (tableMeta == null || tableMeta.getIndexes() == null) {
            Logger.info("No indexes to rebuild for table {}", tableName);
            return;
        }

        Logger.info("Rebuilding indexes for table {}", tableName);

        for (String columnName : tableMeta.getIndexes().keySet()) {
            try {
                // 删除现有索引
                indexManager.dropIndex(tableName, columnName);

                // 重新创建索引
                Index newIndex = indexManager.createIndex(tableName, columnName);
                Logger.info("Successfully rebuilt index for {}.{}", tableName, columnName);

                // 注意：在实际实现中，这里需要遍历表中的所有记录并重新插入到索引中
                // 这通常需要与RecordManager配合完成

            } catch (DBException e) {
                Logger.error("Failed to rebuild index for {}.{}: {}", tableName, columnName, e.getMessage());
                throw e;
            }
        }
    }

    /**
     * 将ByteBuf转换为Value对象
     */
    private Value convertByteBufToValue(io.netty.buffer.ByteBuf byteBuf, edu.sustech.cs307.value.ValueType columnType)
            throws DBException {
        if (columnType == edu.sustech.cs307.value.ValueType.INTEGER) {
            return new Value(byteBuf.getLong(0));
        } else if (columnType == edu.sustech.cs307.value.ValueType.CHAR) {
            return new Value(byteBuf
                    .getCharSequence(0, byteBuf.readableBytes(), java.nio.charset.StandardCharsets.UTF_8).toString());
        } else if (columnType == edu.sustech.cs307.value.ValueType.FLOAT) {
            return new Value(byteBuf.getFloat(0));
        } else if (columnType == edu.sustech.cs307.value.ValueType.DOUBLE) {
            return new Value(byteBuf.getDouble(0));
        } else {
            throw new DBException(
                    edu.sustech.cs307.exception.ExceptionTypes.UnsupportedValueType("IndexSynchronizer", columnType));
        }
    }
}