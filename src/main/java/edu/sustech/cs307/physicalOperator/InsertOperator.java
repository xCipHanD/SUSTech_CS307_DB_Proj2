package edu.sustech.cs307.physicalOperator;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.exception.ExceptionTypes;
import edu.sustech.cs307.meta.ColumnMeta;
import edu.sustech.cs307.meta.TableMeta;
import edu.sustech.cs307.system.DBManager;
import edu.sustech.cs307.tuple.TempTuple;
import edu.sustech.cs307.tuple.Tuple;
import edu.sustech.cs307.value.Value;
import edu.sustech.cs307.value.ValueType;
import edu.sustech.cs307.record.RID;
import edu.sustech.cs307.record.Record;
import edu.sustech.cs307.system.IndexSynchronizer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;

public class InsertOperator implements PhysicalOperator {
    private final String data_file;
    private final List<Value> values;
    private final DBManager dbManager;
    private final int columnSize;
    private int rowCount;
    private boolean outputed;

    public InsertOperator(String data_file, List<String> columnNames, List<Value> values, DBManager dbManager) {
        this.data_file = data_file;
        this.values = values;
        this.dbManager = dbManager;
        this.columnSize = columnNames.size();
        this.rowCount = 0;
        this.outputed = false;
    }

    @Override
    public boolean hasNext() {
        return !this.outputed;
    }

    @Override
    public void Begin() throws DBException {
        try {
            var fileHandle = dbManager.getRecordManager().OpenFile(data_file);
            var tableMeta = dbManager.getMetaManager().getTable(data_file);

            // 🚀 性能优化：启用批量模式减少磁盘同步
            dbManager.getDiskManager().setBatchMode(true);

            // 获取索引同步器
            IndexSynchronizer indexSynchronizer = new IndexSynchronizer(
                    dbManager.getIndexManager(),
                    dbManager.getMetaManager());

            // 获取主键列信息
            String primaryKeyColumn = tableMeta.getPrimaryKeyColumn();
            int primaryKeyIndex = -1;

            if (primaryKeyColumn != null) {
                // 找到主键列在列序列中的索引
                for (int i = 0; i < tableMeta.columns_list.size(); i++) {
                    if (tableMeta.columns_list.get(i).name.equals(primaryKeyColumn)) {
                        primaryKeyIndex = i;
                        break;
                    }
                }
            }

            // 🚀 性能优化：批量收集主键值进行冲突检查（只扫描一次表）
            if (primaryKeyColumn != null && primaryKeyIndex != -1) {
                Set<Value> primaryKeyValues = new HashSet<>();
                int totalRows = values.size() / columnSize;

                // 收集所有要插入的主键值
                for (int row = 0; row < totalRows; row++) {
                    Value primaryKeyValue = values.get(row * columnSize + primaryKeyIndex);
                    primaryKeyValues.add(primaryKeyValue);
                }

                // 批量检查主键冲突（只扫描一次表或使用索引）
                if (batchCheckPrimaryKeyConflict(tableMeta, primaryKeyColumn, primaryKeyValues)) {
                    throw new DBException(ExceptionTypes.PrimaryKeyViolation(
                            data_file, primaryKeyColumn, "One or more primary key conflicts detected"));
                }
            }

            // 🚀 性能优化：批量插入记录
            List<RID> insertedRIDs = new ArrayList<>();
            List<Record> insertedRecords = new ArrayList<>();

            // 使用更大的缓冲区进行批量操作，减少内存分配
            int totalRows = values.size() / columnSize;
            // 预分配容量以避免动态扩容
            insertedRIDs = new ArrayList<>(totalRows);
            insertedRecords = new ArrayList<>(totalRows);

            // 批量处理行数据
            for (int row = 0; row < totalRows; row++) {
                // 为当前行创建缓冲区
                ByteBuf rowBuffer = Unpooled.buffer();
                try {
                    // 写入当前行的所有列数据
                    for (int col = 0; col < columnSize; col++) {
                        Value value = values.get(row * columnSize + col);
                        rowBuffer.writeBytes(value.ToByte());
                    }

                    // 插入记录并收集RID
                    RID insertedRID = fileHandle.InsertRecord(rowBuffer);
                    insertedRIDs.add(insertedRID);
                } finally {
                    rowBuffer.release(); // 确保释放行缓冲区
                }
            }

            // 🚀 性能优化：延迟索引同步到批量插入完成后
            // 只有在所有记录插入成功后才同步索引
            if (!insertedRIDs.isEmpty()) {
                // 批量获取插入的记录用于索引同步
                for (RID rid : insertedRIDs) {
                    Record record = fileHandle.GetRecord(rid);
                    insertedRecords.add(record);
                }

                // 批量同步索引
                try {
                    batchUpdateIndexes(indexSynchronizer, insertedRecords, insertedRIDs);
                } catch (DBException e) {
                    org.pmw.tinylog.Logger.warn("Failed to batch update indexes after insert: {}", e.getMessage());
                    // 继续执行，不因为索引更新失败而中断插入操作
                }
            }

            this.rowCount = totalRows;

            // 🚀 性能优化：批量操作完成后强制同步并关闭批量模式
            dbManager.getDiskManager().setBatchMode(false);
            dbManager.getDiskManager().forceSyncAll();

        } catch (Exception e) {
            // 确保在异常情况下也关闭批量模式
            try {
                dbManager.getDiskManager().setBatchMode(false);
            } catch (Exception ignored) {
            }

            if (e instanceof DBException) {
                throw e;
            }
            throw new RuntimeException(
                    "Failed to insert record: " + e.getMessage() + "\n");
        }
    }

    /**
     * 批量检查主键冲突 - 性能优化版本
     */
    private boolean batchCheckPrimaryKeyConflict(TableMeta tableMeta, String primaryKeyColumn,
            Set<Value> primaryKeyValues)
            throws DBException {
        try {
            var indexManager = dbManager.getIndexManager();
            var index = indexManager.getIndex(tableMeta.tableName, primaryKeyColumn);

            if (index != null) {
                // 使用索引批量查找
                for (Value pkValue : primaryKeyValues) {
                    var matchingRIDs = index.search(pkValue);
                    if (matchingRIDs != null && !matchingRIDs.isEmpty()) {
                        return true; // 发现冲突
                    }
                }
                return false;
            } else {
                // 如果没有索引，使用优化的全表扫描
                return batchCheckPrimaryKeyConflictByTableScan(tableMeta, primaryKeyColumn, primaryKeyValues);
            }
        } catch (Exception e) {
            // 如果索引查找失败，回退到全表扫描
            return batchCheckPrimaryKeyConflictByTableScan(tableMeta, primaryKeyColumn, primaryKeyValues);
        }
    }

    /**
     * 批量全表扫描检查主键冲突 - 只扫描一次表
     */
    private boolean batchCheckPrimaryKeyConflictByTableScan(TableMeta tableMeta, String primaryKeyColumn,
            Set<Value> primaryKeyValues) throws DBException {
        try {
            var seqScan = new SeqScanOperator(tableMeta.tableName, dbManager);
            seqScan.Begin();

            ColumnMeta primaryKeyMeta = tableMeta.getColumnMeta(primaryKeyColumn);
            if (primaryKeyMeta == null) {
                return false;
            }

            // 只扫描一次表，检查所有主键值
            while (seqScan.hasNext()) {
                seqScan.Next();
                var tuple = seqScan.Current();
                if (tuple != null) {
                    var tabCol = new edu.sustech.cs307.meta.TabCol(tableMeta.tableName, primaryKeyColumn);
                    Value existingValue = tuple.getValue(tabCol);
                    if (existingValue != null && primaryKeyValues.contains(existingValue)) {
                        seqScan.Close();
                        return true; // 发现冲突
                    }
                }
            }
            seqScan.Close();
            return false; // 没有冲突
        } catch (Exception e) {
            throw new DBException(
                    ExceptionTypes.InvalidOperation("Failed to batch check primary key conflict: " + e.getMessage()));
        }
    }

    /**
     * 批量更新索引
     */
    private void batchUpdateIndexes(IndexSynchronizer indexSynchronizer,
            List<Record> insertedRecords, List<RID> insertedRIDs) throws DBException {
        for (int i = 0; i < insertedRecords.size(); i++) {
            indexSynchronizer.onRecordInserted(data_file, insertedRecords.get(i), insertedRIDs.get(i));
        }
    }

    @Override
    public void Next() {
    }

    @Override
    public Tuple Current() {
        ArrayList<Value> values = new ArrayList<>();
        values.add(new Value(rowCount, ValueType.INTEGER));
        this.outputed = true;
        return new TempTuple(values);
    }

    @Override
    public void Close() {
    }

    @Override
    public ArrayList<ColumnMeta> outputSchema() {
        ArrayList<ColumnMeta> outputSchema = new ArrayList<>();
        outputSchema.add(new ColumnMeta("insert", "numberOfInsertRows", ValueType.INTEGER, 0, 0));
        return outputSchema;
    }

    public void reset() {
        // nothing to do
    }

    public Tuple getNextTuple() {
        return null;
    }
}
