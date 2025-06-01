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

            // 获取索引同步器
            IndexSynchronizer indexSynchronizer = new IndexSynchronizer(
                    dbManager.getIndexManager(),
                    dbManager.getMetaManager());

            // 获取主键列信息
            String primaryKeyColumn = tableMeta.getPrimaryKeyColumn();
            ColumnMeta primaryKeyMeta = null;
            int primaryKeyIndex = -1;

            if (primaryKeyColumn != null) {
                primaryKeyMeta = tableMeta.getColumnMeta(primaryKeyColumn);
                // 找到主键列在列序列中的索引
                for (int i = 0; i < tableMeta.columns_list.size(); i++) {
                    if (tableMeta.columns_list.get(i).name.equals(primaryKeyColumn)) {
                        primaryKeyIndex = i;
                        break;
                    }
                }
            }

            // Serialize values to ByteBuf
            ByteBuf buffer = Unpooled.buffer();
            for (int i = 0; i < values.size(); i++) {
                buffer.writeBytes(values.get(i).ToByte());
                // 当收集完一行的所有列数据时检查主键冲突并插入记录
                if ((i + 1) % columnSize == 0) {
                    // 检查主键冲突
                    if (primaryKeyColumn != null && primaryKeyIndex != -1) {
                        Value primaryKeyValue = values.get(i - columnSize + 1 + primaryKeyIndex);
                        if (checkPrimaryKeyConflict(tableMeta, primaryKeyColumn, primaryKeyValue)) {
                            throw new DBException(ExceptionTypes.PrimaryKeyViolation(
                                    data_file, primaryKeyColumn, primaryKeyValue.toString()));
                        }
                    }

                    // 插入记录并获取RID
                    RID insertedRID = fileHandle.InsertRecord(buffer);

                    // 获取插入的记录以便同步索引
                    Record insertedRecord = fileHandle.GetRecord(insertedRID);

                    // 同步更新所有相关索引
                    try {
                        indexSynchronizer.onRecordInserted(data_file, insertedRecord, insertedRID);
                    } catch (DBException e) {
                        org.pmw.tinylog.Logger.warn("Failed to update indexes after insert: {}", e.getMessage());
                        // 继续执行，不因为索引更新失败而中断插入操作
                    }

                    buffer.clear();
                }
            }
            this.rowCount = values.size() / columnSize;
        } catch (Exception e) {
            if (e instanceof DBException) {
                throw e;
            }
            throw new RuntimeException(
                    "Failed to insert record: " + e.getMessage() + "\n");
        }
    }

    /**
     * 检查主键是否冲突
     * 
     * @param tableMeta        表元数据
     * @param primaryKeyColumn 主键列名
     * @param primaryKeyValue  要检查的主键值
     * @return 如果存在冲突返回true，否则返回false
     */
    private boolean checkPrimaryKeyConflict(TableMeta tableMeta, String primaryKeyColumn, Value primaryKeyValue)
            throws DBException {
        try {
            // 使用索引查找是否存在相同的主键值
            var indexManager = dbManager.getIndexManager();
            var index = indexManager.getIndex(tableMeta.tableName, primaryKeyColumn);

            if (index != null) {
                // 使用索引查找
                var matchingRIDs = index.search(primaryKeyValue);
                return matchingRIDs != null && !matchingRIDs.isEmpty();
            } else {
                // 如果没有索引，使用全表扫描检查主键冲突
                return checkPrimaryKeyConflictByTableScan(tableMeta, primaryKeyColumn, primaryKeyValue);
            }
        } catch (Exception e) {
            // 如果索引查找失败，回退到全表扫描
            return checkPrimaryKeyConflictByTableScan(tableMeta, primaryKeyColumn, primaryKeyValue);
        }
    }

    /**
     * 通过全表扫描检查主键冲突
     */
    private boolean checkPrimaryKeyConflictByTableScan(TableMeta tableMeta, String primaryKeyColumn,
            Value primaryKeyValue) throws DBException {
        try {
            var seqScan = new SeqScanOperator(tableMeta.tableName, dbManager);
            seqScan.Begin();

            ColumnMeta primaryKeyMeta = tableMeta.getColumnMeta(primaryKeyColumn);
            if (primaryKeyMeta == null) {
                return false;
            }

            while (seqScan.hasNext()) {
                seqScan.Next();
                var tuple = seqScan.Current();
                if (tuple != null) {
                    var tabCol = new edu.sustech.cs307.meta.TabCol(tableMeta.tableName, primaryKeyColumn);
                    Value existingValue = tuple.getValue(tabCol);
                    if (existingValue != null && existingValue.equals(primaryKeyValue)) {
                        seqScan.Close();
                        return true; // 发现冲突
                    }
                }
            }
            seqScan.Close();
            return false; // 没有冲突
        } catch (Exception e) {
            throw new DBException(
                    ExceptionTypes.InvalidOperation("Failed to check primary key conflict: " + e.getMessage()));
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
