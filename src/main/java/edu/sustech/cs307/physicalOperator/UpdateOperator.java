package edu.sustech.cs307.physicalOperator;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.exception.ExceptionTypes;
import edu.sustech.cs307.meta.ColumnMeta;
import edu.sustech.cs307.meta.TabCol;
import edu.sustech.cs307.meta.TableMeta;
import edu.sustech.cs307.record.RecordFileHandle;
import edu.sustech.cs307.record.RID;
import edu.sustech.cs307.record.Record;
import edu.sustech.cs307.system.DBManager;
import edu.sustech.cs307.system.IndexSynchronizer;
import edu.sustech.cs307.tuple.TableTuple;
import edu.sustech.cs307.tuple.TempTuple;
import edu.sustech.cs307.tuple.Tuple;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

import edu.sustech.cs307.value.Value;
import edu.sustech.cs307.value.ValueType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.update.UpdateSet;

public class UpdateOperator implements PhysicalOperator {
    private final SeqScanOperator seqScanOperator;
    private final String tableName;
    private final List<UpdateSet> updateSetList;
    private final Expression whereExpr;
    private final DBManager dbManager;

    private int updateCount;
    private boolean isDone;

    public UpdateOperator(PhysicalOperator inputOperator, String tableName, List<UpdateSet> updateSetList,
            Expression whereExpr, DBManager dbManager) {
        // UpdateOperator 现在可以接受 SeqScanOperator 或 FilterOperator 作为输入
        if (inputOperator instanceof SeqScanOperator seqScanOperator) {
            this.seqScanOperator = seqScanOperator;
        } else if (inputOperator instanceof FilterOperator filterOperator) {
            // 从 FilterOperator 中提取底层的 SeqScanOperator
            PhysicalOperator child = getChildOperator(filterOperator);
            if (!(child instanceof SeqScanOperator)) {
                throw new RuntimeException("The update operator requires SeqScanOperator as the base scanner, but got: "
                        + child.getClass().getSimpleName());
            }
            this.seqScanOperator = (SeqScanOperator) child;
        } else {
            throw new RuntimeException(
                    "The update operator only accepts SeqScanOperator or FilterOperator as input, but got: "
                            + inputOperator.getClass().getSimpleName());
        }

        this.tableName = tableName;
        this.updateSetList = updateSetList;
        this.whereExpr = whereExpr;
        this.dbManager = dbManager;
        this.updateCount = 0;
        this.isDone = false;
    }

    // Helper method to extract the child operator from FilterOperator
    private PhysicalOperator getChildOperator(FilterOperator filterOperator) {
        // 通过反射获取FilterOperator的子操作符
        try {
            java.lang.reflect.Field field = FilterOperator.class.getDeclaredField("child");
            field.setAccessible(true);
            return (PhysicalOperator) field.get(filterOperator);
        } catch (Exception e) {
            throw new RuntimeException("Failed to extract child operator from FilterOperator: " + e.getMessage());
        }
    }

    @Override
    public boolean hasNext() {
        return !isDone;
    }

    @Override
    public void Begin() throws DBException {
        seqScanOperator.Begin();
        RecordFileHandle fileHandle = seqScanOperator.getFileHandle();

        // 获取表元数据和主键信息
        var tableMeta = dbManager.getMetaManager().getTable(tableName);
        String primaryKeyColumn = tableMeta.getPrimaryKeyColumn();

        // 获取索引同步器
        IndexSynchronizer indexSynchronizer = new IndexSynchronizer(
                dbManager.getIndexManager(),
                dbManager.getMetaManager());

        while (seqScanOperator.hasNext()) {
            seqScanOperator.Next();
            TableTuple tuple = (TableTuple) seqScanOperator.Current();

            if (whereExpr == null || tuple.eval_expr(whereExpr)) {
                // 获取更新前的记录用于索引同步
                Record oldRecord = fileHandle.GetRecord(tuple.getRID());

                Value[] oldValues = tuple.getValues();
                List<Value> newValues = new ArrayList<>(Arrays.asList(oldValues));
                TabCol[] schema = tuple.getTupleSchema();

                // 记录主键是否被更新以及新的主键值
                boolean primaryKeyUpdated = false;
                Value newPrimaryKeyValue = null;

                for (UpdateSet currentUpdateSet : this.updateSetList) {
                    List<Column> columnsToUpdate = currentUpdateSet.getColumns();
                    // Corrected: JSqlParser's UpdateSet has getValues() which returns an
                    // ExpressionList,
                    // and ExpressionList has getExpressions() to get the List<Expression>.
                    ExpressionList<Expression> newExpressions = (ExpressionList<Expression>) currentUpdateSet
                            .getValues();

                    if (columnsToUpdate.size() != newExpressions.size()) {
                        // This case should ideally be caught by the parser or an earlier validation
                        // step
                        throw new DBException(ExceptionTypes.InvalidSQL("UPDATE",
                                "Column and value counts do not match in an UPDATE SET clause"));
                    }

                    for (int i = 0; i < columnsToUpdate.size(); i++) {
                        Column column = columnsToUpdate.get(i);
                        Expression expression = newExpressions.get(i);

                        String targetColumnName = column.getColumnName();
                        String targetTable = null;
                        if (column.getTable() != null && column.getTable().getName() != null) {
                            targetTable = column.getTable().getName();
                        } else {
                            // If table is not specified in "SET table.column = ...", assume the table being
                            // updated.
                            // For "SET column = ...", table will be null here.
                            targetTable = tuple.getTableName();
                        }

                        int index = -1;
                        for (int j = 0; j < schema.length; j++) {
                            // Ensure schema table name and column name match, ignoring case.
                            if (schema[j].getColumnName().equalsIgnoreCase(targetColumnName)
                                    && schema[j].getTableName().equalsIgnoreCase(targetTable)) {
                                index = j;
                                break;
                            }
                        }
                        if (index == -1) {
                            throw new DBException(ExceptionTypes
                                    .ColumnDoesNotExist("Column " + targetColumnName + " in table " + targetTable));
                        }
                        // 使用带类型提示的表达式求值，根据目标列的类型来推断常量的正确类型
                        Value newValue = tuple.evaluateExpressionWithTypeHint(expression, targetTable,
                                targetColumnName);
                        newValues.set(index, newValue);

                        // 检查是否更新了主键列
                        if (primaryKeyColumn != null && targetColumnName.equalsIgnoreCase(primaryKeyColumn)) {
                            primaryKeyUpdated = true;
                            newPrimaryKeyValue = newValue;
                        }
                    }
                }

                // 如果主键被更新，检查新的主键值是否与其他记录冲突
                if (primaryKeyUpdated && newPrimaryKeyValue != null) {
                    if (checkPrimaryKeyConflictForUpdate(tableMeta, primaryKeyColumn, newPrimaryKeyValue,
                            tuple.getRID())) {
                        throw new DBException(ExceptionTypes.PrimaryKeyViolation(
                                tableName, primaryKeyColumn, newPrimaryKeyValue.toString()));
                    }
                }

                ByteBuf buffer = Unpooled.buffer();
                for (Value v : newValues) {
                    buffer.writeBytes(v.ToByte());
                }

                fileHandle.UpdateRecord(tuple.getRID(), buffer);

                // 获取更新后的记录用于索引同步
                Record newRecord = fileHandle.GetRecord(tuple.getRID());

                // 同步更新所有相关索引
                try {
                    indexSynchronizer.onRecordUpdated(tableName, oldRecord, newRecord, tuple.getRID());
                } catch (DBException e) {
                    org.pmw.tinylog.Logger.warn("Failed to update indexes after update: {}", e.getMessage());
                    // 继续执行，不因为索引更新失败而中断更新操作
                }

                updateCount++;
            }
        }
    }

    /**
     * 检查UPDATE操作中的主键冲突
     * 
     * @param tableMeta          表元数据
     * @param primaryKeyColumn   主键列名
     * @param newPrimaryKeyValue 新的主键值
     * @param currentRID         当前记录的RID（用于排除自身）
     * @return 如果存在冲突返回true，否则返回false
     */
    private boolean checkPrimaryKeyConflictForUpdate(TableMeta tableMeta, String primaryKeyColumn,
            Value newPrimaryKeyValue, RID currentRID) throws DBException {
        try {
            // 使用索引查找是否存在相同的主键值
            var indexManager = dbManager.getIndexManager();
            var index = indexManager.getIndex(tableMeta.tableName, primaryKeyColumn);

            if (index != null) {
                // 使用索引查找
                var matchingRIDs = index.search(newPrimaryKeyValue);
                if (matchingRIDs != null) {
                    // 检查找到的RID是否不是当前记录的RID
                    for (var rid : matchingRIDs) {
                        if (!rid.equals(currentRID)) {
                            return true; // 发现与其他记录的冲突
                        }
                    }
                }
                return false;
            } else {
                // 如果没有索引，使用全表扫描检查主键冲突
                return checkPrimaryKeyConflictByTableScanForUpdate(tableMeta, primaryKeyColumn, newPrimaryKeyValue,
                        currentRID);
            }
        } catch (Exception e) {
            // 如果索引查找失败，回退到全表扫描
            return checkPrimaryKeyConflictByTableScanForUpdate(tableMeta, primaryKeyColumn, newPrimaryKeyValue,
                    currentRID);
        }
    }

    /**
     * 通过全表扫描检查UPDATE操作中的主键冲突
     */
    private boolean checkPrimaryKeyConflictByTableScanForUpdate(TableMeta tableMeta, String primaryKeyColumn,
            Value newPrimaryKeyValue, RID currentRID) throws DBException {
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
                if (tuple instanceof TableTuple tableTuple) {
                    // 排除当前正在更新的记录
                    if (!tableTuple.getRID().equals(currentRID)) {
                        var tabCol = new edu.sustech.cs307.meta.TabCol(tableMeta.tableName, primaryKeyColumn);
                        Value existingValue = tuple.getValue(tabCol);
                        if (existingValue != null && existingValue.equals(newPrimaryKeyValue)) {
                            seqScan.Close();
                            return true; // 发现冲突
                        }
                    }
                }
            }
            seqScan.Close();
            return false; // 没有冲突
        } catch (Exception e) {
            throw new DBException(ExceptionTypes
                    .InvalidOperation("Failed to check primary key conflict during update: " + e.getMessage()));
        }
    }

    @Override
    public void Next() {
        isDone = true;
    }

    @Override
    public Tuple Current() {
        if (isDone) {
            ArrayList<Value> result = new ArrayList<>();
            result.add(new Value(updateCount, ValueType.INTEGER));
            return new TempTuple(result);
        } else {
            throw new RuntimeException("Call Next() first");
        }
    }

    @Override
    public void Close() {
        seqScanOperator.Close();
    }

    @Override
    public ArrayList<ColumnMeta> outputSchema() {
        ArrayList<ColumnMeta> schema = new ArrayList<>();
        schema.add(new ColumnMeta("update", "numberOfUpdatedRows", ValueType.INTEGER, 0, 0));
        return schema;
    }

    public void reset() {
        updateCount = 0;
        isDone = false;
    }

    public Tuple getNextTuple() {
        if (hasNext()) {
            Next();
            return Current();
        }
        return null;
    }

    public void close() {
        Close();
    }

    public String getTableName() {
        return tableName;
    }
}
