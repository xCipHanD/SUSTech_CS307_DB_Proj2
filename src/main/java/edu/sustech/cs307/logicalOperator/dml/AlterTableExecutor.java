package edu.sustech.cs307.logicalOperator.dml;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.exception.ExceptionTypes;
import edu.sustech.cs307.meta.ColumnMeta;
import edu.sustech.cs307.meta.TableMeta;
import edu.sustech.cs307.physicalOperator.SeqScanOperator;
import edu.sustech.cs307.system.DBManager;
import edu.sustech.cs307.system.IndexSynchronizer;
import edu.sustech.cs307.tuple.TableTuple;
import edu.sustech.cs307.value.Value;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.alter.AlterExpression;
import net.sf.jsqlparser.statement.alter.AlterOperation;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import org.pmw.tinylog.Logger;

import java.util.HashSet;
import java.util.Set;

/**
 * ALTER TABLE 语句执行器
 * 支持删除列和修改列（添加/取消主键）
 */
public class AlterTableExecutor implements DMLExecutor {

    private final Alter alterStmt;
    private final DBManager dbManager;

    public AlterTableExecutor(Alter alterStmt, DBManager dbManager) {
        this.alterStmt = alterStmt;
        this.dbManager = dbManager;
    }

    @Override
    public void execute() throws DBException {
        String tableName = alterStmt.getTable().getName();

        // 检查表是否存在
        if (!dbManager.isTableExists(tableName)) {
            throw new DBException(ExceptionTypes.TableDoesNotExist(tableName));
        }

        Logger.info("Starting ALTER TABLE operation for table {}", tableName);

        try {
            for (AlterExpression alterExpr : alterStmt.getAlterExpressions()) {
                processAlterExpression(tableName, alterExpr);
            }

            Logger.info("Successfully completed ALTER TABLE operation for table {}", tableName);

        } catch (Exception e) {
            if (e instanceof DBException) {
                throw e;
            } else {
                throw new DBException(ExceptionTypes.InvalidOperation("ALTER TABLE failed: " + e.getMessage()));
            }
        }
    }

    private void processAlterExpression(String tableName, AlterExpression alterExpr) throws DBException {
        AlterOperation operation = alterExpr.getOperation();

        if (operation == AlterOperation.DROP) {
            if (alterExpr.getColumnName() != null) {
                dropColumn(tableName, alterExpr.getColumnName());
            } else {
                throw new DBException(ExceptionTypes.UnsupportedCommand("DROP operation must specify a column name"));
            }
        } else if (operation == AlterOperation.MODIFY) {
            if (alterExpr.getColDataTypeList() != null && !alterExpr.getColDataTypeList().isEmpty()) {
                ColumnDefinition colDef = alterExpr.getColDataTypeList().get(0);
                modifyColumn(tableName, colDef);
            } else {
                throw new DBException(
                        ExceptionTypes.UnsupportedCommand("MODIFY operation must specify column definition"));
            }
        } else {
            throw new DBException(ExceptionTypes.UnsupportedCommand("ALTER TABLE operation: " + operation));
        }
    }

    /**
     * 删除列
     */
    private void dropColumn(String tableName, String columnName) throws DBException {
        Logger.info("Dropping column {} from table {}", columnName, tableName);

        TableMeta tableMeta = dbManager.getMetaManager().getTable(tableName);

        if (tableMeta.getColumnMeta(columnName) == null) {
            throw new DBException(ExceptionTypes.ColumnDoesNotExist(columnName));
        }

        String primaryKeyColumn = tableMeta.getPrimaryKeyColumn();
        if (columnName.equals(primaryKeyColumn)) {
            throw new DBException(ExceptionTypes.UnsupportedCommand(
                    "Cannot drop primary key column. Please modify the column to remove primary key first."));
        }

        if (tableMeta.getIndexes() != null && tableMeta.getIndexes().containsKey(columnName)) {
            boolean indexDropped = dbManager.getIndexManager().dropIndex(tableName, columnName);
            if (indexDropped) {
                Logger.info("Successfully dropped B+Tree index for column {}", columnName);
            } else {
                Logger.warn("Failed to drop B+Tree index for column {}", columnName);
            }
        }

        dbManager.getMetaManager().dropColumnInTable(tableName, columnName);

        recalculateColumnOffsets(tableName);

        syncManagerStates(tableName);

        Logger.info("Successfully dropped column {} from table {}", columnName, tableName);
    }

    /**
     * 修改列（添加/取消主键）
     */
    private void modifyColumn(String tableName, ColumnDefinition colDef) throws DBException {
        String columnName = colDef.getColumnName();
        Logger.info("Modifying column {} in table {}", columnName, tableName);

        TableMeta tableMeta = dbManager.getMetaManager().getTable(tableName);
        ColumnMeta columnMeta = tableMeta.getColumnMeta(columnName);

        if (columnMeta == null) {
            throw new DBException(ExceptionTypes.ColumnDoesNotExist(columnName));
        }

        boolean shouldBePrimaryKey = false;
        if (colDef.getColumnSpecs() != null) {
            for (String spec : colDef.getColumnSpecs()) {
                if ("PRIMARY_KEY".equalsIgnoreCase(spec)) {
                    shouldBePrimaryKey = true;
                    break;
                }
            }
        }

        String currentPrimaryKey = tableMeta.getPrimaryKeyColumn();
        boolean isCurrentlyPrimaryKey = columnName.equals(currentPrimaryKey);

        if (shouldBePrimaryKey && !isCurrentlyPrimaryKey) {
            addPrimaryKey(tableName, columnName);
        } else if (!shouldBePrimaryKey && isCurrentlyPrimaryKey) {
            removePrimaryKey(tableName, columnName);
        } else if (shouldBePrimaryKey && isCurrentlyPrimaryKey) {
            Logger.info("Column {} is already a primary key", columnName);
        } else {
            Logger.info("No primary key changes for column {}", columnName);
        }
    }

    /**
     * 添加主键
     */
    private void addPrimaryKey(String tableName, String columnName) throws DBException {
        TableMeta tableMeta = dbManager.getMetaManager().getTable(tableName);

        String currentPrimaryKey = tableMeta.getPrimaryKeyColumn();
        if (currentPrimaryKey != null) {
            throw new DBException(ExceptionTypes.UnsupportedCommand(
                    "Table already has a primary key on column: " + currentPrimaryKey +
                            ". Please remove it first before adding a new primary key."));
        }

        checkForDuplicateValues(tableName, columnName);
        try {
            if (tableMeta.getIndexes() == null) {
                tableMeta.setIndexes(new java.util.HashMap<>());
            }
            tableMeta.getIndexes().put(columnName, TableMeta.IndexType.BTREE);
            dbManager.getIndexManager().createIndex(tableName, columnName);
            syncManagerStates(tableName);

            Logger.info("Successfully created B+Tree index for new primary key column {}", columnName);
        } catch (DBException e) {
            if (tableMeta.getIndexes() != null) {
                tableMeta.getIndexes().remove(columnName);
            }
            Logger.error("Failed to create B+Tree index for primary key column {}: {}", columnName, e.getMessage());
            throw new DBException(ExceptionTypes.UnsupportedCommand(
                    "Failed to create primary key constraint: " + e.getMessage()));
        }

        Logger.info("Successfully added primary key constraint to column {} in table {}", columnName, tableName);
    }

    /**
     * 取消主键
     */
    private void removePrimaryKey(String tableName, String columnName) throws DBException {
        TableMeta tableMeta = dbManager.getMetaManager().getTable(tableName);

        try {
            boolean indexDropped = dbManager.getIndexManager().dropIndex(tableName, columnName);
            if (indexDropped) {
                Logger.info("Successfully dropped B+Tree index for ex-primary key column {}", columnName);
            } else {
                Logger.warn("Index for column {} was not found or already dropped", columnName);
            }

            if (tableMeta.getIndexes() != null) {
                tableMeta.getIndexes().remove(columnName);
            }

            syncManagerStates(tableName);

            Logger.info("Successfully removed primary key constraint from column {}", columnName);
        } catch (DBException e) {
            Logger.error("Failed to remove B+Tree index for ex-primary key column {}: {}", columnName, e.getMessage());
            throw new DBException(ExceptionTypes.UnsupportedCommand(
                    "Failed to remove primary key constraint: " + e.getMessage()));
        }

        Logger.info("Successfully removed primary key constraint from column {} in table {}", columnName, tableName);
    }

    /**
     * 全表扫描检查重复值
     */
    private void checkForDuplicateValues(String tableName, String columnName) throws DBException {
        Logger.info("Scanning table {} for duplicate values in column {}", tableName, columnName);

        Set<Value> seenValues = new HashSet<>();
        SeqScanOperator scanner = new SeqScanOperator(tableName, dbManager);

        try {
            scanner.Begin();

            while (scanner.hasNext()) {
                scanner.Next();
                TableTuple tuple = (TableTuple) scanner.Current();

                if (tuple != null) {
                    Value value = tuple.getValue(new edu.sustech.cs307.meta.TabCol(tableName, columnName));

                    if (value != null) {
                        if (seenValues.contains(value)) {
                            throw new DBException(ExceptionTypes.PrimaryKeyViolation(
                                    tableName, columnName,
                                    "Duplicate value found: " + value.toString() +
                                            ". Cannot add primary key constraint on column with duplicate values."));
                        }
                        seenValues.add(value);
                    }
                }
            }
        } finally {
            scanner.Close();
        }

        Logger.info("No duplicate values found in column {}. Safe to add primary key constraint.", columnName);
    }

    /**
     * 重新计算列的偏移量
     */
    private void recalculateColumnOffsets(String tableName) throws DBException {
        TableMeta tableMeta = dbManager.getMetaManager().getTable(tableName);

        int offset = 0;
        for (ColumnMeta columnMeta : tableMeta.columns_list) {
            columnMeta.offset = offset;
            offset += columnMeta.getLen();
        }

        Logger.info("Recalculated column offsets for table {}", tableName);
    }

    /**
     * 同步更新所有管理器状态 - 确保 IO 相关上下文的完整同步
     */
    private void syncManagerStates(String tableName) throws DBException {
        try {
            dbManager.getMetaManager().saveToJson();
            dbManager.getBufferPool().FlushAllPages("");
            dbManager.getDiskManager().sync();

            IndexSynchronizer indexSynchronizer = new IndexSynchronizer(
                    dbManager.getIndexManager(),
                    dbManager.getMetaManager());

            try {
                indexSynchronizer.rebuildIndexesForTable(tableName);
                Logger.info("Successfully rebuilt all indexes for table {} after ALTER operation", tableName);
            } catch (DBException e) {
                Logger.warn("Failed to rebuild some indexes for table {}: {}", tableName, e.getMessage());
            }

            Logger.info("Successfully synchronized all manager states for table {}", tableName);
        } catch (DBException e) {
            Logger.error("Failed to sync manager states for table {}: {}", tableName, e.getMessage());
            throw new DBException(ExceptionTypes.BadIOError(
                    "Failed to synchronize system state after ALTER operation: " + e.getMessage()));
        }
    }
}