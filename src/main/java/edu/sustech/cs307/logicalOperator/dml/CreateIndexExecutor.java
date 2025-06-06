package edu.sustech.cs307.logicalOperator.dml;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.exception.ExceptionTypes;
import edu.sustech.cs307.index.Index;
import edu.sustech.cs307.meta.TableMeta;
import edu.sustech.cs307.physicalOperator.SeqScanOperator;
import edu.sustech.cs307.system.DBManager;
import edu.sustech.cs307.tuple.TableTuple;
import edu.sustech.cs307.value.Value;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import org.pmw.tinylog.Logger;

import java.util.HashSet;
import java.util.Set;

/**
 * 执行器用于处理 CREATE INDEX 语句
 */
public class CreateIndexExecutor implements DMLExecutor {
    private final CreateIndex createIndexStmt;
    private final DBManager dbManager;

    public CreateIndexExecutor(CreateIndex createIndexStmt, DBManager dbManager) {
        this.createIndexStmt = createIndexStmt;
        this.dbManager = dbManager;
    }

    @Override
    public void execute() throws DBException {
        String indexName = createIndexStmt.getIndex().getName();
        String tableName = createIndexStmt.getTable().getName();
        if (createIndexStmt.getIndex().getColumnsNames() == null ||
                createIndexStmt.getIndex().getColumnsNames().isEmpty()) {
            throw new DBException(ExceptionTypes.InvalidSQL("CREATE INDEX",
                    "No columns specified for index"));
        }

        if (createIndexStmt.getIndex().getColumnsNames().size() > 1) {
            throw new DBException(ExceptionTypes.UnsupportedCommand(
                    "Multi-column indexes are not supported yet"));
        }

        String columnName = createIndexStmt.getIndex().getColumnsNames().get(0);

        Logger.info("Creating index {} on table {}.{}", indexName, tableName, columnName);

        if (!dbManager.isTableExists(tableName)) {
            throw new DBException(ExceptionTypes.TableDoesNotExist(tableName));
        }

        try {
            TableMeta tableMeta = dbManager.getMetaManager().getTable(tableName);
            if (!tableMeta.hasColumn(columnName)) {
                throw new DBException(ExceptionTypes.ColumnDoesNotExist(columnName));
            }

            if (tableMeta.hasIndexName(indexName)) {
                throw new DBException(ExceptionTypes.UnsupportedCommand(
                        "Index name '" + indexName + "' already exists"));
            }

            Index existingIndex = dbManager.getIndexManager().getIndex(tableName, columnName);
            if (existingIndex != null) {
                throw new DBException(ExceptionTypes.UnsupportedCommand(
                        "Index on column '" + columnName + "' already exists"));
            }

            boolean isUniqueIndex = isUniqueIndexRequired(tableMeta, columnName);

            if (isUniqueIndex) {
                checkForDuplicateValues(tableName, columnName);
            } else {
                Logger.info("Creating non-unique index on column {}, skipping duplicate value check", columnName);
            }

            tableMeta.addIndex(indexName, columnName, TableMeta.IndexType.BTREE);

            dbManager.getMetaManager().saveToJson();
            Logger.debug("Updated metadata for index {} on {}.{}", indexName, tableName, columnName);

            Index index = dbManager.getIndexManager().createIndex(tableName, columnName);

            Logger.info("Successfully created index {} on table {}.{}",
                    indexName, tableName, columnName);

        } catch (DBException e) {
            Logger.error("Failed to create index {} on table {}.{}: {}",
                    indexName, tableName, columnName, e.getMessage());

            try {
                TableMeta tableMeta = dbManager.getMetaManager().getTable(tableName);
                tableMeta.removeIndexByName(indexName);
                dbManager.getMetaManager().saveToJson();
                Logger.debug("Cleaned up metadata after failed index creation");
            } catch (Exception cleanupException) {
                Logger.warn("Failed to cleanup metadata after index creation failure: {}",
                        cleanupException.getMessage());
            }

            throw e;
        }
    }

    /**
     * 判断是否需要唯一索引
     * 如果是主键列或明确指定为唯一索引，则需要检查重复值
     */
    private boolean isUniqueIndexRequired(TableMeta tableMeta, String columnName) {
        String primaryKeyColumn = tableMeta.getPrimaryKeyColumn();
        if (columnName.equals(primaryKeyColumn)) {
            Logger.info("Creating index on primary key column {}, will check for duplicate values", columnName);
            return true;
        }

        Logger.info(
                "Creating index on non-primary key column {}, will check for duplicate values to ensure data consistency",
                columnName);
        return true;
    }

    /**
     * 全表扫描检查重复值
     * 参考 AlterTableExecutor.checkForDuplicateValues 的实现
     */
    private void checkForDuplicateValues(String tableName, String columnName) throws DBException {
        Logger.info("Scanning table {} for duplicate values in column {} before creating index", tableName, columnName);

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
                            throw new DBException(ExceptionTypes.UnsupportedCommand(
                                    "Cannot create index on column '" + columnName +
                                            "' - duplicate value found: " + value.toString() +
                                            ". Please ensure all values in the column are unique before creating the index."));
                        }
                        seenValues.add(value);
                    }
                }
            }
        } finally {
            scanner.Close();
        }

        Logger.info("No duplicate values found in column {}. Safe to create index.", columnName);
    }
}