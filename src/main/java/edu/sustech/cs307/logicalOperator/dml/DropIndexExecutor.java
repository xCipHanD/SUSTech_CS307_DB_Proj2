package edu.sustech.cs307.logicalOperator.dml;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.exception.ExceptionTypes;
import edu.sustech.cs307.index.Index;
import edu.sustech.cs307.meta.TableMeta;
import edu.sustech.cs307.system.DBManager;
import net.sf.jsqlparser.statement.drop.Drop;
import org.pmw.tinylog.Logger;

/**
 * 执行器用于处理 DROP INDEX 语句
 */
public class DropIndexExecutor implements DMLExecutor {
    private final Drop dropIndexStmt;
    private final DBManager dbManager;

    public DropIndexExecutor(Drop dropIndexStmt, DBManager dbManager) {
        this.dropIndexStmt = dropIndexStmt;
        this.dbManager = dbManager;
    }

    @Override
    public void execute() throws DBException {
        String indexName = dropIndexStmt.getName().getName();

        Logger.info("Dropping index {}", indexName);

        String foundTableName = null;
        String foundColumnName = null;

        for (String tableName : dbManager.getMetaManager().getTableNames()) {
            try {
                TableMeta tableMeta = dbManager.getMetaManager().getTable(tableName);
                if (tableMeta.hasIndexName(indexName)) {
                    foundTableName = tableName;
                    foundColumnName = tableMeta.getColumnByIndexName(indexName);
                    break;
                }
            } catch (DBException e) {
                Logger.warn("Error checking table {} for index {}: {}", tableName, indexName, e.getMessage());
                continue;
            }
        }

        if (foundTableName == null) {
            Logger.debug("Index name '{}' not found in index name mappings, trying backward compatibility", indexName);

            for (String tableName : dbManager.getMetaManager().getTableNames()) {
                try {
                    TableMeta tableMeta = dbManager.getMetaManager().getTable(tableName);
                    if (tableMeta.getIndexes() != null) {
                        for (String columnName : tableMeta.getIndexes().keySet()) {
                            Index index = dbManager.getIndexManager().getIndex(tableName, columnName);
                            if (index != null) {
                                String implicitIndexName = "idx_" + tableName + "_" + columnName;
                                if (indexName.equals(implicitIndexName) ||
                                        indexName.equals(columnName) ||
                                        indexName.equals(tableName + "_" + columnName)) {
                                    foundTableName = tableName;
                                    foundColumnName = columnName;
                                    break;
                                }
                            }
                        }
                    }
                    if (foundTableName != null) {
                        break;
                    }
                } catch (DBException e) {
                    Logger.warn("Error checking table {} for index {}: {}", tableName, indexName, e.getMessage());
                    continue;
                }
            }
        }

        if (foundTableName == null || foundColumnName == null) {
            throw new DBException(ExceptionTypes.UnsupportedCommand(
                    String.format("Index '%s' not found", indexName)));
        }

        Logger.info("Found index {} on table {}.{}", indexName, foundTableName, foundColumnName);

        try {
            TableMeta tableMeta = dbManager.getMetaManager().getTable(foundTableName);

            boolean indexRemoved = dbManager.getIndexManager().dropIndex(foundTableName, foundColumnName);

            if (!indexRemoved) {
                Logger.warn("Index on {}.{} was not found in IndexManager", foundTableName, foundColumnName);
            }

            String removedColumn = tableMeta.removeIndexByName(indexName);
            if (removedColumn == null) {
                if (tableMeta.getIndexes() != null && tableMeta.getIndexes().containsKey(foundColumnName)) {
                    tableMeta.getIndexes().remove(foundColumnName);
                }
            }

            dbManager.getMetaManager().saveToJson();
            Logger.debug("Removed index metadata for {} (table: {}, column: {})", indexName, foundTableName,
                    foundColumnName);

            Logger.info("Successfully dropped index {} (table: {}, column: {})",
                    indexName, foundTableName, foundColumnName);

        } catch (DBException e) {
            Logger.error("Failed to drop index {}: {}", indexName, e.getMessage());
            throw e;
        }
    }
}