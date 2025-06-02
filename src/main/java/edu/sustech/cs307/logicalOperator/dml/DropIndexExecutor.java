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
        // 获取索引名
        String indexName = dropIndexStmt.getName().getName();

        Logger.info("Dropping index {}", indexName);

        // 查找包含该索引的表和列
        String foundTableName = null;
        String foundColumnName = null;

        // 首先通过索引名称映射查找
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

        // 如果通过索引名称没有找到，尝试向后兼容的方式
        if (foundTableName == null) {
            Logger.debug("Index name '{}' not found in index name mappings, trying backward compatibility", indexName);

            // 遍历所有表查找可能的索引（向后兼容）
            for (String tableName : dbManager.getMetaManager().getTableNames()) {
                try {
                    TableMeta tableMeta = dbManager.getMetaManager().getTable(tableName);
                    if (tableMeta.getIndexes() != null) {
                        for (String columnName : tableMeta.getIndexes().keySet()) {
                            // 检查是否存在对应的索引实例
                            Index index = dbManager.getIndexManager().getIndex(tableName, columnName);
                            if (index != null) {
                                // 尝试多种索引名匹配模式
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

        // 如果仍然没有找到，抛出异常
        if (foundTableName == null || foundColumnName == null) {
            throw new DBException(ExceptionTypes.UnsupportedCommand(
                    String.format("Index '%s' not found", indexName)));
        }

        Logger.info("Found index {} on table {}.{}", indexName, foundTableName, foundColumnName);

        try {
            // 获取表元数据
            TableMeta tableMeta = dbManager.getMetaManager().getTable(foundTableName);

            // 删除实际的索引实例，确保完整的 B+Tree 生命周期管理
            boolean indexRemoved = dbManager.getIndexManager().dropIndex(foundTableName, foundColumnName);

            if (!indexRemoved) {
                Logger.warn("Index on {}.{} was not found in IndexManager", foundTableName, foundColumnName);
            }

            // 从元数据中删除索引定义（包括索引名称映射），确保 IO 数据一致性
            String removedColumn = tableMeta.removeIndexByName(indexName);
            if (removedColumn == null) {
                // 如果索引名称映射中没有找到，使用传统方式删除
                if (tableMeta.getIndexes() != null && tableMeta.getIndexes().containsKey(foundColumnName)) {
                    tableMeta.getIndexes().remove(foundColumnName);
                }
            }

            // 保存元数据到 JSON 文件
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