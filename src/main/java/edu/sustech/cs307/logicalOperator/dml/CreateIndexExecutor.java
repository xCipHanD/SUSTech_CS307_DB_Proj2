package edu.sustech.cs307.logicalOperator.dml;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.exception.ExceptionTypes;
import edu.sustech.cs307.index.Index;
import edu.sustech.cs307.meta.TableMeta;
import edu.sustech.cs307.system.DBManager;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import org.pmw.tinylog.Logger;

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
        // 获取索引名、表名和列名
        String indexName = createIndexStmt.getIndex().getName();
        String tableName = createIndexStmt.getTable().getName();

        // 获取列名列表
        if (createIndexStmt.getIndex().getColumnsNames() == null ||
                createIndexStmt.getIndex().getColumnsNames().isEmpty()) {
            throw new DBException(ExceptionTypes.InvalidSQL("CREATE INDEX",
                    "No columns specified for index"));
        }

        // 当前实现只支持单列索引
        if (createIndexStmt.getIndex().getColumnsNames().size() > 1) {
            throw new DBException(ExceptionTypes.UnsupportedCommand(
                    "Multi-column indexes are not supported yet"));
        }

        String columnName = createIndexStmt.getIndex().getColumnsNames().get(0);

        Logger.info("Creating index {} on table {}.{}", indexName, tableName, columnName);

        // 检查表是否存在
        if (!dbManager.isTableExists(tableName)) {
            throw new DBException(ExceptionTypes.TableDoesNotExist(tableName));
        }

        try {
            // 获取表元数据
            TableMeta tableMeta = dbManager.getMetaManager().getTable(tableName);

            // 检查列是否存在
            if (!tableMeta.hasColumn(columnName)) {
                throw new DBException(ExceptionTypes.ColumnDoesNotExist(columnName));
            }

            // 检查索引名称是否已经存在
            if (tableMeta.hasIndexName(indexName)) {
                throw new DBException(ExceptionTypes.UnsupportedCommand(
                        "Index name '" + indexName + "' already exists"));
            }

            // 检查列上是否已经存在索引
            Index existingIndex = dbManager.getIndexManager().getIndex(tableName, columnName);
            if (existingIndex != null) {
                throw new DBException(ExceptionTypes.UnsupportedCommand(
                        "Index on column '" + columnName + "' already exists"));
            }

            // 在元数据中添加索引定义（包括索引名称）
            tableMeta.addIndex(indexName, columnName, TableMeta.IndexType.BTREE);

            // 保存元数据到 JSON 文件，确保 IO 数据一致性
            dbManager.getMetaManager().saveToJson();
            Logger.debug("Updated metadata for index {} on {}.{}", indexName, tableName, columnName);

            // 创建实际的 B+Tree 索引，确保完整的生命周期管理
            Index index = dbManager.getIndexManager().createIndex(tableName, columnName);

            Logger.info("Successfully created index {} on table {}.{}",
                    indexName, tableName, columnName);

        } catch (DBException e) {
            Logger.error("Failed to create index {} on table {}.{}: {}",
                    indexName, tableName, columnName, e.getMessage());

            // 如果索引创建失败，尝试清理元数据以保持一致性
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
}