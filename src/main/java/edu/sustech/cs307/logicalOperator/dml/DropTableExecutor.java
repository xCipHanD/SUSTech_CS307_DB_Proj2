package edu.sustech.cs307.logicalOperator.dml;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.exception.ExceptionTypes;
import edu.sustech.cs307.storage.DiskManager;
import edu.sustech.cs307.system.DBManager;
import edu.sustech.cs307.meta.TableMeta;
import net.sf.jsqlparser.statement.drop.Drop;
import org.pmw.tinylog.Logger;

public class DropTableExecutor implements DMLExecutor {
    private final Drop dropTableStmt;
    private final DBManager dbManager;

    public DropTableExecutor(Drop dropTableStmt, DBManager dbManager) {
        this.dropTableStmt = dropTableStmt;
        this.dbManager = dbManager;
    }

    @Override
    public void execute() throws DBException {
        String tableName = dropTableStmt.getName().getName();
        if (!dbManager.isTableExists(tableName)) {
            throw new DBException(ExceptionTypes.TableDoesNotExist(tableName));
        }
        String dataFileName = tableName + "/data";

        try {
            // 新增：在删除表之前清理所有相关索引
            cleanupTableIndexes(tableName);
            
            dbManager.getBufferPool().DeleteAllPages(dataFileName);
            dbManager.dropTable(tableName);
            dbManager.getDiskManager().filePages.remove(dataFileName);
            DiskManager.dump_disk_manager_meta(dbManager.getDiskManager());
            Logger.info("Successfully dropped table: {}", tableName);
        } catch (DBException e) {
            Logger.error("Failed to drop table {}: {}", tableName, e.getMessage());
            throw e;
        }
    }
    
    /**
     * 清理表的所有索引
     */
    private void cleanupTableIndexes(String tableName) throws DBException {
        try {
            TableMeta tableMeta = dbManager.getMetaManager().getTable(tableName);
            if (tableMeta != null && tableMeta.getIndexes() != null) {
                // 清理所有列的索引
                for (String columnName : tableMeta.getIndexes().keySet()) {
                    if (dbManager.getIndexManager() != null) {
                        boolean dropped = dbManager.getIndexManager().dropIndex(tableName, columnName);
                        if (dropped) {
                            Logger.info("Cleaned up index for {}.{}", tableName, columnName);
                        }
                    }
                }
            }
        } catch (DBException e) {
            Logger.warn("Failed to cleanup indexes for table {}: {}", tableName, e.getMessage());
            // 继续执行删除表操作，即使索引清理失败
        }
    }
}
