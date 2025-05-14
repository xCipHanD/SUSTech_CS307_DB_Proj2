package edu.sustech.cs307.logicalOperator.dml;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.exception.ExceptionTypes;
import edu.sustech.cs307.storage.DiskManager;
import edu.sustech.cs307.system.DBManager;
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
}
