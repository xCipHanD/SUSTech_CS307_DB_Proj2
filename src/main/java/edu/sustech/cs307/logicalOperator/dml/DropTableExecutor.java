package edu.sustech.cs307.logicalOperator.dml;

import edu.sustech.cs307.exception.DBException;
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
        dbManager.dropTable(tableName);
        Logger.info("Successfully dropped table: {}", tableName);
    }
}
