package edu.sustech.cs307.logicalOperator.dml;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.system.DBManager;
import net.sf.jsqlparser.statement.show.ShowTablesStatement;

public class ShowTablesExecutor implements DMLExecutor {
    private final DBManager dbManager;

    public ShowTablesExecutor(ShowTablesStatement showStatement, DBManager dbManager) {
        this.dbManager = dbManager;
    }

    @Override
    public void execute() throws DBException {
        dbManager.showTables();
    }

}
