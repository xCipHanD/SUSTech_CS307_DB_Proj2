package edu.sustech.cs307.logicalOperator.dml;

import edu.sustech.cs307.system.DBManager;
import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.optimizer.LogicalPlanner;
import edu.sustech.cs307.logicalOperator.LogicalOperator;

import org.pmw.tinylog.Logger;

public class ExplainExecutor implements DMLExecutor {

    private final String sql;
    private final DBManager dbManager;

    public ExplainExecutor(DBManager dbManager, String sql) {
        this.dbManager = dbManager;
        this.sql = sql.strip().replaceFirst("(?i)^explain", "");
    }

    @Override
    public void execute() throws DBException {
        LogicalOperator logicalOperator = LogicalPlanner.resolveAndPlan(dbManager, sql);
        Logger.info(logicalOperator.toString());
    }
}
