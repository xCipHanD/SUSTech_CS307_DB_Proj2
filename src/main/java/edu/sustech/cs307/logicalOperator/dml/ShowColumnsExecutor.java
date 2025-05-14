package edu.sustech.cs307.logicalOperator.dml;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.exception.ExceptionTypes;
import edu.sustech.cs307.system.DBManager;
import net.sf.jsqlparser.statement.DescribeStatement;
import net.sf.jsqlparser.statement.ShowColumnsStatement;

public class ShowColumnsExecutor implements DMLExecutor {
    private final String tableName;
    private final DBManager dbManager;

    public ShowColumnsExecutor(ShowColumnsStatement showStatement, DBManager dbManager) throws DBException {
        this.tableName = showStatement.getTableName();
        if (tableName == null) {
            throw new DBException(ExceptionTypes.TableDoesNotExist(tableName));
        }
        this.dbManager = dbManager;
    }

    public ShowColumnsExecutor(DescribeStatement describeStatement, DBManager dbManager) throws DBException {
        this.tableName = describeStatement.getTable().getName();
        if (tableName == null) {
            throw new DBException(ExceptionTypes.TableDoesNotExist(tableName));
        }
        this.dbManager = dbManager;
    }

    @Override
    public void execute() throws DBException {
        dbManager.descTable(tableName);
    }
}
