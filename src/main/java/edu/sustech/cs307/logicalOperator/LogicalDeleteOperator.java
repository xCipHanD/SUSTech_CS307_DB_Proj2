package edu.sustech.cs307.logicalOperator;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.exception.ExceptionTypes;
import edu.sustech.cs307.system.DBManager;
import net.sf.jsqlparser.expression.Expression;

import java.util.Collections;

public class LogicalDeleteOperator extends LogicalOperator {
    private final String tableName;
    private final DBManager dbManager;
    private final Expression whereCondition;

    public LogicalDeleteOperator(LogicalOperator input, String tableName, Expression whereCondition,
            DBManager dbManager) throws DBException {
        super(Collections.singletonList(input)); // Single input operator (e.g., TableScan or Filter)
        this.tableName = tableName;
        this.dbManager = dbManager; // Reuse DBManager from input operator
        this.whereCondition = whereCondition;
        if (!dbManager.isTableExists(tableName)) {
            throw new DBException(ExceptionTypes.TableDoesNotExist(tableName));
        }
    }

    public String getTableName() {
        return tableName;
    }

    public Expression getWhereCondition() {
        return whereCondition;
    }

    @Override
    public String toString() {
        String conditionStr = whereCondition != null ? ", where=" + whereCondition : "";
        return "DeleteOperator(table=" + tableName + conditionStr + ")";
    }
}
// This class represents a logical delete operation in a query execution plan.(Not secure to be right)