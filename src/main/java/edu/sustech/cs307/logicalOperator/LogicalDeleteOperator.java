package edu.sustech.cs307.logicalOperator;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.exception.ExceptionTypes;
import edu.sustech.cs307.system.DBManager;
import net.sf.jsqlparser.expression.Expression;
import java.util.Collections;

public class LogicalDeleteOperator extends LogicalOperator {
    private final String tableName;
    private final DBManager dbManager;
    private final Expression whereExpr;

    public LogicalDeleteOperator(LogicalOperator input, String tableName, Expression whereExpr,
            DBManager dbManager) throws DBException {
        super(Collections.singletonList(input)); // Single input operator (e.g., TableScan or Filter)
        this.tableName = tableName;
        this.dbManager = dbManager;
        this.whereExpr = whereExpr;
        if (!dbManager.isTableExists(tableName)) {
            throw new DBException(ExceptionTypes.TableDoesNotExist(tableName));
        }
    }

    public String getTableName() {
        return tableName;
    }

    public Expression getWhereExpr() {
        return whereExpr;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        String conditionStr = whereExpr != null ? ", condition=" + whereExpr : "";
        String nodeHeader = "LogicalDeleteOperator(table=" + tableName + conditionStr + ")";

        sb.append(nodeHeader);
        if (!childern.isEmpty()) {
            LogicalOperator child = childern.get(0);
            String childStr = child.toString();
            String[] childLines = childStr.split("\\R");

            if (childLines.length > 0) {
                sb.append("\n└── ").append(childLines[0]);
                for (int i = 1; i < childLines.length; i++) {
                    sb.append("\n\t").append(childLines[i]);
                }
            }
        }

        return sb.toString();
    }
}