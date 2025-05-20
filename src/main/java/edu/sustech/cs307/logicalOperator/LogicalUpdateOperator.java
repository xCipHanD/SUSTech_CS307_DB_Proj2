package edu.sustech.cs307.logicalOperator;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.update.UpdateSet;

import java.util.Collections;
import java.util.List;

public class LogicalUpdateOperator extends LogicalOperator {
    private final String tableName;
    private final List<UpdateSet> updateSetList;
    private final Expression expressions;

    public LogicalUpdateOperator(LogicalOperator child, String tableName, List<UpdateSet> updateSets,
            Expression expressions) {
        super(Collections.singletonList(child));
        this.tableName = tableName;
        this.updateSetList = updateSets;
        this.expressions = expressions;
    }

    public String getTableName() {
        return tableName;
    }

    public List<UpdateSet> getUpdateSets() {
        return updateSetList;
    }

    public Expression getExpression() {
        return expressions;
    }

    @Override
    public String toString() {
        return "UpdateOperator(table=" + tableName + ", columns=" + updateSetList + ", expressions=" + expressions
                + ")\n ├── " + childern.get(0);
    }
}
