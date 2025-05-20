package edu.sustech.cs307.logicalOperator;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.List;
import java.util.Collections;

public class LogicalAggregateOperator extends LogicalOperator {
    private final List<SelectItem<?>> aggregateExpressions;
    private final List<Expression> groupByExpressions;
    private final LogicalOperator child;

    public LogicalAggregateOperator(LogicalOperator child, List<SelectItem<?>> aggregateExpressions,
            List<Expression> groupByExpressions) {
        super(Collections.singletonList(child));
        this.child = child;
        this.aggregateExpressions = aggregateExpressions;
        this.groupByExpressions = groupByExpressions;
    }

    public List<SelectItem<?>> getAggregateExpressions() {
        return aggregateExpressions;
    }

    public List<Expression> getGroupByExpressions() {
        return groupByExpressions;
    }

    public LogicalOperator getChild() {
        return child;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("LogicalAggregateOperator(aggregates=").append(aggregateExpressions);
        if (groupByExpressions != null && !groupByExpressions.isEmpty()) {
            sb.append(", groupBy=").append(groupByExpressions);
        }
        sb.append(")");

        if (child != null) {
            String[] childLines = child.toString().split("\\R");
            if (childLines.length > 0) {
                sb.append("\n└── ").append(childLines[0]);
                for (int i = 1; i < childLines.length; i++) {
                    sb.append("\n    ").append(childLines[i]);
                }
            }
        }
        return sb.toString();
    }
}
