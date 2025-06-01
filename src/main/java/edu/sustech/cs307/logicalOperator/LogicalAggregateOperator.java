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
        sb.append("LogicalAggregateOperator(");

        // 详细显示聚合表达式
        if (aggregateExpressions != null && !aggregateExpressions.isEmpty()) {
            sb.append("aggregates=[");
            for (int i = 0; i < aggregateExpressions.size(); i++) {
                SelectItem<?> item = aggregateExpressions.get(i);
                if (i > 0)
                    sb.append(", ");

                if (item.getAlias() != null) {
                    sb.append(item.getExpression()).append(" AS ").append(item.getAlias().getName());
                } else {
                    sb.append(item.getExpression());
                }
            }
            sb.append("]");
        }

        // 详细显示分组表达式
        if (groupByExpressions != null && !groupByExpressions.isEmpty()) {
            if (aggregateExpressions != null && !aggregateExpressions.isEmpty()) {
                sb.append(", ");
            }
            sb.append("groupBy=[");
            for (int i = 0; i < groupByExpressions.size(); i++) {
                if (i > 0)
                    sb.append(", ");
                sb.append(groupByExpressions.get(i));
            }
            sb.append("]");
        }

        sb.append(")");

        // 递归显示子操作符，保持与其他操作符一致的树状格式
        if (child != null) {
            String[] childLines = child.toString().split("\\R");
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
