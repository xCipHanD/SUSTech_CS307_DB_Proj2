package edu.sustech.cs307.logicalOperator;

import net.sf.jsqlparser.statement.select.OrderByElement;

import java.util.Collections;
import java.util.List;

/**
 * LogicalOrderByOperator represents an ORDER BY operation in the logical query
 * plan.
 */
public class LogicalOrderByOperator extends LogicalOperator {
    private final List<OrderByElement> orderByElements;

    public LogicalOrderByOperator(LogicalOperator child, List<OrderByElement> orderByElements) {
        super(Collections.singletonList(child));
        this.orderByElements = orderByElements;
    }

    public List<OrderByElement> getOrderByElements() {
        return orderByElements;
    }

    public LogicalOperator getChild() {
        return childern.get(0);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("LogicalOrderByOperator(orderBy=").append(orderByElements).append(")");

        // 递归显示子操作符
        if (!childern.isEmpty()) {
            LogicalOperator child = childern.get(0);
            String childStr = child.toString();
            String[] childLines = childStr.split("\n");

            for (int i = 0; i < childLines.length; i++) {
                sb.append("\n");
                if (i == 0) {
                    sb.append("└── ");
                } else {
                    sb.append("    ");
                }
                sb.append(childLines[i]);
            }
        }

        return sb.toString();
    }
}