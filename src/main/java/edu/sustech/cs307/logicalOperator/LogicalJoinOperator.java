package edu.sustech.cs307.logicalOperator;

import net.sf.jsqlparser.expression.Expression;

import java.util.Arrays;
import java.util.Collection;

public class LogicalJoinOperator extends LogicalOperator {

    // 定义 JOIN 类型枚举
    public enum JoinType {
        INNER,
        LEFT,
        RIGHT,
        CROSS
    }

    private final Collection<Expression> onExpressions;
    private final LogicalOperator leftInput;
    private final LogicalOperator rightInput;
    private final JoinType joinType;

    public LogicalJoinOperator(LogicalOperator left, LogicalOperator right,
            Collection<Expression> onExpr, JoinType joinType,
            int depth) {
        super(Arrays.asList(left, right));
        this.leftInput = left;
        this.rightInput = right;
        this.onExpressions = onExpr;
        this.joinType = joinType;
    }

    // 兼容性构造函数，默认为 INNER JOIN
    public LogicalJoinOperator(LogicalOperator left, LogicalOperator right,
            Collection<Expression> onExpr,
            int depth) {
        this(left, right, onExpr, JoinType.INNER, depth);
    }

    public LogicalOperator getLeftInput() {
        return leftInput;
    }

    public LogicalOperator getRightInput() {
        return rightInput;
    }

    public Collection<Expression> getJoinExprs() {
        return onExpressions;
    }

    public JoinType getJoinType() {
        return joinType;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        String nodeHeader = "LogicalJoinOperator(type=" + joinType + ", condition=" + onExpressions + ")";
        String[] leftLines = leftInput.toString().split("\\R");
        String[] rightLines = rightInput.toString().split("\\R");

        sb.append(nodeHeader);

        if (leftLines.length > 0) {
            sb.append("\n├── ").append(leftLines[0]);
            for (int i = 1; i < leftLines.length; i++) {
                sb.append("\n│   ").append(leftLines[i]);
            }
        }

        if (rightLines.length > 0) {
            sb.append("\n└── ").append(rightLines[0]);
            for (int i = 1; i < rightLines.length; i++) {
                sb.append("\n\t").append(rightLines[i]);
            }
        }

        return sb.toString();
    }
}
