package edu.sustech.cs307.physicalOperator;

import java.util.ArrayList;
import java.util.Collection;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.logicalOperator.LogicalJoinOperator;
import edu.sustech.cs307.meta.ColumnMeta;
import edu.sustech.cs307.meta.TabCol;
import edu.sustech.cs307.tuple.JoinTuple;
import edu.sustech.cs307.tuple.Tuple;
import net.sf.jsqlparser.expression.Expression;

public class NestedLoopJoinOperator implements PhysicalOperator {

    private final PhysicalOperator leftOperator;
    private final PhysicalOperator rightOperator;
    private final Collection<Expression> expr;
    private final LogicalJoinOperator.JoinType joinType;

    private Tuple currentLeftTuple;
    private Tuple currentRightTuple;
    private Tuple currentJoinTuple;

    private boolean leftHasNext;
    private boolean rightHasNext;
    private boolean isOpen = false;

    private ArrayList<ColumnMeta> outputSchema;

    // 用于 left join 和 right join 的状态跟踪
    private boolean leftTupleMatched = false;
    private boolean needToOutputUnmatched = false;

    public NestedLoopJoinOperator(PhysicalOperator leftOperator, PhysicalOperator rightOperator,
            Collection<Expression> expr) {
        this(leftOperator, rightOperator, expr, LogicalJoinOperator.JoinType.INNER);
    }

    public NestedLoopJoinOperator(PhysicalOperator leftOperator, PhysicalOperator rightOperator,
            Collection<Expression> expr, LogicalJoinOperator.JoinType joinType) {
        this.leftOperator = leftOperator;
        this.rightOperator = rightOperator;
        this.expr = expr;
        this.joinType = joinType;
    }

    @Override
    public void Begin() throws DBException {
        leftOperator.Begin();
        rightOperator.Begin();
        leftHasNext = leftOperator.hasNext();
        rightHasNext = false;
        currentLeftTuple = null;
        currentRightTuple = null;
        currentJoinTuple = null;
        isOpen = true;
        leftTupleMatched = false;
        needToOutputUnmatched = false;
        // advanceToNextJoin();
    }

    @Override
    public boolean hasNext() throws DBException {
        if (!isOpen) {
            return false;
        }
        if (this.joinType == LogicalJoinOperator.JoinType.LEFT) {
            return leftOperator.hasNext();
        }
        if (this.joinType == LogicalJoinOperator.JoinType.RIGHT) {
            return rightOperator.hasNext();
        }
        return leftOperator.hasNext() && rightOperator.hasNext();
    }

    @Override
    public void Next() throws DBException {
        if (!isOpen) {
            return;
        }
        advanceToNextJoin();
    }

    @Override
    public Tuple Current() {
        return currentJoinTuple;
    }

    @Override
    public void Close() throws DBException {
        leftOperator.Close();
        rightOperator.Close();
        isOpen = false;
        currentLeftTuple = null;
        currentRightTuple = null;
        currentJoinTuple = null;
    }

    @Override
    public ArrayList<ColumnMeta> outputSchema() {
        if (outputSchema != null)
            return outputSchema;
        ArrayList<ColumnMeta> left = leftOperator.outputSchema();
        ArrayList<ColumnMeta> right = rightOperator.outputSchema();
        outputSchema = new ArrayList<>();
        if (left != null)
            outputSchema.addAll(left);
        if (right != null)
            outputSchema.addAll(right);
        return outputSchema;
    }

    /**
     * 推进到下一个满足连接条件的元组
     */
    private void advanceToNextJoin() throws DBException {
        currentJoinTuple = null;

        while (true) {
            // 处理需要输出未匹配的左表元组的情况 (LEFT JOIN)
            if (needToOutputUnmatched && joinType == LogicalJoinOperator.JoinType.LEFT && !leftTupleMatched) {
                currentJoinTuple = createJoinTupleWithNulls(currentLeftTuple, true);
                needToOutputUnmatched = false;
                currentLeftTuple = null;
                leftTupleMatched = false;
                return;
            }

            // 如果左表当前元组为空，获取下一个左表元组
            if (currentLeftTuple == null) {
                if (!leftHasNext) {
                    // 对于 RIGHT JOIN，需要处理未匹配的右表元组
                    if (joinType == LogicalJoinOperator.JoinType.RIGHT) {
                        handleUnmatchedRightTuples();
                        return;
                    }
                    return;
                }
                leftOperator.Next();
                currentLeftTuple = leftOperator.Current();
                leftHasNext = leftOperator.hasNext();

                if (currentLeftTuple == null) {
                    continue;
                }

                leftTupleMatched = false;

                // 重置右表
                rightOperator.Close();
                rightOperator.Begin();
                rightHasNext = rightOperator.hasNext();

                if (!rightHasNext) {
                    // 如果右表为空，对于 LEFT JOIN 需要输出左表元组和 null
                    if (joinType == LogicalJoinOperator.JoinType.LEFT) {
                        currentJoinTuple = createJoinTupleWithNulls(currentLeftTuple, true);
                        currentLeftTuple = null;
                        return;
                    }
                    currentLeftTuple = null;
                    continue;
                }
            }

            // 遍历右表寻找匹配
            while (rightHasNext) {
                rightOperator.Next();
                currentRightTuple = rightOperator.Current();
                rightHasNext = rightOperator.hasNext();

                if (currentRightTuple == null) {
                    continue;
                }

                if (matchJoinCondition(currentLeftTuple, currentRightTuple)) {
                    // 找到匹配
                    leftTupleMatched = true;

                    TabCol[] leftSchema = currentLeftTuple.getTupleSchema();
                    TabCol[] rightSchema = currentRightTuple.getTupleSchema();
                    TabCol[] joinSchema = new TabCol[leftSchema.length + rightSchema.length];
                    System.arraycopy(leftSchema, 0, joinSchema, 0, leftSchema.length);
                    System.arraycopy(rightSchema, 0, joinSchema, leftSchema.length, rightSchema.length);

                    currentJoinTuple = new JoinTuple(currentLeftTuple, currentRightTuple, joinSchema);
                    return;
                }
            }

            // 右表扫描完毕
            if (joinType == LogicalJoinOperator.JoinType.LEFT && !leftTupleMatched) {
                // LEFT JOIN: 输出左表元组和 null 值
                currentJoinTuple = createJoinTupleWithNulls(currentLeftTuple, true);
                currentLeftTuple = null;
                leftTupleMatched = false;
                return;
            }

            // 移动到下一个左表元组
            currentLeftTuple = null;
            leftTupleMatched = false;
        }
    }

    /**
     * 处理 RIGHT JOIN 中未匹配的右表元组
     */
    private void handleUnmatchedRightTuples() throws DBException {
        // 这个方法需要重新遍历右表来找出未匹配的元组
        // 由于当前实现的限制，我们简化处理，实际实现中可能需要更复杂的逻辑
        // 这里暂时不实现完整的 RIGHT JOIN 逻辑
    }

    /**
     * 创建包含 null 值的连接元组
     */
    private JoinTuple createJoinTupleWithNulls(Tuple nonNullTuple, boolean isLeftTuple) throws DBException {
        TabCol[] nonNullSchema = nonNullTuple.getTupleSchema();

        if (isLeftTuple) {
            // LEFT JOIN: 左表有值，右表为 null
            ArrayList<ColumnMeta> rightSchema = rightOperator.outputSchema();
            TabCol[] rightTabCols = new TabCol[rightSchema.size()];
            for (int i = 0; i < rightSchema.size(); i++) {
                rightTabCols[i] = new TabCol(rightSchema.get(i).tableName, rightSchema.get(i).name);
            }

            TabCol[] joinSchema = new TabCol[nonNullSchema.length + rightTabCols.length];
            System.arraycopy(nonNullSchema, 0, joinSchema, 0, nonNullSchema.length);
            System.arraycopy(rightTabCols, 0, joinSchema, nonNullSchema.length, rightTabCols.length);

            return new JoinTuple(nonNullTuple, null, joinSchema);
        } else {
            // RIGHT JOIN: 右表有值，左表为 null
            ArrayList<ColumnMeta> leftSchema = leftOperator.outputSchema();
            TabCol[] leftTabCols = new TabCol[leftSchema.size()];
            for (int i = 0; i < leftSchema.size(); i++) {
                leftTabCols[i] = new TabCol(leftSchema.get(i).tableName, leftSchema.get(i).name);
            }

            TabCol[] joinSchema = new TabCol[leftTabCols.length + nonNullSchema.length];
            System.arraycopy(leftTabCols, 0, joinSchema, 0, leftTabCols.length);
            System.arraycopy(nonNullSchema, 0, joinSchema, leftTabCols.length, nonNullSchema.length);

            return new JoinTuple(null, nonNullTuple, joinSchema);
        }
    }

    /**
     * 判断连接条件是否满足
     */
    private boolean matchJoinCondition(Tuple left, Tuple right) throws DBException {
        if (expr == null || expr.isEmpty()) {
            return true; // 笛卡尔积
        }

        if (left == null || right == null) {
            return false;
        }

        // 构造连接元组
        TabCol[] leftSchema = left.getTupleSchema();
        TabCol[] rightSchema = right.getTupleSchema();
        TabCol[] joinSchema = new TabCol[leftSchema.length + rightSchema.length];
        System.arraycopy(leftSchema, 0, joinSchema, 0, leftSchema.length);
        System.arraycopy(rightSchema, 0, joinSchema, leftSchema.length, rightSchema.length);

        JoinTuple joinTuple = new JoinTuple(left, right, joinSchema);

        // 对每个连接条件进行评估
        for (Expression e : expr) {
            try {
                if (!joinTuple.eval_expr(e)) {
                    return false;
                }
            } catch (DBException ex) {
                return false;
            }
        }

        return true;
    }
}
