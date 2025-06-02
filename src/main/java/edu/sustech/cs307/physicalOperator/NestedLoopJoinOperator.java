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

    // 实际使用的操作符（可能交换了左右顺序）
    private final PhysicalOperator outerOperator;
    private final PhysicalOperator innerOperator;
    private final boolean swapped; // 是否交换了左右表

    private Tuple currentOuterTuple;
    private Tuple currentInnerTuple;
    private Tuple currentJoinTuple;
    private Tuple lastJoinTuple;

    private boolean outerHasNext;
    private boolean innerHasNext;
    private boolean isOpen = false;

    private ArrayList<ColumnMeta> outputSchema;

    // 用于 LEFT/RIGHT JOIN 的状态跟踪
    private boolean outerTupleMatched = false;

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

        // 对于 RIGHT JOIN，我们交换左右表的角色，将其转换为 LEFT JOIN
        if (joinType == LogicalJoinOperator.JoinType.RIGHT) {
            this.outerOperator = rightOperator; // 右表作为外表
            this.innerOperator = leftOperator; // 左表作为内表
            this.swapped = true;
        } else {
            this.outerOperator = leftOperator; // 左表作为外表
            this.innerOperator = rightOperator; // 右表作为内表
            this.swapped = false;
        }
    }

    @Override
    public void Begin() throws DBException {
        leftOperator.Begin();
        rightOperator.Begin();
        outerHasNext = outerOperator.hasNext();
        innerHasNext = false;
        currentOuterTuple = null;
        currentInnerTuple = null;
        currentJoinTuple = null;
        isOpen = true;
        outerTupleMatched = false;
        advanceToNextJoin();
    }

    @Override
    public boolean hasNext() throws DBException {
        if (!isOpen) {
            return false;
        }
        return currentJoinTuple != null;
    }

    @Override
    public void Next() throws DBException {
        if (!isOpen)
            return;
        lastJoinTuple = currentJoinTuple;
        // Next() 方法负责推进到下一个结果
        advanceToNextJoin();
    }

    @Override
    public Tuple Current() {
        return lastJoinTuple;
    }

    @Override
    public void Close() throws DBException {
        leftOperator.Close();
        rightOperator.Close();
        isOpen = false;
        currentOuterTuple = null;
        currentInnerTuple = null;
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
            // 如果外表当前元组为空，获取下一个外表元组
            if (currentOuterTuple == null) {
                if (!outerHasNext) {
                    return; // 没有更多数据
                }
                outerOperator.Next();
                currentOuterTuple = outerOperator.Current();
                outerHasNext = outerOperator.hasNext();

                if (currentOuterTuple == null) {
                    continue;
                }

                outerTupleMatched = false;

                // 重置内表
                innerOperator.Close();
                innerOperator.Begin();
                innerHasNext = innerOperator.hasNext();

                if (!innerHasNext) {
                    // 如果内表为空，对于 LEFT/RIGHT JOIN 需要输出外表元组和 null
                    if (isOuterJoin()) {
                        currentJoinTuple = createJoinTupleWithNulls(currentOuterTuple);
                        currentOuterTuple = null;
                        return;
                    }
                    // 对于 CROSS JOIN，如果内表为空，跳过当前外表元组
                    if (joinType == LogicalJoinOperator.JoinType.CROSS) {
                        currentOuterTuple = null;
                        continue;
                    }
                    currentOuterTuple = null;
                    continue;
                }
            }

            // 遍历内表寻找匹配
            if (joinType == LogicalJoinOperator.JoinType.CROSS) {
                // 对于 CROSS JOIN，直接连接每一对元组，不检查连接条件
                while (innerHasNext) {
                    innerOperator.Next();
                    currentInnerTuple = innerOperator.Current();
                    innerHasNext = innerOperator.hasNext();

                    if (currentInnerTuple != null) {
                        // CROSS JOIN 总是产生连接结果，不需要检查条件
                        currentJoinTuple = createJoinTuple(currentOuterTuple, currentInnerTuple);
                        return;
                    }
                }
            } else {
                // 原有的内表遍历逻辑（INNER, LEFT, RIGHT JOIN）
                while (innerHasNext) {
                    innerOperator.Next();
                    currentInnerTuple = innerOperator.Current();
                    innerHasNext = innerOperator.hasNext();

                    if (currentInnerTuple == null) {
                        continue;
                    }

                    if (matchJoinCondition(currentOuterTuple, currentInnerTuple)) {
                        // 找到匹配
                        outerTupleMatched = true;
                        currentJoinTuple = createJoinTuple(currentOuterTuple, currentInnerTuple);
                        return;
                    }
                }
            }

            // 内表扫描完毕
            if (isOuterJoin() && !outerTupleMatched) {
                // LEFT/RIGHT JOIN: 输出外表元组和 null 值
                currentJoinTuple = createJoinTupleWithNulls(currentOuterTuple);
                currentOuterTuple = null;
                outerTupleMatched = false;
                return;
            }

            // 移动到下一个外表元组
            currentOuterTuple = null;
            outerTupleMatched = false;
        }
    }

    /**
     * 检查是否为外连接（LEFT JOIN 或 RIGHT JOIN）
     */
    private boolean isOuterJoin() {
        return joinType == LogicalJoinOperator.JoinType.LEFT || joinType == LogicalJoinOperator.JoinType.RIGHT;
    }

    /**
     * 创建普通的连接元组
     */
    private JoinTuple createJoinTuple(Tuple outerTuple, Tuple innerTuple) throws DBException {
        Tuple leftTuple, rightTuple;

        if (swapped) {
            // 对于 RIGHT JOIN，外表是原来的右表，内表是原来的左表
            leftTuple = innerTuple; // 原左表
            rightTuple = outerTuple; // 原右表
        } else {
            // 对于 INNER JOIN 和 LEFT JOIN
            leftTuple = outerTuple; // 原左表
            rightTuple = innerTuple; // 原右表
        }

        TabCol[] leftSchema = leftTuple.getTupleSchema();
        TabCol[] rightSchema = rightTuple.getTupleSchema();
        TabCol[] joinSchema = new TabCol[leftSchema.length + rightSchema.length];
        System.arraycopy(leftSchema, 0, joinSchema, 0, leftSchema.length);
        System.arraycopy(rightSchema, 0, joinSchema, leftSchema.length, rightSchema.length);

        return new JoinTuple(leftTuple, rightTuple, joinSchema);
    }

    /**
     * 创建包含 null 值的连接元组（用于外连接）
     */
    private JoinTuple createJoinTupleWithNulls(Tuple outerTuple) throws DBException {
        if (swapped) {
            // RIGHT JOIN: 右表（外表）有值，左表（内表）为 null
            ArrayList<ColumnMeta> leftSchema = leftOperator.outputSchema();
            TabCol[] leftTabCols = new TabCol[leftSchema.size()];
            for (int i = 0; i < leftSchema.size(); i++) {
                leftTabCols[i] = new TabCol(leftSchema.get(i).tableName, leftSchema.get(i).name);
            }

            TabCol[] rightSchema = outerTuple.getTupleSchema();
            TabCol[] joinSchema = new TabCol[leftTabCols.length + rightSchema.length];
            System.arraycopy(leftTabCols, 0, joinSchema, 0, leftTabCols.length);
            System.arraycopy(rightSchema, 0, joinSchema, leftTabCols.length, rightSchema.length);

            return new JoinTuple(null, outerTuple, joinSchema);
        } else {
            // LEFT JOIN: 左表（外表）有值，右表（内表）为 null
            ArrayList<ColumnMeta> rightSchema = rightOperator.outputSchema();
            TabCol[] rightTabCols = new TabCol[rightSchema.size()];
            for (int i = 0; i < rightSchema.size(); i++) {
                rightTabCols[i] = new TabCol(rightSchema.get(i).tableName, rightSchema.get(i).name);
            }

            TabCol[] leftSchema = outerTuple.getTupleSchema();
            TabCol[] joinSchema = new TabCol[leftSchema.length + rightTabCols.length];
            System.arraycopy(leftSchema, 0, joinSchema, 0, leftSchema.length);
            System.arraycopy(rightTabCols, 0, joinSchema, leftSchema.length, rightTabCols.length);

            return new JoinTuple(outerTuple, null, joinSchema);
        }
    }

    /**
     * 判断连接条件是否满足
     */
    private boolean matchJoinCondition(Tuple outerTuple, Tuple innerTuple) throws DBException {
        if (expr == null || expr.isEmpty()) {
            return true; // 笛卡尔积
        }

        if (outerTuple == null || innerTuple == null) {
            return false;
        }

        // 根据是否交换了左右表，构造正确顺序的连接元组来评估条件
        Tuple leftTuple, rightTuple;
        if (swapped) {
            leftTuple = innerTuple; // 原左表
            rightTuple = outerTuple; // 原右表
        } else {
            leftTuple = outerTuple; // 原左表
            rightTuple = innerTuple; // 原右表
        }

        TabCol[] leftSchema = leftTuple.getTupleSchema();
        TabCol[] rightSchema = rightTuple.getTupleSchema();
        TabCol[] joinSchema = new TabCol[leftSchema.length + rightSchema.length];
        System.arraycopy(leftSchema, 0, joinSchema, 0, leftSchema.length);
        System.arraycopy(rightSchema, 0, joinSchema, leftSchema.length, rightSchema.length);

        JoinTuple joinTuple = new JoinTuple(leftTuple, rightTuple, joinSchema);

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
