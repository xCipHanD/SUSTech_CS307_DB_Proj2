package edu.sustech.cs307.physicalOperator;

import java.util.ArrayList;
import java.util.Collection;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.meta.ColumnMeta;
import edu.sustech.cs307.meta.TabCol;
import edu.sustech.cs307.tuple.JoinTuple;
import edu.sustech.cs307.tuple.Tuple;
import net.sf.jsqlparser.expression.Expression;

public class NestedLoopJoinOperator implements PhysicalOperator {

    private final PhysicalOperator leftOperator;
    private final PhysicalOperator rightOperator;
    private final Collection<Expression> expr;

    private Tuple currentLeftTuple;
    private Tuple currentRightTuple;
    private Tuple currentJoinTuple;

    private boolean leftHasNext;
    private boolean rightHasNext;
    private boolean isOpen = false;

    private ArrayList<ColumnMeta> outputSchema;

    // 用于右表重置
    private boolean rightResetNeeded = false;

    public NestedLoopJoinOperator(PhysicalOperator leftOperator, PhysicalOperator rightOperator,
            Collection<Expression> expr) {
        this.leftOperator = leftOperator;
        this.rightOperator = rightOperator;
        this.expr = expr;
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
        rightResetNeeded = false;
        // 预先推进到第一个可用的连接元组
        advanceToNextJoin();
    }

    @Override
    public boolean hasNext() throws DBException {
        if (!isOpen)
            return false;
        return currentJoinTuple != null;
    }

    @Override
    public void Next() throws DBException {
        if (!isOpen)
            return;
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
            if (currentLeftTuple == null) {
                if (!leftHasNext) {
                    return;
                }
                leftOperator.Next();
                currentLeftTuple = leftOperator.Current();
                if (currentLeftTuple == null) {
                    leftHasNext = false;
                    continue;
                }
                rightOperator.Close();
                rightOperator.Begin();
                rightHasNext = rightOperator.hasNext();
                rightResetNeeded = false;
                leftHasNext = leftOperator.hasNext();
            }

            while (rightHasNext) {
                rightOperator.Next();
                currentRightTuple = rightOperator.Current();
                rightHasNext = rightOperator.hasNext();
                
                if (currentRightTuple != null && matchJoinCondition(currentLeftTuple, currentRightTuple)) {
                    TabCol[] leftSchema = currentLeftTuple.getTupleSchema();
                    TabCol[] rightSchema = currentRightTuple.getTupleSchema();
                    TabCol[] joinSchema = new TabCol[leftSchema.length + rightSchema.length];
                    System.arraycopy(leftSchema, 0, joinSchema, 0, leftSchema.length);
                    System.arraycopy(rightSchema, 0, joinSchema, leftSchema.length, rightSchema.length);
                    currentJoinTuple = new JoinTuple(currentLeftTuple, currentRightTuple, joinSchema);
                    return;
                }
            }
            
            currentLeftTuple = null;
        }
    }

    /**
     * 判断连接条件是否满足
     */
    private boolean matchJoinCondition(Tuple left, Tuple right) throws DBException {
        if (expr == null || expr.isEmpty()) {
            return true;
        }
        if (left == null || right == null) {
            return false;
        }
            
        // 先构造连接元组
        TabCol[] leftSchema = left.getTupleSchema();
        TabCol[] rightSchema = right.getTupleSchema();
        TabCol[] joinSchema = new TabCol[leftSchema.length + rightSchema.length];
        System.arraycopy(leftSchema, 0, joinSchema, 0, leftSchema.length);
        System.arraycopy(rightSchema, 0, joinSchema, leftSchema.length, rightSchema.length);
        JoinTuple joinTuple = new JoinTuple(left, right, joinSchema);

        // 评估所有连接条件
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
