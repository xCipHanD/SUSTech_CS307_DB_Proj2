package edu.sustech.cs307.tuple;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.meta.TabCol;
import edu.sustech.cs307.value.Value;

/**
 * JoinTuple 类表示两个元组的连接结果。
 * 它包含两个元组（leftTuple 和 rightTuple）以及连接后的列信息（tabCol）。
 * 支持 LEFT JOIN 和 RIGHT JOIN 中的 null 值处理。
 */
public class JoinTuple extends Tuple {
    private final Tuple leftTuple;
    private final Tuple rightTuple;
    private final TabCol[] tupleSchema;

    public JoinTuple(Tuple leftTuple, Tuple rightTuple, TabCol[] tabCol) {
        this.leftTuple = leftTuple;
        this.rightTuple = rightTuple;
        this.tupleSchema = tabCol;
    }

    /**
     * 获取指定记录中对应列的值。
     * 首先尝试从左侧元组中获取值，如果左侧值为 null 或左侧元组为 null，则从右侧元组中获取值。
     *
     * @param tabCol 要获取值的列
     * @return 返回对应列的值，如果两侧元组均无值，则返回 null
     */
    @Override
    public Value getValue(TabCol tabCol) throws DBException {
        String tableName = tabCol.getTableName();
        String columnName = tabCol.getColumnName();

        if (tableName == null) {
            for (TabCol tc : tupleSchema) {
                if (tc.getColumnName().equals(columnName)) {
                    tableName = tc.getTableName();
                    break;
                }
            }
        }

        // 尝试从左侧元组获取值
        if (leftTuple != null) {
            try {
                if (tableName != null) {
                    TabCol leftTabCol = new TabCol(tableName, columnName);
                    Value leftValue = leftTuple.getValue(leftTabCol);
                    if (leftValue != null) {
                        return leftValue;
                    }
                }
            } catch (DBException e) {
                // 左侧元组中没有找到该列，继续尝试右侧
            }
        }

        // 尝试从右侧元组获取值
        if (rightTuple != null) {
            try {
                if (tableName != null) {
                    TabCol rightTabCol = new TabCol(tableName, columnName);
                    return rightTuple.getValue(rightTabCol);
                }
            } catch (DBException e) {
                // 右侧元组中也没有找到该列
            }
        }

        return null;
    }

    /**
     * 获取当前元组的模式（列信息）。
     *
     * @return 返回一个包含列信息的 TabCol 数组。
     */
    @Override
    public TabCol[] getTupleSchema() {
        return tupleSchema;
    }

    @Override
    public Value[] getValues() throws DBException {
        Value[] result = new Value[tupleSchema.length];
        for (int i = 0; i < tupleSchema.length; i++) {
            result[i] = getValue(tupleSchema[i]);
        }
        return result;
    }

    /**
     * 获取左侧元组
     */
    public Tuple getLeftTuple() {
        return leftTuple;
    }

    /**
     * 获取右侧元组
     */
    public Tuple getRightTuple() {
        return rightTuple;
    }
}
