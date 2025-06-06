package edu.sustech.cs307.tuple;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.meta.TabCol;
import edu.sustech.cs307.value.Value;

import java.util.ArrayList;
import java.util.List;

/**
 * ProjectTuple 类用于表示一个投影元组，它从输入元组中提取指定列的值。
 * 该类扩展了 Tuple 类，并提供了获取列值和元组模式的方法。
 */
public class ProjectTuple extends Tuple {
    private final List<TabCol> schema;
    private final Tuple inputTuple;

    public ProjectTuple(Tuple inputTuple, List<TabCol> schema) {
        this.schema = schema;
        this.inputTuple = inputTuple;
    }

    /**
     * 根据给定的记录和列信息，从输入元组中获取对应的值。
     * 如果指定的列在投影列表中，则返回该列的值；否则返回 null。
     *
     * @param tabCol 要获取值的列信息
     * @return 指定列的值，如果列不在投影列表中则返回 null
     */
    @Override
    public Value getValue(TabCol tabCol) throws DBException {
        // 首先尝试在当前投影的 schema 中查找完全匹配
        for (int i = 0; i < schema.size(); i++) {
            TabCol colMeta = schema.get(i);
            if (colMeta.getColumnName().equalsIgnoreCase(tabCol.getColumnName()) &&
                    (tabCol.getTableName() == null || colMeta.getTableName() == null ||
                            colMeta.getTableName().equalsIgnoreCase(tabCol.getTableName()))) {

                return inputTuple.getValue(colMeta);
            }
        }

        // 如果在投影的 schema 中没有找到，委托给原始输入 tuple
        return inputTuple.getValue(tabCol);
    }

    /**
     * 获取当前元组的模式（Schema）。
     *
     * @return 返回一个包含列信息的 TabCol 数组，表示元组的结构。
     */
    @Override
    public TabCol[] getTupleSchema() {
        return schema.toArray(new TabCol[0]);
    }

    @Override
    public Value[] getValues() throws DBException {
        // 通过 meta 顺序和信息获取所有 Value
        ArrayList<Value> values = new ArrayList<>();
        for (var tabCol : this.schema) {
            Value value = getValue(tabCol);
            values.add(value);
        }
        return values.toArray(new Value[0]);
    }
}
