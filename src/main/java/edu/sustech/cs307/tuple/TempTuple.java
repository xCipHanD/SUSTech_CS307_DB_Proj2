package edu.sustech.cs307.tuple;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.exception.ExceptionTypes;
import edu.sustech.cs307.meta.TabCol;
import edu.sustech.cs307.value.Value;

import java.util.ArrayList;
import java.util.List;

public class TempTuple extends Tuple {
    private List<Value> values;
    private TabCol[] schema;

    public TempTuple(List<Value> values) {
        this.values = values;
        this.schema = null;
    }

    public TempTuple(List<Value> values, TabCol[] schema) {
        this.values = values;
        this.schema = schema;
    }

    @Override
    public Value getValue(TabCol tabCol) throws DBException {
        // 如果有schema信息，尝试根据schema查找值
        if (schema != null) {
            for (int i = 0; i < schema.length; i++) {
                TabCol schemaCol = schema[i];
                if (schemaCol.getColumnName().equalsIgnoreCase(tabCol.getColumnName()) &&
                        (tabCol.getTableName() == null || schemaCol.getTableName() == null ||
                                schemaCol.getTableName().equalsIgnoreCase(tabCol.getTableName()))) {
                    if (i < values.size()) {
                        return values.get(i);
                    }
                }
            }
        }
        throw new DBException(ExceptionTypes.GetValueFromTempTuple());
    }

    @Override
    public TabCol[] getTupleSchema() {
        return schema;
    }

    @Override
    public Value[] getValues() throws DBException {
        return this.values.toArray(new Value[0]);
    }
}