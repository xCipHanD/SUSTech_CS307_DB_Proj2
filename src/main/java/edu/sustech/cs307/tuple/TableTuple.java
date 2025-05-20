package edu.sustech.cs307.tuple;

import edu.sustech.cs307.meta.TabCol;
import edu.sustech.cs307.meta.TableMeta;
import edu.sustech.cs307.record.RID;
import edu.sustech.cs307.record.Record;
import edu.sustech.cs307.value.Value;
import edu.sustech.cs307.value.ValueType;

import java.util.ArrayList;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.exception.ExceptionTypes;
import edu.sustech.cs307.meta.ColumnMeta;

import io.netty.buffer.ByteBuf;

public class TableTuple extends Tuple {
    private final String tableName;
    private final TableMeta tableMeta;
    private final Record record;
    private final RID rid;

    public TableTuple(String tableName, TableMeta tableMeta, Record record, RID rid) {
        this.tableName = tableName;
        this.tableMeta = tableMeta;
        this.record = record;
        this.rid = rid;
    }

    @Override
    public Value getValue(TabCol tabCol) throws DBException {
        // If the TabCol's table name is null, it might be a derived column (e.g., from
        // a function).
        // In this case, a TableTuple, which represents a row from a physical table,
        // wouldn't directly have this value.
        // This logic might be more appropriate in a ProjectTuple or a similar class
        // that handles derived columns.
        // For now, if tableName in TabCol is null, we can't match it to this
        // TableTuple's tableName.
        if (tabCol.getTableName() == null) {
            // Option 1: Return null if the column is not directly from this table.
            // This is a reasonable approach if TableTuple is strictly for physical table
            // rows.
            return null;
            // Option 2: Or, if there's a convention for how derived columns are named or
            // aliased,
            // you might try to find a match based on columnName only. This is less safe.
            // For example: if (tableMeta.getColumnMeta(tabCol.getColumnName()) != null) {
            // ... }
            // However, this could lead to ambiguity if multiple tables have same column
            // names.
        }

        if (!tabCol.getTableName().equals(tableName)) {
            return null;
        }
        ColumnMeta columnMeta = tableMeta.getColumnMeta(tabCol.getColumnName());
        if (columnMeta == null) {
            return null;
        }
        int offset = columnMeta.getOffset();
        int len = columnMeta.getLen();
        // Use GetColumnValue to get the value based on offset and len
        ByteBuf columnValueBuf = record.GetColumnValue(offset, len); // Use the record passed to getValue
        // Convert ByteBuf to Value (assuming you have a method for this)
        return convertByteBufToValue(columnValueBuf, columnMeta.type);
    }

    private Value convertByteBufToValue(ByteBuf byteBuf, ValueType columnType) throws DBException {
        if (columnType == ValueType.INTEGER) {
            return new Value(byteBuf.getLong(0));
        } else if (columnType == ValueType.CHAR) {
            return new Value(byteBuf.getCharSequence(0, 64, java.nio.charset.StandardCharsets.UTF_8).toString());
        } else if (columnType == ValueType.FLOAT) {
            return new Value(byteBuf.getFloat(0));
        } else if (columnType == ValueType.DOUBLE) {
            return new Value(byteBuf.getDouble(0));
        } else {
            throw new DBException(ExceptionTypes.UnsupportedValueType(columnType));
        }
    }

    @Override
    public TabCol[] getTupleSchema() {
        ArrayList<TabCol> result = new ArrayList<TabCol>();
        // 使用column_list 而不是columns
        for (ColumnMeta columnMeta : tableMeta.columns_list) {
            TabCol tabCol = new TabCol(columnMeta.tableName, columnMeta.name);
            result.add(tabCol);
        }
        return result.toArray(new TabCol[0]);

    }

    @Override
    public Value[] getValues() throws DBException {
        // 通过 meta 顺序和信息获取所有 Value
        ArrayList<Value> values = new ArrayList<>();
        // 使用column_list而不是columns
        for (ColumnMeta columnMeta : this.tableMeta.columns_list) {
            TabCol tabCol = new TabCol(columnMeta.tableName, columnMeta.name);
            Value value = getValue(tabCol);
            values.add(value);
        }
        return values.toArray(new Value[0]);
    }

    public RID getRID() {
        return this.rid;
    }

    public String getTableName() {
        return this.tableName;
    }
}
