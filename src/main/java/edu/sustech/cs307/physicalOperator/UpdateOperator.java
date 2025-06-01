package edu.sustech.cs307.physicalOperator;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.exception.ExceptionTypes;
import edu.sustech.cs307.meta.ColumnMeta;
import edu.sustech.cs307.meta.TabCol;
import edu.sustech.cs307.record.RecordFileHandle;
import edu.sustech.cs307.tuple.TableTuple;
import edu.sustech.cs307.tuple.TempTuple;
import edu.sustech.cs307.tuple.Tuple;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

import edu.sustech.cs307.value.Value;
import edu.sustech.cs307.value.ValueType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.update.UpdateSet;

public class UpdateOperator implements PhysicalOperator {
    private final SeqScanOperator seqScanOperator;
    private final String tableName;
    private final List<UpdateSet> updateSetList;
    private final Expression whereExpr;

    private int updateCount;
    private boolean isDone;

    public UpdateOperator(PhysicalOperator inputOperator, String tableName, List<UpdateSet> updateSetList,
            Expression whereExpr) {
        // UpdateOperator 现在可以接受 SeqScanOperator 或 FilterOperator 作为输入
        if (inputOperator instanceof SeqScanOperator seqScanOperator) {
            this.seqScanOperator = seqScanOperator;
        } else if (inputOperator instanceof FilterOperator filterOperator) {
            // 从 FilterOperator 中提取底层的 SeqScanOperator
            PhysicalOperator child = getChildOperator(filterOperator);
            if (!(child instanceof SeqScanOperator)) {
                throw new RuntimeException("The update operator requires SeqScanOperator as the base scanner, but got: "
                        + child.getClass().getSimpleName());
            }
            this.seqScanOperator = (SeqScanOperator) child;
        } else {
            throw new RuntimeException(
                    "The update operator only accepts SeqScanOperator or FilterOperator as input, but got: "
                            + inputOperator.getClass().getSimpleName());
        }

        this.tableName = tableName;
        this.updateSetList = updateSetList;
        this.whereExpr = whereExpr;
        this.updateCount = 0;
        this.isDone = false;
    }

    // Helper method to extract the child operator from FilterOperator
    private PhysicalOperator getChildOperator(FilterOperator filterOperator) {
        // 通过反射获取FilterOperator的子操作符
        try {
            java.lang.reflect.Field field = FilterOperator.class.getDeclaredField("child");
            field.setAccessible(true);
            return (PhysicalOperator) field.get(filterOperator);
        } catch (Exception e) {
            throw new RuntimeException("Failed to extract child operator from FilterOperator: " + e.getMessage());
        }
    }

    @Override
    public boolean hasNext() {
        return !isDone;
    }

    @Override
    public void Begin() throws DBException {
        seqScanOperator.Begin();
        RecordFileHandle fileHandle = seqScanOperator.getFileHandle();

        while (seqScanOperator.hasNext()) {
            seqScanOperator.Next();
            TableTuple tuple = (TableTuple) seqScanOperator.Current();

            if (whereExpr == null || tuple.eval_expr(whereExpr)) {
                Value[] oldValues = tuple.getValues();
                List<Value> newValues = new ArrayList<>(Arrays.asList(oldValues));
                TabCol[] schema = tuple.getTupleSchema();

                for (UpdateSet currentUpdateSet : this.updateSetList) {
                    List<Column> columnsToUpdate = currentUpdateSet.getColumns();
                    // Corrected: JSqlParser's UpdateSet has getValues() which returns an
                    // ExpressionList,
                    // and ExpressionList has getExpressions() to get the List<Expression>.
                    ExpressionList<Expression> newExpressions = (ExpressionList<Expression>) currentUpdateSet
                            .getValues();

                    if (columnsToUpdate.size() != newExpressions.size()) {
                        // This case should ideally be caught by the parser or an earlier validation
                        // step
                        throw new DBException(ExceptionTypes.InvalidSQL("UPDATE",
                                "Column and value counts do not match in an UPDATE SET clause"));
                    }

                    for (int i = 0; i < columnsToUpdate.size(); i++) {
                        Column column = columnsToUpdate.get(i);
                        Expression expression = newExpressions.get(i);

                        String targetColumnName = column.getColumnName();
                        String targetTable = null;
                        if (column.getTable() != null && column.getTable().getName() != null) {
                            targetTable = column.getTable().getName();
                        } else {
                            // If table is not specified in "SET table.column = ...", assume the table being
                            // updated.
                            // For "SET column = ...", table will be null here.
                            targetTable = tuple.getTableName();
                        }

                        int index = -1;
                        for (int j = 0; j < schema.length; j++) {
                            // Ensure schema table name and column name match, ignoring case.
                            if (schema[j].getColumnName().equalsIgnoreCase(targetColumnName)
                                    && schema[j].getTableName().equalsIgnoreCase(targetTable)) {
                                index = j;
                                break;
                            }
                        }
                        if (index == -1) {
                            throw new DBException(ExceptionTypes
                                    .ColumnDoesNotExist("Column " + targetColumnName + " in table " + targetTable));
                        }
                        // 使用带类型提示的表达式求值，根据目标列的类型来推断常量的正确类型
                        Value newValue = tuple.evaluateExpressionWithTypeHint(expression, targetTable,
                                targetColumnName);
                        newValues.set(index, newValue);
                    }
                }
                ByteBuf buffer = Unpooled.buffer();
                for (Value v : newValues) {
                    buffer.writeBytes(v.ToByte());
                }

                fileHandle.UpdateRecord(tuple.getRID(), buffer);
                updateCount++;
            }
        }
    }

    @Override
    public void Next() {
        isDone = true;
    }

    @Override
    public Tuple Current() {
        if (isDone) {
            ArrayList<Value> result = new ArrayList<>();
            result.add(new Value(updateCount, ValueType.INTEGER));
            return new TempTuple(result);
        } else {
            throw new RuntimeException("Call Next() first");
        }
    }

    @Override
    public void Close() {
        seqScanOperator.Close();
    }

    @Override
    public ArrayList<ColumnMeta> outputSchema() {
        ArrayList<ColumnMeta> schema = new ArrayList<>();
        schema.add(new ColumnMeta("update", "numberOfUpdatedRows", ValueType.INTEGER, 0, 0));
        return schema;
    }

    public void reset() {
        updateCount = 0;
        isDone = false;
    }

    public Tuple getNextTuple() {
        if (hasNext()) {
            Next();
            return Current();
        }
        return null;
    }

    public void close() {
        Close();
    }

    public String getTableName() {
        return tableName;
    }
}
