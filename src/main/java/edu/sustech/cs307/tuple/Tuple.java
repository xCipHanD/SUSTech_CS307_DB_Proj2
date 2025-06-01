package edu.sustech.cs307.tuple;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.exception.ExceptionTypes;
import edu.sustech.cs307.meta.TabCol;
import edu.sustech.cs307.value.Value;
import edu.sustech.cs307.value.ValueComparer;
import edu.sustech.cs307.value.ValueType;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;

public abstract class Tuple {
    public abstract Value getValue(TabCol tabCol) throws DBException;

    public abstract TabCol[] getTupleSchema();

    public abstract Value[] getValues() throws DBException;

    public boolean eval_expr(Expression expr) throws DBException {
        return evaluateCondition(this, expr);
    }

    private boolean evaluateCondition(Tuple tuple, Expression whereExpr) {
        if (whereExpr instanceof AndExpression andExpr) {
            return evaluateCondition(tuple, andExpr.getLeftExpression())
                    && evaluateCondition(tuple, andExpr.getRightExpression());
        } else if (whereExpr instanceof OrExpression orExpr) {
            return evaluateCondition(tuple, orExpr.getLeftExpression())
                    || evaluateCondition(tuple, orExpr.getRightExpression());
        } else if (whereExpr instanceof BinaryExpression binaryExpression) {
            return evaluateBinaryExpression(tuple, binaryExpression);
        } else {
            return true; // For non-binary and non-AND expressions, just return true for now
        }
    }

    private boolean evaluateBinaryExpression(Tuple tuple, BinaryExpression binaryExpr) {
        Expression leftExpr = binaryExpr.getLeftExpression();
        Expression rightExpr = binaryExpr.getRightExpression();
        String operator = binaryExpr.getStringExpression();
        Value leftValue = null;
        Value rightValue = null;

        try {
            if (leftExpr instanceof Column leftColumn) {
                String tableName = leftColumn.getTableName();
                String columnName = leftColumn.getColumnName();
                
                // 如果表名为空，从schema中查找正确的表名
                if (tableName == null) {
                    TabCol[] schema = getTupleSchema();
                    for (TabCol tc : schema) {
                        if (tc.getColumnName().equals(columnName)) {
                            tableName = tc.getTableName();
                            break;
                        }
                    }
                }
                
                if (tableName != null) {
                    leftValue = tuple.getValue(new TabCol(tableName, columnName));
                    if (leftValue != null && leftValue.type == ValueType.CHAR) {
                        leftValue = new Value(leftValue.toString());
                    }
                }
            } else {
                leftValue = getConstantValue(leftExpr);
            }

            if (rightExpr instanceof Column rightColumn) {
                String tableName = rightColumn.getTableName();
                String columnName = rightColumn.getColumnName();
                
                // 如果表名为空，从schema中查找正确的表名
                if (tableName == null) {
                    TabCol[] schema = getTupleSchema();
                    for (TabCol tc : schema) {
                        if (tc.getColumnName().equals(columnName)) {
                            tableName = tc.getTableName();
                            break;
                        }
                    }
                }
                
                if (tableName != null) {
                    rightValue = tuple.getValue(new TabCol(tableName, columnName));
                    if (rightValue != null && rightValue.type == ValueType.CHAR) {
                        rightValue = new Value(rightValue.toString());
                    }
                }
            } else {
                // 智能类型推断：如果左边是列，根据列的类型来决定右边常量的类型
                if (leftExpr instanceof Column leftColumn) {
                    String table_name = leftColumn.getTableName();
                    if (tuple instanceof TableTuple) {
                        TableTuple tableTuple = (TableTuple) tuple;
                        table_name = tableTuple.getTableName();
                    }
                    rightValue = getConstantValueWithTypeHint(rightExpr, table_name, leftColumn.getColumnName(), tuple);
                } else {
                    rightValue = getConstantValue(rightExpr); // Handle constant right value
                }
            }

            // 如果任一值为null，根据SQL标准返回false
            if (leftValue == null || rightValue == null) {
                return false;
            }

            int comparisonResult = ValueComparer.compare(leftValue, rightValue);
            return switch (operator) {
                case "=" -> comparisonResult == 0;
                case ">" -> comparisonResult > 0;
                case "<" -> comparisonResult < 0;
                case "<>", "!=" -> comparisonResult != 0;
                case ">=" -> comparisonResult >= 0;
                case "<=" -> comparisonResult <= 0;
                default -> false;
            };
        } catch (DBException e) {
            e.printStackTrace(); // Handle exception properly
        }
        return false;
    }

    private Value getConstantValue(Expression expr) {
        if (expr instanceof StringValue) {
            return new Value(((StringValue) expr).getValue(), ValueType.CHAR);
        } else if (expr instanceof DoubleValue) {
            return new Value(((DoubleValue) expr).getValue(), ValueType.DOUBLE);
        } else if (expr instanceof LongValue) {
            return new Value(((LongValue) expr).getValue(), ValueType.INTEGER);
        }
        return null; // Unsupported constant type
    }

    /**
     * 根据列类型提示来推断常量的正确类型
     * 这解决了 SQL parser 将所有小数都解析为 double 的问题
     */
    private Value getConstantValueWithTypeHint(Expression expr, String tableName, String columnName, Tuple tuple)
            throws DBException {
        if (expr instanceof StringValue) {
            return new Value(((StringValue) expr).getValue(), ValueType.CHAR);
        } else if (expr instanceof LongValue) {
            return new Value(((LongValue) expr).getValue(), ValueType.INTEGER);
        } else if (expr instanceof DoubleValue doubleValue) {
            // 关键：根据列的类型来决定常量的类型
            ValueType columnType = getColumnType(tableName, columnName, tuple);
            double value = doubleValue.getValue();

            if (columnType == ValueType.FLOAT) {
                return new Value((float) value, ValueType.FLOAT);
            } else if (columnType == ValueType.DOUBLE) {
                return new Value(value, ValueType.DOUBLE);
            } else if (columnType == ValueType.INTEGER) {
                return new Value((long) value, ValueType.INTEGER);
            } else {
                // 如果无法确定列类型，默认使用 DOUBLE
                return new Value(value, ValueType.DOUBLE);
            }
        }
        return null; // Unsupported constant type
    }

    /**
     * 获取指定列的数据类型
     */
    private ValueType getColumnType(String tableName, String columnName, Tuple tuple) throws DBException {
        if (tuple instanceof TableTuple tableTuple) {
            // 从 TableTuple 的 tableMeta 中获取列类型
            try {
                TabCol tabCol = new TabCol(tableName, columnName);
                Value columnValue = tuple.getValue(tabCol);
                if (columnValue != null) {
                    return columnValue.type;
                }
            } catch (Exception e) {
                // 如果获取失败，尝试从 schema 中推断
            }
        }

        // 从 tuple 的 schema 中查找列类型
        TabCol[] schema = tuple.getTupleSchema();
        if (schema != null) {
            for (TabCol tabCol : schema) {
                if (tabCol.getColumnName().equalsIgnoreCase(columnName) &&
                        (tableName == null || tabCol.getTableName() == null ||
                                tabCol.getTableName().equalsIgnoreCase(tableName))) {
                    // 尝试获取该列的值来确定类型
                    try {
                        Value value = tuple.getValue(tabCol);
                        if (value != null) {
                            return value.type;
                        }
                    } catch (Exception e) {
                        // 忽略错误，继续查找
                    }
                }
            }
        }

        // 无法确定类型，返回 UNKNOWN
        return ValueType.UNKNOWN;
    }

    public Value evaluateExpression(Expression expr) throws DBException {
        if (expr instanceof StringValue) {
            return new Value(((StringValue) expr).getValue(), ValueType.CHAR);
        } else if (expr instanceof DoubleValue doubleValue) {
            // 对于 DoubleValue，我们需要更智能的类型推断
            // 但在 evaluateExpression 中我们没有列类型的上下文，所以先返回 DOUBLE
            // 具体的类型推断会在调用处处理
            return new Value(doubleValue.getValue(), ValueType.DOUBLE);
        } else if (expr instanceof LongValue) {
            return new Value(((LongValue) expr).getValue(), ValueType.INTEGER);
        } else if (expr instanceof Column) {
            Column col = (Column) expr;
            String tableName = col.getTableName();
            String columnName = col.getColumnName();

            // 如果表名为null，尝试从当前tuple的schema中推断
            if (tableName == null) {
                // 对于TableTuple，使用其表名
                if (this instanceof TableTuple) {
                    TableTuple tableTuple = (TableTuple) this;
                    tableName = tableTuple.getTableName();
                } else {
                    // 对于其他类型的tuple，尝试在schema中查找列名
                    TabCol[] schema = getTupleSchema();
                    for (TabCol tabCol : schema) {
                        if (tabCol.getColumnName().equalsIgnoreCase(columnName)) {
                            tableName = tabCol.getTableName();
                            break;
                        }
                    }
                }
            }

            TabCol tabCol = new TabCol(tableName, columnName);
            Value result = getValue(tabCol);
            return result;
        } else if (expr instanceof Function) {
            Function function = (Function) expr;
            String functionName = function.getName();
            // function.getParameters() returns a raw ExpressionList
            ExpressionList<?> expressionList = function.getParameters();

            if (functionName.equalsIgnoreCase("float")) {
                if (expressionList != null && !expressionList.isEmpty()) {
                    // Get the first parameter, cast to Expression
                    Expression firstParamExpr = (Expression) expressionList.get(0);
                    Value val = evaluateExpression(firstParamExpr);
                    if (val.value instanceof Number) {
                        return new Value(((Number) val.value).floatValue(), ValueType.FLOAT);
                    } else {
                        throw new DBException(
                                ExceptionTypes.InvalidSQL("float function", "Argument must be a number."));
                    }
                } else {
                    throw new DBException(ExceptionTypes.InvalidSQL("float function", "Requires one argument."));
                }
            } else if (functionName.equalsIgnoreCase("sum")) {
                if (expressionList == null || expressionList.isEmpty()) {
                    throw new DBException(
                            ExceptionTypes.InvalidSQL("SUM", "SUM function requires at least one argument."));
                }
                double sum = 0;
                ValueType type = null;
                // Iterate over the raw ExpressionList, casting each item to Expression
                for (Object item : expressionList) {
                    if (item instanceof Expression) {
                        Expression e = (Expression) item;
                        Value val = evaluateExpression(e);
                        if (type == null) {
                            if (val.type == ValueType.INTEGER || val.type == ValueType.DOUBLE
                                    || val.type == ValueType.FLOAT) {
                                type = val.type;
                            } else {
                                throw new DBException(ExceptionTypes.InvalidSQL("SUM",
                                        "SUM function can only be applied to numeric types."));
                            }
                        }
                        if (val.type == ValueType.DOUBLE)
                            type = ValueType.DOUBLE;
                        else if (val.type == ValueType.FLOAT && type != ValueType.DOUBLE)
                            type = ValueType.FLOAT;

                        switch (val.type) {
                            case INTEGER:
                                sum += (Long) val.value;
                                break;
                            case DOUBLE:
                                sum += (Double) val.value;
                                break;
                            case FLOAT:
                                sum += (Float) val.value;
                                break;
                            default:
                                throw new DBException(ExceptionTypes.InvalidSQL("SUM",
                                        "SUM function can only be applied to numeric types."));
                        }
                    } else {
                        // This case should ideally not happen if JSqlParser populates ExpressionList
                        // correctly
                        throw new DBException(ExceptionTypes.InvalidSQL("SUM", "Invalid item in function parameters."));
                    }
                }
                if (type == ValueType.DOUBLE) {
                    return new Value(sum, ValueType.DOUBLE);
                } else if (type == ValueType.FLOAT) {
                    return new Value((float) sum, ValueType.FLOAT);
                } else { // INTEGER
                    return new Value((long) sum, ValueType.INTEGER);
                }
            } else {
                throw new DBException(ExceptionTypes.UnsupportedExpression(expr));
            }
        } else {
            throw new DBException(ExceptionTypes.UnsupportedExpression(expr));
        }
    }

    /**
     * 为 UPDATE 语句提供带类型提示的表达式求值
     * 根据目标列的类型来推断常量的正确类型
     */
    public Value evaluateExpressionWithTypeHint(Expression expr, String targetTableName, String targetColumnName)
            throws DBException {
        if (expr instanceof StringValue) {
            return new Value(((StringValue) expr).getValue(), ValueType.CHAR);
        } else if (expr instanceof LongValue) {
            return new Value(((LongValue) expr).getValue(), ValueType.INTEGER);
        } else if (expr instanceof DoubleValue doubleValue) {
            // 根据目标列的类型来决定常量的类型
            ValueType columnType = getColumnType(targetTableName, targetColumnName, this);
            double value = doubleValue.getValue();

            if (columnType == ValueType.FLOAT) {
                return new Value((float) value, ValueType.FLOAT);
            } else if (columnType == ValueType.DOUBLE) {
                return new Value(value, ValueType.DOUBLE);
            } else if (columnType == ValueType.INTEGER) {
                return new Value((long) value, ValueType.INTEGER);
            } else {
                // 如果无法确定列类型，默认使用 DOUBLE
                return new Value(value, ValueType.DOUBLE);
            }
        } else if (expr instanceof Column) {
            // 对于列引用，使用标准的 evaluateExpression
            return evaluateExpression(expr);
        } else if (expr instanceof Function) {
            // 对于函数，使用标准的 evaluateExpression
            return evaluateExpression(expr);
        } else {
            throw new DBException(ExceptionTypes.UnsupportedExpression(expr));
        }
    }

}