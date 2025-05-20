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
                // get table name
                String table_name = leftColumn.getTableName();
                if (tuple instanceof TableTuple) {
                    TableTuple tableTuple = (TableTuple) tuple;
                    table_name = tableTuple.getTableName();
                }
                leftValue = tuple.getValue(new TabCol(table_name, leftColumn.getColumnName()));
                if (leftValue.type == ValueType.CHAR) {
                    leftValue = new Value(leftValue.toString());
                }
            } else {
                leftValue = getConstantValue(leftExpr); // Handle constant left value
            }

            if (rightExpr instanceof Column rightColumn) {
                // get table name
                String table_name = rightColumn.getTableName();
                if (tuple instanceof TableTuple) {
                    TableTuple tableTuple = (TableTuple) tuple;
                    table_name = tableTuple.getTableName();
                }
                rightValue = tuple.getValue(new TabCol(table_name, rightColumn.getColumnName()));
            } else {
                rightValue = getConstantValue(rightExpr); // Handle constant right value

            }

            if (leftValue == null || rightValue == null)
                return false;

            int comparisonResult = ValueComparer.compare(leftValue, rightValue);
            if (operator.equals("=")) {
                return comparisonResult == 0;
            } else if (operator.equals(">")) {
                return comparisonResult > 0;
            } else if (operator.equals("<")) {
                return comparisonResult < 0;
            } else if (operator.equals("<>") || operator.equals("!=")) {
                return comparisonResult != 0;
            } else if (operator.equals(">=")) {
                return comparisonResult >= 0;
            } else if (operator.equals("<=")) {
                return comparisonResult <= 0;
            }

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

    public Value evaluateExpression(Expression expr) throws DBException {
        if (expr instanceof StringValue) {
            return new Value(((StringValue) expr).getValue(), ValueType.CHAR);
        } else if (expr instanceof DoubleValue) {
            return new Value(((DoubleValue) expr).getValue(), ValueType.DOUBLE);
        } else if (expr instanceof LongValue) {
            return new Value(((LongValue) expr).getValue(), ValueType.INTEGER);
        } else if (expr instanceof Column) {
            Column col = (Column) expr;
            return getValue(new TabCol(col.getTableName(), col.getColumnName()));
        } else if (expr instanceof Function) {
            Function function = (Function) expr;
            String functionName = function.getName();
            // function.getParameters() returns a raw ExpressionList
            ExpressionList expressionList = function.getParameters();

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
            }
            else {
                throw new DBException(ExceptionTypes.UnsupportedExpression(expr));
            }
        } else {
            throw new DBException(ExceptionTypes.UnsupportedExpression(expr));
        }
    }

}