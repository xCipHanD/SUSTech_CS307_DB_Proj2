package edu.sustech.cs307.physicalOperator;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.exception.ExceptionTypes;
import edu.sustech.cs307.logicalOperator.LogicalAggregateOperator;
import edu.sustech.cs307.meta.ColumnMeta;
import edu.sustech.cs307.tuple.TempTuple;
import edu.sustech.cs307.tuple.Tuple;
import edu.sustech.cs307.value.Value;
import edu.sustech.cs307.value.ValueComparer;
import edu.sustech.cs307.value.ValueType;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PhysicalAggregateOperator implements PhysicalOperator {
    private final PhysicalOperator child;
    private final LogicalAggregateOperator logicalOperator;
    private List<Tuple> resultTuples;
    private int iteratorIndex;
    private ArrayList<ColumnMeta> schema;

    public PhysicalAggregateOperator(PhysicalOperator child, LogicalAggregateOperator logicalOperator) {
        this.child = child;
        this.logicalOperator = logicalOperator;
        this.resultTuples = new ArrayList<>();
        this.iteratorIndex = -1; // 初始化为第一条记录之前
    }

    @Override
    public void Begin() throws DBException {
        child.Begin();
        this.iteratorIndex = -1; // 在构建resultTuples之前重置指针
        // 首先初始化模式
        this.schema = new ArrayList<>();
        List<SelectItem<?>> selectItems = logicalOperator.getAggregateExpressions();
        List<Expression> groupByExpressions = logicalOperator.getGroupByExpressions();

        // 输出模式将首先包含分组列，然后是聚合函数的结果
        if (groupByExpressions != null) {
            for (Expression expr : groupByExpressions) {
                if (expr instanceof Column col) {
                    // 尝试从子运算符的模式中查找列元数据
                    ColumnMeta childColMeta = findColumnMeta(col, child.outputSchema());
                    if (childColMeta != null) {
                        schema.add(new ColumnMeta(childColMeta.tableName, childColMeta.name, childColMeta.type,
                                childColMeta.size, schema.size()));
                    } else {
                        // 如果未找到（例如表达式）- 这可能需要更强大的处理
                        schema.add(new ColumnMeta(null, col.getColumnName(), ValueType.UNKNOWN, 0, schema.size()));
                    }
                } else {
                    // 对于GROUP BY中的表达式，可能更难静态确定类型
                    schema.add(new ColumnMeta(null, expr.toString(), ValueType.UNKNOWN, 0, schema.size()));
                }
            }
        }

        for (SelectItem<?> item : selectItems) {
            if (item.getExpression() instanceof Function func) {
                String alias = item.getAlias() != null ? item.getAlias().getName() : func.toString();
                // 根据函数确定类型，例如，COUNT为INTEGER
                ValueType type = inferAggregateType(func);
                // 提取表名
                String tableName = null;
                if (func.getParameters() != null && func.getParameters().getExpressions() != null
                        && !func.getParameters().getExpressions().isEmpty()) {
                    if (func.getParameters().getExpressions().get(0) instanceof Column colParam
                            && colParam.getTable() != null) {
                        tableName = colParam.getTable().getName();
                    }
                }
                schema.add(new ColumnMeta(tableName, alias, type, 0, schema.size()));
            } else if (item.getExpression() instanceof Column col && groupByExpressions == null) {
                // 此情况适用于没有GROUP BY时的非聚合列。
                // SQL标准通常要求所有非聚合列都在GROUP BY中。
                // 如果我们允许这种情况，我们需要决定如何获取其元数据。
                // 目前，我们假设有效的SQL不会在没有GROUP BY的情况下发生这种情况，
                // 或者它在聚合后由project运算符处理。
                // 如果它是聚合后的简单列投影（例如 SELECT agg_col FROM (...)）
                // 这个运算符不应该看到它。
                // 如果是 SELECT col, COUNT(*) FROM table，这在没有GROUP BY col的情况下是无效的SQL。
            }
        }

        // 聚合逻辑
        if (groupByExpressions == null || groupByExpressions.isEmpty()) {
            // 没有GROUP BY的简单聚合：始终产生一行
            performSimpleAggregation(selectItems);
        } else {
            // 带有GROUP BY的聚合
            performGroupByAggregation(selectItems, groupByExpressions);
        }
        child.Close();
    }

    /**
     * 推断聚合函数的返回类型
     */
    private ValueType inferAggregateType(Function func) {
        String funcName = func.getName().toUpperCase();
        switch (funcName) {
            case "COUNT":
                return ValueType.INTEGER;
            case "SUM":
                // SUM类型取决于输入类型，这里需要更复杂的类型推断
                // 为简化，我们假设是 INTEGER，但实际应该基于列类型
                return inferSumType(func);
            case "AVG":
                return ValueType.DOUBLE; // AVG通常是浮点数/双精度型
            case "MIN":
            case "MAX":
                // MIN/MAX类型取决于输入类型
                return inferMinMaxType(func);
            default:
                return ValueType.INTEGER; // 默认值
        }
    }

    /**
     * 推断SUM函数的返回类型
     */
    private ValueType inferSumType(Function func) {
        if (func.getParameters() != null && func.getParameters().getExpressions() != null
                && !func.getParameters().getExpressions().isEmpty()) {
            Expression paramExpr = func.getParameters().getExpressions().get(0);
            if (paramExpr instanceof Column column) {
                ColumnMeta colMeta = findColumnMeta(column, child.outputSchema());
                if (colMeta != null) {
                    // 基于列类型返回适当的SUM类型
                    switch (colMeta.type) {
                        case INTEGER:
                            return ValueType.INTEGER;
                        case FLOAT:
                            return ValueType.FLOAT;
                        case DOUBLE:
                            return ValueType.DOUBLE;
                        default:
                            return ValueType.INTEGER;
                    }
                }
            }
        }
        return ValueType.INTEGER; // 默认
    }

    /**
     * 推断MIN/MAX函数的返回类型
     */
    private ValueType inferMinMaxType(Function func) {
        if (func.getParameters() != null && func.getParameters().getExpressions() != null
                && !func.getParameters().getExpressions().isEmpty()) {
            Expression paramExpr = func.getParameters().getExpressions().get(0);
            if (paramExpr instanceof Column column) {
                ColumnMeta colMeta = findColumnMeta(column, child.outputSchema());
                if (colMeta != null) {
                    return colMeta.type; // MIN/MAX保持原始类型
                }
            }
        }
        return ValueType.INTEGER; // 默认
    }

    /**
     * 执行简单聚合（无GROUP BY）
     */
    private void performSimpleAggregation(List<SelectItem<?>> selectItems) throws DBException {
        // 初始化聚合计算器
        Map<String, Object> aggregateValues = new HashMap<>();

        for (SelectItem<?> item : selectItems) {
            if (item.getExpression() instanceof Function func) {
                String key = func.toString();
                String funcName = func.getName().toUpperCase();

                switch (funcName) {
                    case "COUNT":
                        aggregateValues.put(key, 0L);
                        break;
                    case "SUM":
                        aggregateValues.put(key, null); // 初始为null
                        break;
                    case "MIN":
                    case "MAX":
                        aggregateValues.put(key, null); // 初始为null
                        break;
                }
            }
        }

        // 处理每一行数据
        while (child.hasNext()) {
            child.Next();
            Tuple tuple = child.Current();
            if (tuple == null)
                continue;

            for (SelectItem<?> item : selectItems) {
                if (item.getExpression() instanceof Function func) {
                    String key = func.toString();
                    String funcName = func.getName().toUpperCase();

                    switch (funcName) {
                        case "COUNT":
                            updateCount(aggregateValues, key, func, tuple);
                            break;
                        case "SUM":
                            updateSum(aggregateValues, key, func, tuple);
                            break;
                        case "MIN":
                            updateMin(aggregateValues, key, func, tuple);
                            break;
                        case "MAX":
                            updateMax(aggregateValues, key, func, tuple);
                            break;
                    }
                }
            }
        }

        // 构建结果行
        List<Value> row = new ArrayList<>();
        for (SelectItem<?> item : selectItems) {
            if (item.getExpression() instanceof Function func) {
                String key = func.toString();
                Object aggValue = aggregateValues.get(key);
                row.add(convertToValue(aggValue, func));
            } else {
                row.add(new Value(null, ValueType.UNKNOWN));
            }
        }
        resultTuples.add(new TempTuple(row));
    }

    /**
     * 执行分组聚合（带GROUP BY）
     */
    private void performGroupByAggregation(List<SelectItem<?>> selectItems, List<Expression> groupByExpressions)
            throws DBException {
        Map<List<Value>, Map<String, Object>> groupedAggregates = new HashMap<>();

        while (child.hasNext()) {
            child.Next();
            Tuple currentTuple = child.Current();
            if (currentTuple == null)
                continue;

            List<Value> groupByKey = new ArrayList<>();
            for (Expression expr : groupByExpressions) {
                groupByKey.add(currentTuple.evaluateExpression(expr));
            }

            // 初始化该组的聚合值
            groupedAggregates.putIfAbsent(groupByKey, new HashMap<>());
            Map<String, Object> groupAggregates = groupedAggregates.get(groupByKey);

            for (SelectItem<?> item : selectItems) {
                if (item.getExpression() instanceof Function func) {
                    String key = func.toString();
                    String funcName = func.getName().toUpperCase();

                    // 如果这是该组第一次处理此聚合函数，初始化
                    if (!groupAggregates.containsKey(key)) {
                        switch (funcName) {
                            case "COUNT":
                                groupAggregates.put(key, 0L);
                                break;
                            case "SUM":
                            case "MIN":
                            case "MAX":
                                groupAggregates.put(key, null);
                                break;
                        }
                    }

                    // 更新聚合值
                    switch (funcName) {
                        case "COUNT":
                            updateCount(groupAggregates, key, func, currentTuple);
                            break;
                        case "SUM":
                            updateSum(groupAggregates, key, func, currentTuple);
                            break;
                        case "MIN":
                            updateMin(groupAggregates, key, func, currentTuple);
                            break;
                        case "MAX":
                            updateMax(groupAggregates, key, func, currentTuple);
                            break;
                    }
                }
            }
        }

        // 构建结果
        for (Map.Entry<List<Value>, Map<String, Object>> entry : groupedAggregates.entrySet()) {
            List<Value> groupByKey = entry.getKey();
            Map<String, Object> groupAggregates = entry.getValue();

            List<Value> finalRow = new ArrayList<>(groupByKey); // 从分组值开始

            // 添加聚合函数结果
            for (SelectItem<?> item : selectItems) {
                if (item.getExpression() instanceof Function func) {
                    String key = func.toString();
                    Object aggValue = groupAggregates.get(key);
                    finalRow.add(convertToValue(aggValue, func));
                }
            }

            resultTuples.add(new TempTuple(finalRow));
        }
    }

    /**
     * 更新COUNT聚合
     */
    private void updateCount(Map<String, Object> aggregates, String key, Function func, Tuple tuple)
            throws DBException {
        Long count = (Long) aggregates.get(key);

        // 检查是COUNT(*)还是COUNT(column)
        if (func.getParameters() != null && func.getParameters().getExpressions() != null
                && !func.getParameters().getExpressions().isEmpty()) {
            Expression paramExpr = func.getParameters().getExpressions().get(0);
            if (paramExpr instanceof Column column) {
                // 对于COUNT(column)，我们需要检查列值是否为null
                Value val = tuple.evaluateExpression(column);
                if (val != null && !val.isNull()) {
                    // 只有在列值不为null时才增加计数
                    aggregates.put(key, count + 1);
                }
            } else {
                // 其他表达式如COUNT(1)，总是增加计数
                aggregates.put(key, count + 1);
            }
        } else {
            // COUNT(*)，总是增加计数
            aggregates.put(key, count + 1);
        }
    }

    /**
     * 更新SUM聚合
     */
    private void updateSum(Map<String, Object> aggregates, String key, Function func, Tuple tuple) throws DBException {
        if (func.getParameters() != null && func.getParameters().getExpressions() != null
                && !func.getParameters().getExpressions().isEmpty()) {
            Expression paramExpr = func.getParameters().getExpressions().get(0);
            if (paramExpr instanceof Column column) {
                Value val = tuple.evaluateExpression(column);
                if (val != null && !val.isNull()) {
                    Object currentSum = aggregates.get(key);
                    Object newSum = addValues(currentSum, val);
                    aggregates.put(key, newSum);
                }
            }
        }
    }

    /**
     * 更新MIN聚合
     */
    private void updateMin(Map<String, Object> aggregates, String key, Function func, Tuple tuple) throws DBException {
        if (func.getParameters() != null && func.getParameters().getExpressions() != null
                && !func.getParameters().getExpressions().isEmpty()) {
            Expression paramExpr = func.getParameters().getExpressions().get(0);
            if (paramExpr instanceof Column column) {
                Value val = tuple.evaluateExpression(column);
                if (val != null && !val.isNull()) {
                    Object currentMin = aggregates.get(key);
                    Object newMin = minValue(currentMin, val);
                    aggregates.put(key, newMin);
                }
            }
        }
    }

    /**
     * 更新MAX聚合
     */
    private void updateMax(Map<String, Object> aggregates, String key, Function func, Tuple tuple) throws DBException {
        if (func.getParameters() != null && func.getParameters().getExpressions() != null
                && !func.getParameters().getExpressions().isEmpty()) {
            Expression paramExpr = func.getParameters().getExpressions().get(0);
            if (paramExpr instanceof Column column) {
                Value val = tuple.evaluateExpression(column);
                if (val != null && !val.isNull()) {
                    Object currentMax = aggregates.get(key);
                    Object newMax = maxValue(currentMax, val);
                    aggregates.put(key, newMax);
                }
            }
        }
    }

    /**
     * 将两个值相加
     */
    private Object addValues(Object current, Value val) {
        if (current == null) {
            // 第一个值
            switch (val.type) {
                case INTEGER:
                    return (Long) val.value;
                case FLOAT:
                    return (Float) val.value;
                case DOUBLE:
                    return (Double) val.value;
                case CHAR:
                    throw new RuntimeException("SUM operation not supported for CHAR type");
                case UNKNOWN:
                    throw new RuntimeException("SUM operation not supported for UNKNOWN type");
                default:
                    return null;
            }
        } else {
            // 累加
            switch (val.type) {
                case INTEGER:
                    if (current instanceof Long) {
                        return (Long) current + (Long) val.value;
                    } else if (current instanceof Float) {
                        return (Float) current + (Long) val.value;
                    } else if (current instanceof Double) {
                        return (Double) current + (Long) val.value;
                    }
                    break;
                case FLOAT:
                    if (current instanceof Long) {
                        return (Long) current + (Float) val.value;
                    } else if (current instanceof Float) {
                        return (Float) current + (Float) val.value;
                    } else if (current instanceof Double) {
                        return (Double) current + (Float) val.value;
                    }
                    break;
                case DOUBLE:
                    if (current instanceof Long) {
                        return (Long) current + (Double) val.value;
                    } else if (current instanceof Float) {
                        return (Float) current + (Double) val.value;
                    } else if (current instanceof Double) {
                        return (Double) current + (Double) val.value;
                    }
                    break;
                case CHAR:
                    throw new RuntimeException("SUM operation not supported for CHAR type");
                case UNKNOWN:
                    throw new RuntimeException("SUM operation not supported for UNKNOWN type");
            }
        }
        return current;
    }

    /**
     * 比较并返回较小值
     */
    private Object minValue(Object current, Value val) {
        if (val == null || val.isNull()) {
            return current;
        }

        if (current == null) {
            // 第一个值
            return getValueObject(val);
        } else {
            try {
                Value currentVal = createValueFromObject(current);
                if (currentVal != null && ValueComparer.compare(val, currentVal) < 0) {
                    return getValueObject(val);
                }
            } catch (DBException e) {
                // 比较失败时保持当前值
                return current;
            }
        }
        return current;
    }

    /**
     * 比较并返回较大值
     */
    private Object maxValue(Object current, Value val) {
        if (val == null || val.isNull()) {
            return current;
        }

        if (current == null) {
            // 第一个值
            return getValueObject(val);
        } else {
            try {
                Value currentVal = createValueFromObject(current);
                if (currentVal != null && ValueComparer.compare(val, currentVal) > 0) {
                    return getValueObject(val);
                }
            } catch (DBException e) {
                // 比较失败时保持当前值
                return current;
            }
        }
        return current;
    }

    /**
     * 从Value对象获取原始值
     */
    private Object getValueObject(Value val) {
        return val.value;
    }

    /**
     * 从Object创建Value对象，需要正确推断类型
     */
    private Value createValueFromObject(Object obj) {
        if (obj instanceof Long) {
            return new Value((Long) obj, ValueType.INTEGER);
        } else if (obj instanceof Float) {
            return new Value((Float) obj, ValueType.FLOAT);
        } else if (obj instanceof Double) {
            return new Value((Double) obj, ValueType.DOUBLE);
        } else if (obj instanceof String) {
            return new Value((String) obj, ValueType.CHAR);
        }
        return null;
    }

    /**
     * 将聚合结果转换为Value对象
     */
    private Value convertToValue(Object aggValue, Function func) {
        String funcName = func.getName().toUpperCase();

        if (aggValue == null) {
            // 对于没有数据的情况
            switch (funcName) {
                case "COUNT":
                    return new Value(0L, ValueType.INTEGER); // COUNT返回0
                default:
                    return new Value(null, inferAggregateType(func)); // 其他函数返回NULL
            }
        }

        // 确保返回的Value对象有正确的类型信息
        if (aggValue instanceof Long) {
            return new Value((Long) aggValue, ValueType.INTEGER);
        } else if (aggValue instanceof Float) {
            return new Value((Float) aggValue, ValueType.FLOAT);
        } else if (aggValue instanceof Double) {
            return new Value((Double) aggValue, ValueType.DOUBLE);
        } else if (aggValue instanceof String) {
            return new Value((String) aggValue, ValueType.CHAR);
        }

        return new Value(null, inferAggregateType(func));
    }

    private ColumnMeta findColumnMeta(Column column, ArrayList<ColumnMeta> searchSchema) {
        String colName = column.getColumnName();
        String tblName = column.getTable() != null ? column.getTable().getName() : null;
        for (ColumnMeta cm : searchSchema) {
            if (cm.name.equalsIgnoreCase(colName)) {
                if (tblName == null || cm.tableName.equalsIgnoreCase(tblName)) {
                    return cm;
                }
            }
        }
        return null; // 未找到
    }

    @Override
    public boolean hasNext() throws DBException {
        // 检查是否有下一个元组
        return iteratorIndex + 1 < resultTuples.size();
    }

    @Override
    public void Next() throws DBException {
        if (!hasNext()) {
            throw new DBException(ExceptionTypes.NoMoreTuples());
        }
        // 前进到下一个结果
        iteratorIndex++;
    }

    @Override
    public Tuple Current() {
        // 如果在有效范围内，返回当前元组
        if (iteratorIndex < 0 || iteratorIndex >= resultTuples.size()) {
            return null;
        }
        return resultTuples.get(iteratorIndex);
    }

    @Override
    public void Close() {
        // 子运算符已在Begin()中关闭
        resultTuples = null; // 释放内存
        schema = null;
    }

    @Override
    public ArrayList<ColumnMeta> outputSchema() {
        if (this.schema == null) {
            // 模式应该在Begin()中构建。如果之前调用，这是有问题的。
            // 为了安全起见，尝试构建一个基本模式，但这表明存在问题。
            // 这是一个简化版本，Begin()应该完成完整的构建。
            this.schema = new ArrayList<>();
            List<SelectItem<?>> selectItems = logicalOperator.getAggregateExpressions();
            List<Expression> groupByExpressions = logicalOperator.getGroupByExpressions();

            if (groupByExpressions != null) {
                for (Expression expr : groupByExpressions) {
                    this.schema.add(new ColumnMeta(null, expr.toString(), ValueType.UNKNOWN, 0, this.schema.size()));
                }
            }
            for (SelectItem<?> item : selectItems) {
                if (item.getExpression() instanceof Function func) {
                    String alias = item.getAlias() != null ? item.getAlias().getName() : func.toString();
                    ValueType type = inferAggregateType(func);
                    this.schema.add(new ColumnMeta(null, alias, type, 0, this.schema.size()));
                } else if (item.getExpression() instanceof Column col && groupByExpressions == null) {
                    // 如果没有分组，这种情况理想情况下不应由此运算符构建
                } else if (item.getExpression() instanceof Column col && groupByExpressions != null) {
                    // 如果它是一个也是分组键的列，则已添加。
                    // 如果它是一个投影的列，而不是分组键，也不是聚合，
                    // 那么查询可能是无效的，或者需要ProjectOperator来处理它。
                    // 我们在这里只向模式添加聚合函数或分组键。
                }
            }
        }
        return this.schema;
    }
}
