package edu.sustech.cs307.physicalOperator;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.exception.ExceptionTypes;
import edu.sustech.cs307.logicalOperator.LogicalAggregateOperator;
import edu.sustech.cs307.meta.ColumnMeta;
import edu.sustech.cs307.meta.TabCol;
import edu.sustech.cs307.tuple.TempTuple;
import edu.sustech.cs307.tuple.Tuple;
import edu.sustech.cs307.value.Value;
import edu.sustech.cs307.value.ValueType;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.ArrayList;
import java.util.Collections;
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
                ValueType type = ValueType.INTEGER; // 默认值，为SUM、AVG等调整
                if (func.getName().equalsIgnoreCase("COUNT")) {
                    type = ValueType.INTEGER;
                } else if (func.getName().equalsIgnoreCase("SUM")) {
                    // SUM类型取决于输入类型，可能是INTEGER或DOUBLE
                    // 这需要基于子模式的更复杂的类型推断
                    type = ValueType.INTEGER; // 占位符
                } else if (func.getName().equalsIgnoreCase("AVG")) {
                    type = ValueType.DOUBLE; // AVG通常是浮点数/双精度型
                } else if (func.getName().equalsIgnoreCase("MIN") || func.getName().equalsIgnoreCase("MAX")) {
                    // 类型取决于输入类型
                    type = ValueType.INTEGER; // 占位符
                }
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
            // 为COUNT函数计算计数
            Map<String, Long> columnCounts = new HashMap<>();
            for (SelectItem<?> item : selectItems) {
                if (item.getExpression() instanceof Function func
                        && func.getName().equalsIgnoreCase("COUNT")) {
                    columnCounts.put(func.toString(), 0L);
                }
            }
            while (child.hasNext()) {
                child.Next();
                Tuple t = child.Current();
                if (t == null)
                    continue;
                for (SelectItem<?> item : selectItems) {
                    if (item.getExpression() instanceof Function func
                            && func.getName().equalsIgnoreCase("COUNT")) {
                        String key = func.toString();
                        Long cnt = columnCounts.get(key);
                        // COUNT(column) 与 COUNT(*)
                        if (func.getParameters() != null && func.getParameters().getExpressions() != null
                                && !func.getParameters().getExpressions().isEmpty()) {
                            Expression p = func.getParameters().getExpressions().get(0);
                            // 目前没null检查
                            cnt++;
                        } else {
                            cnt++;
                        }
                        columnCounts.put(key, cnt);
                    }
                }
            }
            // 构建结果行
            List<Value> row = new ArrayList<>();
            for (SelectItem<?> item : selectItems) {
                Function func = (Function) item.getExpression();
                if (func.getName().equalsIgnoreCase("COUNT")) {
                    row.add(new Value(columnCounts.get(func.toString())));
                } else {
                    row.add(new Value(null, ValueType.UNKNOWN));
                }
            }
            resultTuples.add(new TempTuple(row));
        } else {
            // 带有GROUP BY的聚合
            Map<List<Value>, List<Value>> groupedAggregates = new HashMap<>();
            Map<List<Value>, Map<String, Long>> groupColumnCounts = new HashMap<>(); // 为每个分组的每个COUNT函数存储计数

            while (child.hasNext()) {
                child.Next();
                Tuple currentTuple = child.Current();
                if (currentTuple == null)
                    continue;

                List<Value> groupByKey = new ArrayList<>();
                for (Expression expr : groupByExpressions) {
                    groupByKey.add(currentTuple.evaluateExpression(expr));
                }

                groupedAggregates.putIfAbsent(groupByKey,
                        new ArrayList<>(Collections.nCopies(selectItems.size(), null))); // 聚合值的占位符
                groupColumnCounts.putIfAbsent(groupByKey, new HashMap<>());

                Map<String, Long> currentGroupCounts = groupColumnCounts.get(groupByKey);

                for (int i = 0; i < selectItems.size(); i++) {
                    SelectItem<?> item = selectItems.get(i);
                    if (item.getExpression() instanceof Function func) {
                        if (func.getName().equalsIgnoreCase("COUNT")) {
                            String countKey = func.toString();
                            currentGroupCounts.putIfAbsent(countKey, 0L);

                            // 检查是COUNT(*)还是COUNT(column)
                            if (func.getParameters() != null && func.getParameters().getExpressions() != null
                                    && !func.getParameters().getExpressions().isEmpty()) {
                                Expression paramExpr = func.getParameters().getExpressions().get(0);
                                if (paramExpr instanceof Column column) {
                                    // 对于COUNT(column)，我们需要检查列值是否为null
                                    Value val = currentTuple.evaluateExpression(column);
                                    if (val != null && !val.isNull()) {
                                        // 只有在列值不为null时才增加计数
                                        currentGroupCounts.put(countKey, currentGroupCounts.get(countKey) + 1);
                                    }
                                } else {
                                    // 其他表达式如COUNT(1)，总是增加计数
                                    currentGroupCounts.put(countKey, currentGroupCounts.get(countKey) + 1);
                                }
                            } else {
                                // COUNT(*)，总是增加计数
                                currentGroupCounts.put(countKey, currentGroupCounts.get(countKey) + 1);
                            }
                        }
                        // 为每个组添加SUM, AVG, MIN, MAX的逻辑
                    } else {
                        // 如果不是函数，那么一定是GROUP BY的键（或者涉及它们的表达式）
                        // ProjectOperator将处理这些值的投影
                        // 这里只确保GROUP BY键本身是输出的一部分
                        // 这些值已经在groupByKey中
                    }
                }
            }

            for (Map.Entry<List<Value>, List<Value>> entry : groupedAggregates.entrySet()) {
                List<Value> groupByKey = entry.getKey();
                List<Value> finalRow = new ArrayList<>(groupByKey); // 从分组值开始

                // 计算该组的最终聚合值
                List<Value> aggValuesForRow = new ArrayList<>();
                Map<String, Long> currentGroupCounts = groupColumnCounts.get(groupByKey);

                for (SelectItem<?> item : selectItems) {
                    if (item.getExpression() instanceof Function func) {
                        if (func.getName().equalsIgnoreCase("COUNT")) {
                            String countKey = func.toString();
                            long countValue = currentGroupCounts.getOrDefault(countKey, 0L);
                            aggValuesForRow.add(new Value(countValue));
                        }
                        // 为此组添加SUM, AVG, MIN, MAX的结果
                        else {
                            aggValuesForRow.add(new Value(null, ValueType.UNKNOWN)); // 空占位符
                        }
                    } else {
                        // 这是一个非聚合表达式。它应该是分组键之一。
                        // ProjectOperator负责选择它。
                        // PhysicalAggregateOperator的输出模式（因此TempTuple）
                        // 应与分组键+聚合函数结果对齐。
                        // 我们可以添加一个占位符，或确保ProjectOperator处理它。
                        // 现在，我们假设ProjectOperator将从groupByKey部分选择。
                    }
                }
                finalRow.addAll(aggValuesForRow);
                resultTuples.add(new TempTuple(finalRow));
            }

            // 空表GROUP BY的特殊处理
            if (resultTuples.isEmpty() && child.outputSchema() != null && !child.outputSchema().isEmpty() &&
                    logicalOperator.getAggregateExpressions().stream()
                            .anyMatch(si -> si.getExpression() instanceof Function
                                    && ((Function) si.getExpression()).getName().equalsIgnoreCase("COUNT"))
                    &&
                    (groupByExpressions != null && !groupByExpressions.isEmpty())) {
                // 如果GROUP BY存在，并且请求了COUNT，但没有形成组（过滤后的空输入）
                // SQL标准：COUNT为每个*将*存在（如果有数据）的组返回0。
                // 这很复杂。对于GROUP BY + COUNT的空输入，一个更简单的方法是没有行。
                // 然而，如果查询是`SELECT COUNT(*) FROM my_table GROUP BY
                // col_with_no_rows_matching_where`，
                // 它应该不产生输出。
                // 如果是`SELECT COUNT(col) FROM my_table GROUP BY grp_col`且表为空，则不输出。
            }
        }
        child.Close();
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
                    ValueType type = ValueType.INTEGER; // 默认
                    if (func.getName().equalsIgnoreCase("COUNT"))
                        type = ValueType.INTEGER;
                    else if (func.getName().equalsIgnoreCase("AVG"))
                        type = ValueType.DOUBLE;
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
