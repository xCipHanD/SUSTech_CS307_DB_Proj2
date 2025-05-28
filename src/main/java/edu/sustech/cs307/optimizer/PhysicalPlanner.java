package edu.sustech.cs307.optimizer;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.exception.ExceptionTypes;
import edu.sustech.cs307.logicalOperator.*;
import edu.sustech.cs307.physicalOperator.*;
import edu.sustech.cs307.system.DBManager;
import edu.sustech.cs307.value.Value;
import edu.sustech.cs307.value.ValueType;
import edu.sustech.cs307.meta.ColumnMeta;
import edu.sustech.cs307.meta.TableMeta;
import edu.sustech.cs307.index.Index; // Added import for Index
import edu.sustech.cs307.index.InMemoryOrderedIndex; // Added
import java.io.File; // Added

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo; // Added import for EqualsTo
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ParenthesedExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.Values;

import org.pmw.tinylog.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class PhysicalPlanner {
    public static PhysicalOperator generateOperator(DBManager dbManager, LogicalOperator logicalOp) throws DBException {
        if (logicalOp instanceof LogicalTableScanOperator tableScanOperator) {
            return handleTableScan(dbManager, tableScanOperator);
        } else if (logicalOp instanceof LogicalFilterOperator filterOperator) {
            return handleFilter(dbManager, filterOperator);
        } else if (logicalOp instanceof LogicalJoinOperator joinOperator) {
            return handleJoin(dbManager, joinOperator);
        } else if (logicalOp instanceof LogicalProjectOperator projectOperator) {
            return handleProject(dbManager, projectOperator);
        } else if (logicalOp instanceof LogicalInsertOperator insertOperator) {
            return handleInsert(dbManager, insertOperator);
        } else if (logicalOp instanceof LogicalUpdateOperator updateOperator) {
            return handleUpdate(dbManager, updateOperator);
        } else if (logicalOp instanceof LogicalDeleteOperator deleteOperator) {
            return handleDelete(dbManager, deleteOperator);
        } else if (logicalOp instanceof LogicalAggregateOperator aggregateOperator) {
            return handleAggregate(dbManager, aggregateOperator);
        } else if (logicalOp instanceof LogicalOrderByOperator orderByOperator) {
            return handleOrderBy(dbManager, orderByOperator);
        } else {
            throw new DBException(ExceptionTypes.UnsupportedOperator(logicalOp.getClass().getSimpleName()));
        }
    }

    private static PhysicalOperator handleDelete(DBManager dbManager, LogicalDeleteOperator deleteOperator)
            throws DBException {
        String tableName = deleteOperator.getTableName();
        Expression whereExpr = deleteOperator.getWhereExpr();
        Logger.debug("Handling DELETE for table: " + tableName + ", condition: " + whereExpr);

        // Create a SeqScanOperator to scan the table
        SeqScanOperator seqScan = new SeqScanOperator(tableName, dbManager);

        // Wrap with FilterOperator if there is a WHERE condition
        PhysicalOperator child = (whereExpr != null) ? new FilterOperator(seqScan, whereExpr) : seqScan;

        // Create and return the DeleteOperator
        return new DeleteOperator(tableName, child, dbManager);
    }

    private static PhysicalOperator handleTableScan(DBManager dbManager, LogicalTableScanOperator logicalTableScanOp)
            throws DBException {
        String tableName = logicalTableScanOp.getTableName();
        TableMeta tableMeta = dbManager.getMetaManager().getTable(tableName);

        // 检查表是否有可用的索引，优先使用索引进行全表扫描
        if (tableMeta.getIndexes() != null && !tableMeta.getIndexes().isEmpty()) {
            // 选择第一个可用的索引进行全表扫描（可以进一步优化选择策略）
            String firstIndexedColumn = tableMeta.getIndexes().keySet().iterator().next();

            try {
                // 尝试获取或创建索引
                Index index = dbManager.getIndexManager().getIndex(tableName, firstIndexedColumn);

                if (index == null && tableMeta.getIndexes().containsKey(firstIndexedColumn)) {
                    // 尝试创建B+树索引
                    try {
                        index = dbManager.getIndexManager().createIndex(tableName, firstIndexedColumn);
                        Logger.info("Created new B+Tree index for full table scan on {}.{}", tableName,
                                firstIndexedColumn);
                    } catch (DBException e) {
                        Logger.warn("Failed to create B+Tree index for {}.{}: {}, trying InMemoryOrderedIndex",
                                tableName, firstIndexedColumn, e.getMessage());

                        // B+树索引创建失败，回退到InMemoryOrderedIndex
                        String dbName = "CS307-DB";
                        if (dbManager.getDiskManager() != null && dbManager.getDiskManager().getDbName() != null) {
                            dbName = dbManager.getDiskManager().getDbName();
                        }

                        String indexPersistPath = dbName + File.separator + "indexes" + File.separator + tableName
                                + File.separator + firstIndexedColumn + ".json";

                        index = new InMemoryOrderedIndex(indexPersistPath, tableName, firstIndexedColumn);
                        Logger.info("Using InMemoryOrderedIndex for full table scan on {}.{}", tableName,
                                firstIndexedColumn);
                    }
                }

                // 如果成功获得索引，使用索引进行全表扫描
                if (index != null) {
                    if (index instanceof InMemoryOrderedIndex) {
                        Logger.info("Using InMemoryIndexScanOperator for full table scan on table {}", tableName);
                        return new InMemoryIndexScanOperator((InMemoryOrderedIndex) index, dbManager);
                    } else {
                        // 对于B+树索引，使用HybridScanOperator，但不指定搜索键（全表扫描）
                        Logger.info("Using HybridScanOperator for full table scan on table {} with index on column {}",
                                tableName, firstIndexedColumn);
                        return new HybridScanOperator(tableName, firstIndexedColumn, null, dbManager, index);
                    }
                }
            } catch (DBException e) {
                Logger.warn("Failed to use index for table scan on {}: {}, falling back to SeqScan",
                        tableName, e.getMessage());
                // 如果索引操作失败，继续使用顺序扫描
            }
        }

        // 如果没有索引或索引操作失败，使用传统的顺序扫描
        Logger.info("Using SeqScanOperator for table {} (no suitable index available)", tableName);
        return new SeqScanOperator(tableName, dbManager);
    }

    private static PhysicalOperator handleFilter(DBManager dbManager, LogicalFilterOperator logicalFilterOp)
            throws DBException {
        LogicalOperator childLogicalOp = logicalFilterOp.getChild();
        Expression whereExpr = logicalFilterOp.getWhereExpr();

        if (childLogicalOp instanceof LogicalTableScanOperator) {
            LogicalTableScanOperator tableScanOp = (LogicalTableScanOperator) childLogicalOp;
            String tableName = tableScanOp.getTableName();
            TableMeta tableMeta = dbManager.getMetaManager().getTable(tableName);

            if (whereExpr instanceof EqualsTo) {
                EqualsTo equalsTo = (EqualsTo) whereExpr;
                Expression leftExpr = equalsTo.getLeftExpression();
                Expression rightExpr = equalsTo.getRightExpression();

                Column queryColumn = null;
                Value constantValue = null;

                if (leftExpr instanceof Column && !(rightExpr instanceof Column)) {
                    queryColumn = (Column) leftExpr;
                    constantValue = getConstantValueFromExpression(rightExpr, dbManager, tableMeta,
                            queryColumn.getColumnName());
                } else if (rightExpr instanceof Column && !(leftExpr instanceof Column)) {
                    queryColumn = (Column) rightExpr;
                    constantValue = getConstantValueFromExpression(leftExpr, dbManager, tableMeta,
                            queryColumn.getColumnName());
                }

                if (queryColumn != null && constantValue != null) {
                    String columnName = queryColumn.getColumnName();

                    // 首先尝试从IndexManager获取现有索引
                    Index index = dbManager.getIndexManager().getIndex(tableName, columnName);

                    // 如果IndexManager中没有，检查元数据中是否定义了索引
                    if (index == null && tableMeta.getIndexes() != null
                            && tableMeta.getIndexes().containsKey(columnName)) {
                        // 尝试创建或加载索引
                        try {
                            // 首先尝试创建B+树索引
                            index = dbManager.getIndexManager().createIndex(tableName, columnName);
                            Logger.info("Created new B+Tree index for {}.{}", tableName, columnName);
                        } catch (DBException e) {
                            Logger.warn("Failed to create B+Tree index for {}.{}: {}, trying InMemoryOrderedIndex",
                                    tableName, columnName, e.getMessage());

                            // B+树索引创建失败，回退到InMemoryOrderedIndex
                            String dbName = "CS307-DB"; // Fallback, ideally from dbManager
                            if (dbManager.getDiskManager() != null && dbManager.getDiskManager().getDbName() != null) {
                                dbName = dbManager.getDiskManager().getDbName();
                            }

                            String indexPersistPath = dbName + File.separator + "indexes" + File.separator + tableName
                                    + File.separator + columnName + ".json";

                            index = new InMemoryOrderedIndex(indexPersistPath, tableName, columnName);
                            Logger.info("Using InMemoryOrderedIndex for {}.{} with path {}", tableName, columnName,
                                    indexPersistPath);
                        }
                    }

                    // 使用混合扫描器，它会智能地选择索引扫描或顺序扫描
                    if (index != null) {
                        Logger.info("Using HybridScanOperator for table {} on column {} with search key {}",
                                tableName, columnName, constantValue);
                        return new HybridScanOperator(tableName, columnName, constantValue, dbManager, index);
                    }
                }
            }
        }

        PhysicalOperator inputOp = generateOperator(dbManager, logicalFilterOp.getChild());
        return new FilterOperator(inputOp, whereExpr);
    }

    // Helper method to extract a Value from a JSQLParser Expression (literal)
    // This needs to be robust and handle different types.
    private static Value getConstantValueFromExpression(Expression expr, DBManager dbManager, TableMeta tableMeta,
            String columnName) throws DBException {
        // We need the column's type to correctly create the Value object
        ColumnMeta columnMeta = tableMeta.getColumnMeta(columnName);
        if (columnMeta == null) {
            throw new DBException(ExceptionTypes.ColumnDoesNotExist("Column " + columnName + " not found in table "
                    + tableMeta.tableName + " for filter value extraction."));
        }
        ValueType expectedType = columnMeta.type;

        if (expr instanceof StringValue) {
            // Assuming ValueType.CHAR can represent various string types like VARCHAR,
            // TEXT, etc.
            if (expectedType != ValueType.CHAR) {
                throw new DBException(ExceptionTypes.TypeMismatch(
                        "Type mismatch for column " + columnName + ". Expected " + expectedType + " but got CHAR."));
            }
            return new Value(((StringValue) expr).getValue());
        } else if (expr instanceof LongValue) {
            // Assuming ValueType.INTEGER can represent various integer types like BIGINT,
            // SMALLINT, etc.
            if (expectedType != ValueType.INTEGER) {
                throw new DBException(ExceptionTypes.TypeMismatch(
                        "Type mismatch for column " + columnName + ". Expected " + expectedType + " but got INTEGER."));
            }
            return new Value(((LongValue) expr).getValue());
        } else if (expr instanceof DoubleValue) {
            if (expectedType == ValueType.FLOAT) {
                return new Value((float) ((DoubleValue) expr).getValue());
            } else if (expectedType == ValueType.DOUBLE) {
                return new Value(((DoubleValue) expr).getValue());
            } else {
                throw new DBException(ExceptionTypes.TypeMismatch("Type mismatch for column " + columnName
                        + ". Expected " + expectedType + " but got DOUBLE/FLOAT."));
            }
        }
        // Add more types as needed (DateValue, etc.)
        return null; // Or throw an exception if the expression type is not supported as a constant
    }

    private static PhysicalOperator handleJoin(DBManager dbManager, LogicalJoinOperator logicalJoinOp)
            throws DBException {
        PhysicalOperator leftOp = generateOperator(dbManager, logicalJoinOp.getLeftInput());
        PhysicalOperator rightOp = generateOperator(dbManager, logicalJoinOp.getRightInput());
        PhysicalOperator joinOp = new NestedLoopJoinOperator(leftOp, rightOp, logicalJoinOp.getJoinExprs());

        Collection<Expression> joinFilters = logicalJoinOp.getJoinExprs();
        PhysicalOperator finalOp = new FilterOperator(joinOp, joinFilters);

        return finalOp;
    }

    private static PhysicalOperator handleProject(DBManager dbManager, LogicalProjectOperator logicalProjectOp)
            throws DBException {
        PhysicalOperator inputOp = generateOperator(dbManager, logicalProjectOp.getChild());
        return new ProjectOperator(inputOp, logicalProjectOp.getOutputSchema());
    }

    /**
     * Converts a {@link LogicalAggregateOperator} into a
     * {@link PhysicalAggregateOperator}.
     * It recursively generates the physical operator for the child and then wraps
     * it with the aggregate operator.
     *
     * @param dbManager          The database manager instance.
     * @param logicalAggregateOp The logical aggregate operator to convert.
     * @return The corresponding physical aggregate operator.
     * @throws DBException If an error occurs during physical plan generation.
     */
    private static PhysicalOperator handleAggregate(DBManager dbManager, LogicalAggregateOperator logicalAggregateOp)
            throws DBException {
        PhysicalOperator childOperator = generateOperator(dbManager, logicalAggregateOp.getChild());
        return new PhysicalAggregateOperator(childOperator, logicalAggregateOp);
    }

    /**
     * 处理将逻辑插入操作转换为物理插入运算符的过程
     *
     * @param dbManager       提供数据库操作访问的数据库管理器实例
     * @param logicalInsertOp 需要被转换的逻辑插入运算符
     * @return 准备好执行的物理插入运算符
     * @throws DBException 如果存在列不匹配、类型不匹配或无效SQL语法时抛出
     */
    @SuppressWarnings("deprecation") // for ExpressionList<?>::getExpressions
    private static PhysicalOperator handleInsert(DBManager dbManager, LogicalInsertOperator logicalInsertOp)
            throws DBException {
        var tableMeta = dbManager.getMetaManager().getTable(logicalInsertOp.tableName);
        // Process columns
        List<String> columns = new ArrayList<>();
        if (logicalInsertOp.columns != null) {
            // the length must equal to the number of columns in the table
            if (tableMeta.columns.size() != logicalInsertOp.columns.size()) {
                throw new DBException(ExceptionTypes.InsertColumnSizeMismatch());
            }
            for (int i = 0; i < logicalInsertOp.columns.size(); i++) {
                String colName = logicalInsertOp.columns.get(i).getColumnName();
                if (tableMeta.getColumnMeta(colName) == null) {
                    throw new DBException(ExceptionTypes.ColumnDoesNotExist(colName));
                }
                if (!tableMeta.columns_list.get(i).name.equals(colName)) {
                    throw new DBException(ExceptionTypes.InsertColumnNameMismatch());
                }
                columns.add(colName);
            }
        } else {
            // If no columns specified, use all table columns in order
            for (ColumnMeta columnMeta : tableMeta.columns_list) {
                columns.add(columnMeta.name);
            }
        }
        if (!(logicalInsertOp.values instanceof Values)) {
            throw new DBException(ExceptionTypes.InvalidSQL("INSERT", "Values must be an expression list"));
        }
        ExpressionList<?> valuesList = ((Values) logicalInsertOp.values).getExpressions();
        if (columns.size() != valuesList.size()) {
            var element = valuesList.get(0);
            if (element instanceof ParenthesedExpressionList<?>) {
                // check the children reexpressions
                for (Expression expr : valuesList) {
                    if (expr instanceof ParenthesedExpressionList<?> expressionList) {
                        if (expressionList.getExpressions().size() != columns.size()) {
                            throw new DBException(ExceptionTypes.InsertColumnSizeMismatch());
                        }
                    } else {
                        throw new DBException(ExceptionTypes.InsertColumnSizeMismatch());
                    }
                }
            } else {
                throw new DBException(ExceptionTypes.InsertColumnSizeMismatch());
            }
        }

        List<Value> values = new ArrayList<>();
        parseValue(values, valuesList, tableMeta);
        // will always be same size tuple

        return new InsertOperator(logicalInsertOp.tableName, columns,
                values, dbManager);
    }

    @SuppressWarnings("deprecation")
    private static void parseValue(List<Value> values, ExpressionList<?> valuesList, TableMeta tableMeta)
            throws DBException {
        for (int i = 0; i < valuesList.size(); i++) {
            var expr = valuesList.getExpressions().get(i);
            if (expr instanceof StringValue string_value) {
                if (tableMeta.columns_list.get(i).type != ValueType.CHAR) {
                    throw new DBException(ExceptionTypes.InsertColumnTypeMismatch());
                }
                String value_str = string_value.getValue();
                if (value_str.length() > 64) {
                    value_str = value_str.substring(0, 64);
                }
                values.add(new Value(value_str));
            } else if (expr instanceof DoubleValue float_value) {
                // This should handle both FLOAT and DOUBLE from JSqlParser
                // We need to check the target column type to differentiate
                if (tableMeta.columns_list.get(i).type == ValueType.FLOAT) {
                    values.add(new Value((float) float_value.getValue())); // Create a FLOAT Value
                } else if (tableMeta.columns_list.get(i).type == ValueType.DOUBLE) {
                    values.add(new Value(float_value.getValue())); // Create a DOUBLE Value
                } else {
                    throw new DBException(ExceptionTypes.InsertColumnTypeMismatch());
                }
            } else if (expr instanceof LongValue long_value) {
                if (tableMeta.columns_list.get(i).type != ValueType.INTEGER) {
                    throw new DBException(ExceptionTypes.InsertColumnTypeMismatch());
                }
                values.add(new Value(long_value.getValue()));
            } else if (expr instanceof ParenthesedExpressionList<?> expressionList) {
                parseValue(values, expressionList, tableMeta);
            } else {
                throw new DBException(ExceptionTypes.InvalidSQL("INSERT", "Unsupported value type in VALUES clause"));
            }
        }
    }

    private static PhysicalOperator handleUpdate(DBManager dbManager, LogicalUpdateOperator logicalUpdateOp)
            throws DBException {
        PhysicalOperator scanner = generateOperator(dbManager, logicalUpdateOp.getChild());
        return new UpdateOperator(scanner, logicalUpdateOp.getTableName(),
                logicalUpdateOp
                        .getUpdateSets(),
                logicalUpdateOp.getExpression());
    }

    private static PhysicalOperator handleOrderBy(DBManager dbManager, LogicalOrderByOperator orderByOperator)
            throws DBException {
        PhysicalOperator childOperator = generateOperator(dbManager, orderByOperator.getChild());
        return new PhysicalOrderByOperator(childOperator, orderByOperator);
    }
}