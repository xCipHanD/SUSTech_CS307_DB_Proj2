package edu.sustech.cs307.optimizer;

import java.io.StringReader;
import java.util.List;
import java.util.function.Function;

import edu.sustech.cs307.logicalOperator.dml.DropTableExecutor;
import edu.sustech.cs307.logicalOperator.dml.AlterTableExecutor;
import edu.sustech.cs307.logicalOperator.dml.CreateIndexExecutor;
import edu.sustech.cs307.logicalOperator.dml.DropIndexExecutor;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.parser.JSqlParser;
import net.sf.jsqlparser.statement.DescribeStatement;
import net.sf.jsqlparser.statement.ExplainStatement;
import net.sf.jsqlparser.statement.ShowColumnsStatement;
import net.sf.jsqlparser.statement.ShowStatement;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.show.ShowTablesStatement;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.expression.Expression;

import edu.sustech.cs307.exception.ExceptionTypes;
import edu.sustech.cs307.logicalOperator.*;
import edu.sustech.cs307.system.DBManager;
import edu.sustech.cs307.logicalOperator.dml.CreateTableExecutor;
import edu.sustech.cs307.logicalOperator.dml.ExplainExecutor;
import edu.sustech.cs307.logicalOperator.dml.ShowColumnsExecutor;
import edu.sustech.cs307.logicalOperator.dml.ShowDatabaseExecutor;
import edu.sustech.cs307.logicalOperator.dml.ShowTablesExecutor;
import edu.sustech.cs307.exception.DBException;

public class LogicalPlanner {
    public static LogicalOperator resolveAndPlan(DBManager dbManager, String sql) throws DBException {
        JSqlParser parser = new CCJSqlParserManager();
        Statement stmt = null;
        try {
            stmt = parser.parse(new StringReader(sql));
        } catch (JSQLParserException e) {
            throw new DBException(ExceptionTypes.InvalidSQL(sql, e.getMessage()));
        }
        LogicalOperator operator = null;
        // Query
        if (stmt instanceof Select selectStmt) {
            operator = handleSelect(dbManager, selectStmt);
        } else if (stmt instanceof Insert insertStmt) {
            operator = handleInsert(dbManager, insertStmt);
        } else if (stmt instanceof Update updateStmt) {
            operator = handleUpdate(dbManager, updateStmt);
        } else if (stmt instanceof Delete deleteStmt) {
            operator = handleDelete(dbManager, deleteStmt);
        }
        // functional
        else if (stmt instanceof CreateTable createTableStmt) {
            CreateTableExecutor createTable = new CreateTableExecutor(createTableStmt, dbManager, sql);
            createTable.execute();
            return null;
        } else if (stmt instanceof CreateIndex createIndexStmt) {
            CreateIndexExecutor createIndex = new CreateIndexExecutor(createIndexStmt, dbManager);
            createIndex.execute();
            return null;
        } else if (stmt instanceof Drop dropStmt) {
            if (dropStmt.getType().equalsIgnoreCase("INDEX")) {
                DropIndexExecutor dropIndex = new DropIndexExecutor(dropStmt, dbManager);
                dropIndex.execute();
                return null;
            } else {
                DropTableExecutor dropTable = new DropTableExecutor(dropStmt, dbManager);
                dropTable.execute();
                return null;
            }
        } else if (stmt instanceof Alter alterStmt) {
            AlterTableExecutor alterTable = new AlterTableExecutor(alterStmt, dbManager);
            alterTable.execute();
            return null;
        } else if (stmt instanceof ExplainStatement) {
            ExplainExecutor explainExecutor = new ExplainExecutor(dbManager, sql);
            explainExecutor.execute();
            return null;
        } else if (stmt instanceof ShowStatement showStatement) {
            ShowDatabaseExecutor showDatabaseExecutor = new ShowDatabaseExecutor(showStatement);
            showDatabaseExecutor.execute();
            return null;
        } else if (stmt instanceof ShowColumnsStatement showColumnsStatement) {
            ShowColumnsExecutor showColumnsExecutor = new ShowColumnsExecutor(showColumnsStatement, dbManager);
            showColumnsExecutor.execute();
            return null;
        } else if (stmt instanceof ShowTablesStatement) {
            ShowTablesExecutor showTablesExecutor = new ShowTablesExecutor((ShowTablesStatement) stmt, dbManager);
            showTablesExecutor.execute();
            return null;
        } else if (stmt instanceof DescribeStatement) {
            ShowColumnsExecutor showColumnsExecutor = new ShowColumnsExecutor((DescribeStatement) stmt, dbManager);
            showColumnsExecutor.execute();
            return null;
        } else {
            throw new DBException(ExceptionTypes.UnsupportedCommand((stmt.toString())));
        }
        return operator;
    }

    public static LogicalOperator handleSelect(DBManager dbManager, Select selectStmt) throws DBException {
        PlainSelect plainSelect = selectStmt.getPlainSelect();
        if (plainSelect.getFromItem() == null) {

            boolean hasAggregates = plainSelect.getSelectItems().stream()
                    .anyMatch(item -> item.getExpression() instanceof Function);
            if (hasAggregates) {

            }

            throw new DBException(ExceptionTypes.UnsupportedCommand("SELECT without FROM: " + plainSelect.toString()));
        }

        LogicalOperator root = new LogicalTableScanOperator(plainSelect.getFromItem().toString(), dbManager);

        int depth = 0;
        if (plainSelect.getJoins() != null) {
            for (Join join : plainSelect.getJoins()) {
                LogicalJoinOperator.JoinType joinType = LogicalJoinOperator.JoinType.INNER; 

                if (join.isLeft()) {
                    joinType = LogicalJoinOperator.JoinType.LEFT;
                } else if (join.isRight()) {
                    joinType = LogicalJoinOperator.JoinType.RIGHT;
                } else if (join.isCross()) {
                    joinType = LogicalJoinOperator.JoinType.CROSS;
                } else if (join.isInner() || join.isSimple()) {
                    joinType = LogicalJoinOperator.JoinType.INNER;
                } else {
                    throw new DBException(
                            ExceptionTypes.UnsupportedCommand("Unsupported JOIN type: " + join.toString()));
                }

                root = new LogicalJoinOperator(
                        root,
                        new LogicalTableScanOperator(join.getRightItem().toString(), dbManager),
                        join.getOnExpressions(),
                        joinType,
                        depth);
                depth += 1;
            }
        }

        if (plainSelect.getWhere() != null) {
            root = new LogicalFilterOperator(root, plainSelect.getWhere());
        }

        // Check for aggregation
        boolean hasAggregates = plainSelect.getSelectItems().stream()
                .anyMatch(item -> {
                    if (item.getExpression() instanceof net.sf.jsqlparser.expression.Function function) {
                        String funcName = function.toString().toUpperCase();
                        return funcName.contains("COUNT") || funcName.contains("SUM") ||
                                funcName.contains("AVG") || funcName.contains("MIN") ||
                                funcName.contains("MAX");
                    }
                    return false;
                });

        List<Expression> groupByExpressions = null;
        if (plainSelect.getGroupBy() != null) {
            groupByExpressions = plainSelect.getGroupBy().getGroupByExpressions();
        }

        if (hasAggregates || (groupByExpressions != null && !groupByExpressions.isEmpty())) {
            root = new LogicalAggregateOperator(root, plainSelect.getSelectItems(), groupByExpressions);
        }
        if (!hasAggregates && (groupByExpressions == null || groupByExpressions.isEmpty())) {
            root = new LogicalProjectOperator(root, plainSelect.getSelectItems());
        }

        if (plainSelect.getOrderByElements() != null && !plainSelect.getOrderByElements().isEmpty()) {
            root = new LogicalOrderByOperator(root, plainSelect.getOrderByElements());
        }

        return root;
    }

    private static LogicalOperator handleInsert(DBManager dbManager, Insert insertStmt) {
        return new LogicalInsertOperator(insertStmt.getTable().getName(), insertStmt.getColumns(),
                insertStmt.getValues());
    }

    private static LogicalOperator handleUpdate(DBManager dbManager, Update updateStmt) throws DBException {
        LogicalTableScanOperator tableScan = new LogicalTableScanOperator(updateStmt.getTable().getName(), dbManager);
        LogicalOperator root = tableScan;
        if (updateStmt.getWhere() != null) {
            root = new LogicalFilterOperator(root, updateStmt.getWhere());
        }
        return new LogicalUpdateOperator(root, updateStmt.getTable().getName(), updateStmt.getUpdateSets(),
                updateStmt.getWhere());
    }

    private static LogicalOperator handleDelete(DBManager dbManager, Delete deleteStmt) throws DBException {
        LogicalOperator root = new LogicalTableScanOperator(deleteStmt.getTable().getName(), dbManager);
        if (deleteStmt.getWhere() != null) {
            root = new LogicalFilterOperator(root, deleteStmt.getWhere());
        }
        return new LogicalDeleteOperator(root, deleteStmt.getTable().getName(), deleteStmt.getWhere(), dbManager);
    }
}