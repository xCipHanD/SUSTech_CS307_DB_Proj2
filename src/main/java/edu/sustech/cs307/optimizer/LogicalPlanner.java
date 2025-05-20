package edu.sustech.cs307.optimizer;

import java.io.StringReader;
import java.util.List;
import java.util.function.Function;

import edu.sustech.cs307.logicalOperator.dml.DropTableExecutor;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.parser.JSqlParser;
import net.sf.jsqlparser.statement.DescribeStatement;
import net.sf.jsqlparser.statement.ExplainStatement;
import net.sf.jsqlparser.statement.ShowColumnsStatement;
import net.sf.jsqlparser.statement.ShowStatement;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.drop.Drop;
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
        } // TODO: modify the delete operator
        else if (stmt instanceof Delete deleteStmt) {
            operator = handleDelete(dbManager, deleteStmt);
        }
        // functional
        else if (stmt instanceof CreateTable createTableStmt) {
            CreateTableExecutor createTable = new CreateTableExecutor(createTableStmt, dbManager, sql);
            createTable.execute();
            return null;
        } else if (stmt instanceof Drop dropTableStmt) {
            DropTableExecutor dropTable = new DropTableExecutor(dropTableStmt, dbManager);
            dropTable.execute();
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
            // Handle SELECT without FROM, e.g., SELECT 1+1;
            // This might involve a special operator or direct evaluation.
            // For now, let's assume it's not supported or needs a different path.
            // If there are aggregate functions without FROM, it's also a special case.
            boolean hasAggregates = plainSelect.getSelectItems().stream()
                    .anyMatch(item -> item.getExpression() instanceof Function);
            if (hasAggregates) {
                // Create a dummy operator that produces one row, if necessary, for global
                // aggregates
                // This part needs careful design. For now, let's assume aggregates need a FROM.
                // Or, handle it by creating a LogicalAggregateOperator with no child or a
                // special one.
            }
            // If no aggregates and no FROM, it could be SELECT 1, 'abc';
            // This could be a LogicalProjectOperator with a special "dummy" child or no
            // child.
            // For simplicity, we'll require a FROM for now or throw an error.
            throw new DBException(ExceptionTypes.UnsupportedCommand("SELECT without FROM: " + plainSelect.toString()));
        }

        LogicalOperator root = new LogicalTableScanOperator(plainSelect.getFromItem().toString(), dbManager);

        int depth = 0;
        if (plainSelect.getJoins() != null) {
            for (Join join : plainSelect.getJoins()) {
                root = new LogicalJoinOperator(
                        root,
                        new LogicalTableScanOperator(join.getRightItem().toString(), dbManager),
                        join.getOnExpressions(),
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

        // First project all select items
        root = new LogicalProjectOperator(root, plainSelect.getSelectItems());
        // Then apply aggregation if needed
        if (hasAggregates || (groupByExpressions != null && !groupByExpressions.isEmpty())) {
            root = new LogicalAggregateOperator(root, plainSelect.getSelectItems(), groupByExpressions);
        }
        return root;
    }

    private static LogicalOperator handleInsert(DBManager dbManager, Insert insertStmt) {
        return new LogicalInsertOperator(insertStmt.getTable().getName(), insertStmt.getColumns(),
                insertStmt.getValues());
    }

    private static LogicalOperator handleUpdate(DBManager dbManager, Update updateStmt) throws DBException {
        LogicalOperator root = new LogicalTableScanOperator(updateStmt.getTable().getName(), dbManager);
        return new LogicalUpdateOperator(root, updateStmt.getTable().getName(), updateStmt.getUpdateSets(),
                updateStmt.getWhere());
    }

    private static LogicalOperator handleDelete(DBManager dbManager, Delete deleteStmt) throws DBException {
        LogicalOperator root = new LogicalTableScanOperator(deleteStmt.getTable().getName(), dbManager);
        if (deleteStmt.getWhere() != null) {
            root = new LogicalFilterOperator(root, deleteStmt.getWhere());
        }
        return new LogicalDeleteOperator(root, deleteStmt.getTable().getName(), deleteStmt.getWhere(), dbManager);//
        // don't
        // know
        // is
        // it
        // right
        // to
        // use
        // dbmanager
        // here
    }
}