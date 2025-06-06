package edu.sustech.cs307.physicalOperator;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.exception.ExceptionTypes;
import edu.sustech.cs307.logicalOperator.LogicalOrderByOperator;
import edu.sustech.cs307.meta.ColumnMeta;
import edu.sustech.cs307.meta.TabCol;
import edu.sustech.cs307.tuple.Tuple;
import edu.sustech.cs307.value.Value;
import edu.sustech.cs307.value.ValueComparer;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * PhysicalOrderByOperator implements ORDER BY functionality by collecting all
 * tuples,
 * sorting them according to the ORDER BY clause, and then returning them in
 * order.
 */
public class PhysicalOrderByOperator implements PhysicalOperator {
    private final PhysicalOperator child;
    private final LogicalOrderByOperator logicalOperator;
    private List<Tuple> sortedTuples;
    private int currentIndex;

    public PhysicalOrderByOperator(PhysicalOperator child, LogicalOrderByOperator logicalOperator) {
        this.child = child;
        this.logicalOperator = logicalOperator;
        this.sortedTuples = new ArrayList<>();
        this.currentIndex = -1;
    }

    @Override
    public void Begin() throws DBException {
        child.Begin();

        // 收集所有tuple
        List<Tuple> allTuples = new ArrayList<>();
        while (child.hasNext()) {
            child.Next();
            allTuples.add(child.Current());
        }

        Comparator<Tuple> comparator = createComparator();
        sortedTuples = new ArrayList<>(allTuples);
        Collections.sort(sortedTuples, comparator);

        currentIndex = -1;
    }

    private Comparator<Tuple> createComparator() {
        return (t1, t2) -> {
            try {
                for (OrderByElement orderByElement : logicalOperator.getOrderByElements()) {
                    Expression expr = orderByElement.getExpression();
                    boolean isAsc = orderByElement.isAsc();

                    Value v1 = evaluateOrderByExpression(t1, expr);
                    Value v2 = evaluateOrderByExpression(t2, expr);

                    if (v1 == null && v2 == null)
                        continue;
                    if (v1 == null)
                        return isAsc ? -1 : 1;
                    if (v2 == null)
                        return isAsc ? 1 : -1;

                    int comparison = ValueComparer.compare(v1, v2);
                    if (comparison != 0) {
                        return isAsc ? comparison : -comparison;
                    }
                }
                return 0;
            } catch (DBException e) {
                throw new RuntimeException(e);
            }
        };
    }

    private Value evaluateOrderByExpression(Tuple tuple, Expression expr) throws DBException {
        if (expr instanceof Column) {
            Column col = (Column) expr;
            String tableName = col.getTableName();
            String columnName = col.getColumnName();

            if (tableName == null) {
                TabCol[] schema = tuple.getTupleSchema();
                for (TabCol tabCol : schema) {
                    if (tabCol.getColumnName().equalsIgnoreCase(columnName)) {
                        tableName = tabCol.getTableName();
                        break;
                    }
                }
            }

            return tuple.getValue(new TabCol(tableName, columnName));
        } else {
            throw new DBException(ExceptionTypes.UnsupportedExpression(expr));
        }
    }

    @Override
    public boolean hasNext() throws DBException {
        return currentIndex + 1 < sortedTuples.size();
    }

    @Override
    public void Next() throws DBException {
        if (!hasNext()) {
            throw new DBException(ExceptionTypes.NoMoreTuples());
        }
        currentIndex++;
    }

    @Override
    public Tuple Current() {
        if (currentIndex < 0 || currentIndex >= sortedTuples.size()) {
            return null;
        }
        return sortedTuples.get(currentIndex);
    }

    @Override
    public void Close() {
        try {
            child.Close();
        } catch (DBException e) {
        }
        sortedTuples.clear();
    }

    @Override
    public ArrayList<ColumnMeta> outputSchema() {
        return child.outputSchema();
    }
}