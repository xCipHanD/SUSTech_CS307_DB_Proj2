package edu.sustech.cs307.physicalOperator;

import net.sf.jsqlparser.expression.Expression;
import edu.sustech.cs307.meta.ColumnMeta;
import edu.sustech.cs307.tuple.Tuple;
import java.util.ArrayList;
import java.util.Collection;

import edu.sustech.cs307.exception.DBException;
import org.pmw.tinylog.Logger;

public class FilterOperator implements PhysicalOperator {
    private PhysicalOperator child;
    private Expression whereExpr;
    private Tuple currentTuple;
    private boolean isOpen = false;
    private boolean readyForNext = false;

    public FilterOperator(PhysicalOperator child, Expression whereExpr) {
        this.child = child;
        this.whereExpr = whereExpr;
    }

    public FilterOperator(PhysicalOperator child, Collection<Expression> whereExpr) {
        this.child = child;
        this.whereExpr = whereExpr.iterator().next();
    }

    @Override
    public void Begin() throws DBException {
        Logger.debug("FilterOperator.Begin() 被调用");
        child.Begin();
        isOpen = true;
        currentTuple = null;
        readyForNext = false;
    }

    @Override
    public boolean hasNext() throws DBException {
        Logger.debug("FilterOperator.hasNext() 被调用");
        if (!isOpen) {
            return false;
        }

        if (!readyForNext) {
            return findNext();
        }
        return currentTuple != null;
    }

    @Override
    public void Next() throws DBException {
        Logger.debug("FilterOperator.Next() 被调用");
        if (!isOpen) {
            return;
        }

        if (!readyForNext) {
            hasNext(); // 这会调用findNext()来准备下一个元组
        }

        readyForNext = false;
    }

    /**
     * 查找下一个符合条件的元组，并准备好它
     * 
     * @return 如果找到则返回true，否则返回false
     */
    private boolean findNext() throws DBException {
        // 标记没有找到合适的元组
        currentTuple = null;

        while (child.hasNext()) {
            child.Next();
            Tuple tuple = child.Current();

            if (tuple != null && tuple.eval_expr(whereExpr)) {
                Logger.debug("FilterOperator找到匹配的元组: " + tuple);
                currentTuple = tuple;
                readyForNext = true;
                return true;
            }
        }
        Logger.debug("FilterOperator没有找到更多匹配的元组");
        return false;
    }

    @Override
    public Tuple Current() {
        Logger.debug("FilterOperator.Current() 被调用，返回: " + currentTuple);
        return currentTuple;
    }

    @Override
    public void Close() throws DBException {
        if (child != null) {
            child.Close();
        }
        isOpen = false;
        currentTuple = null;
        readyForNext = false;
        Logger.debug("FilterOperator.Close() 被调用");
    }

    @Override
    public ArrayList<ColumnMeta> outputSchema() {
        return child.outputSchema();
    }
}
