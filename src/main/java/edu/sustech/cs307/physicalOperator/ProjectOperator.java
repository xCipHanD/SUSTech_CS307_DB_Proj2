package edu.sustech.cs307.physicalOperator;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.meta.ColumnMeta;
import edu.sustech.cs307.tuple.ProjectTuple;
import edu.sustech.cs307.tuple.Tuple;
import edu.sustech.cs307.meta.TabCol;

import java.util.ArrayList;
import java.util.List;

public class ProjectOperator implements PhysicalOperator {
    private PhysicalOperator child;
    private List<TabCol> outputSchema;
    private Tuple currentTuple;

    public ProjectOperator(PhysicalOperator child, List<TabCol> outputSchema) {
        this.child = child;
        this.outputSchema = outputSchema;
        // 修复：添加空指针检查，确保getTableName()返回值不为null
        if (this.outputSchema.size() == 1 &&
                ("*".equals(this.outputSchema.get(0).getTableName()) ||
                        (this.outputSchema.get(0).getTableName() == null
                                && "*".equals(this.outputSchema.get(0).getColumnName())))) {
            List<TabCol> newOutputSchema = new ArrayList<>();
            for (ColumnMeta tabCol : child.outputSchema()) {
                newOutputSchema.add(new TabCol(tabCol.tableName, tabCol.name));
            }
            this.outputSchema = newOutputSchema;
        }
    }

    @Override
    public boolean hasNext() throws DBException {
        return child.hasNext();
    }

    @Override
    public void Begin() throws DBException {
        child.Begin();
    }

    @Override
    public void Next() throws DBException {
        if (hasNext()) {
            child.Next();
            Tuple inputTuple = child.Current();
            if (inputTuple != null) {
                currentTuple = new ProjectTuple(inputTuple, outputSchema); // Create ProjectTuple
            } else {
                currentTuple = null;
            }
        } else {
            currentTuple = null;
        }
    }

    @Override
    public Tuple Current() {
        return currentTuple;
    }

    @Override
    public void Close() {
        child.Close();
        currentTuple = null;
    }

    @Override
    public ArrayList<ColumnMeta> outputSchema() {
        ArrayList<ColumnMeta> finalOutputSchema = new ArrayList<>();
        for (TabCol selectedCol : this.outputSchema) { // Iterate through the selected columns in desired order
            for (ColumnMeta childColMeta : child.outputSchema()) { // Find the corresponding ColumnMeta from child
                if ((selectedCol.getTableName() == null && childColMeta.tableName == null) ||
                        (selectedCol.getTableName() != null
                                && selectedCol.getTableName().equals(childColMeta.tableName)) &&
                                selectedCol.getColumnName().equals(childColMeta.name)) {
                    finalOutputSchema.add(childColMeta);
                    break;
                }
            }
        }
        return finalOutputSchema;
    }
}
