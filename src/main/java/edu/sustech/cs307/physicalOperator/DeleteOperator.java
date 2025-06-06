package edu.sustech.cs307.physicalOperator;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.meta.ColumnMeta;
import edu.sustech.cs307.record.RID;
import edu.sustech.cs307.record.Record;
import edu.sustech.cs307.record.RecordFileHandle;
import edu.sustech.cs307.system.DBManager;
import edu.sustech.cs307.system.IndexSynchronizer;
import edu.sustech.cs307.tuple.TableTuple;
import edu.sustech.cs307.tuple.TempTuple;
import edu.sustech.cs307.tuple.Tuple;
import edu.sustech.cs307.value.Value;
import edu.sustech.cs307.value.ValueType;
import org.pmw.tinylog.Logger;

import java.util.ArrayList;

public class DeleteOperator implements PhysicalOperator {
    private final PhysicalOperator child;
    private final String tableName;
    private final DBManager dbManager;
    private RecordFileHandle fileHandle;
    private boolean isOpen = false;
    private int deletedCount = 0;
    private boolean isComplete = false;
    private boolean outputed = false;

    public DeleteOperator(String tableName, PhysicalOperator child, DBManager dbManager) {
        this.tableName = tableName;
        this.child = child;
        this.dbManager = dbManager;
    }

    @Override
    public void Begin() throws DBException {
        Logger.debug("DeleteOperator.Begin() called for table: " + tableName);
        try {
            fileHandle = dbManager.getRecordManager().OpenFile(tableName);
            child.Begin();
            isOpen = true;
            deletedCount = 0;
            isComplete = false;
            outputed = false;
        } catch (DBException e) {
            Logger.error("Failed to open file handle for table: " + tableName, e);
            isOpen = false;
            throw e;
        }
    }

    @Override
    public boolean hasNext() throws DBException {
        if (!isOpen) {
            Logger.debug("DeleteOperator.hasNext() returning false: operator not open for table " + tableName);
            return false;
        }
        if (outputed) {
            Logger.debug(
                    "DeleteOperator.hasNext() returning false: output tuple already produced for table " + tableName);
            return false;
        }
        if (isComplete) {
            Logger.debug("DeleteOperator.hasNext() returning true: deletions complete, ready to output tuple for table "
                    + tableName);
            return true; // Allow one output tuple after deletions
        }
        boolean hasNext = child.hasNext();
        if (!hasNext) {
            isComplete = true;
            Logger.debug(
                    "DeleteOperator.hasNext() setting isComplete: no more tuples to delete for table " + tableName);
            return true; // Signal output tuple is ready
        }
        Logger.debug("DeleteOperator.hasNext() for table " + tableName + ": " + hasNext);
        return hasNext;
    }

    @Override
    public void Next() throws DBException {
        if (!isOpen) {
            Logger.debug("DeleteOperator.Next() skipped: operator not open for table " + tableName);
            return;
        }
        if (isComplete || outputed) {
            Logger.debug("DeleteOperator.Next() skipped: deletions complete or output produced for table " + tableName);
            outputed = true; // Ensure output tuple is produced only once
            return;
        }

        // 获取索引同步器
        IndexSynchronizer indexSynchronizer = new IndexSynchronizer(
                dbManager.getIndexManager(),
                dbManager.getMetaManager());

        try {
            while (child.hasNext()) {
                child.Next();
                Tuple tuple = child.Current();
                if (tuple instanceof TableTuple tableTuple) {
                    RID rid = tableTuple.getRID();
                    if (rid != null && fileHandle.IsRecord(rid)) {
                        Record recordToDelete = fileHandle.GetRecord(rid);

                        fileHandle.DeleteRecord(rid);
                        deletedCount++;
                        Logger.debug("Deleted record with RID: " + rid + " from table " + tableName);

                        try {
                            indexSynchronizer.onRecordDeleted(tableName, recordToDelete, rid);
                        } catch (DBException e) {
                            Logger.warn("Failed to update indexes after delete: {}", e.getMessage());
                        }
                    } else {
                        Logger.warn("Skipping invalid RID or non-existent record: " + rid + " in table " + tableName);
                    }
                } else {
                    Logger.warn("Skipping non-TableTuple or null tuple from child operator in table " + tableName);
                }
            }
            isComplete = true;
            Logger.debug("DeleteOperator.Next() completed deletions for table " + tableName + ", deletedCount: "
                    + deletedCount);
        } catch (DBException e) {
            Logger.error("Error processing next tuple for deletion in table " + tableName, e);
            throw e;
        }
    }

    @Override
    public Tuple Current() {
        if (!isComplete || outputed) {
            Logger.debug("DeleteOperator.Current() returning null: not ready or already output for table " + tableName);
            return null;
        }
        ArrayList<Value> values = new ArrayList<>();
        values.add(new Value(deletedCount, ValueType.INTEGER));
        outputed = true;
        Logger.debug("DeleteOperator.Current() returning tuple with deletedCount: " + deletedCount + " for table "
                + tableName);
        return new TempTuple(values);
    }

    @Override
    public void Close() {
        if (!isOpen) {
            Logger.debug("DeleteOperator.Close() skipped: operator not open for table " + tableName);
            return;
        }
        try {
            if (fileHandle != null) {
                dbManager.getRecordManager().CloseFile(fileHandle);
            }
            child.Close();
            isOpen = false;
            Logger.debug(
                    "DeleteOperator.Close() called for table " + tableName + ", deleted " + deletedCount + " records");
        } catch (DBException e) {
            Logger.error("Error closing DeleteOperator for table " + tableName, e);
        }
    }

    @Override
    public ArrayList<ColumnMeta> outputSchema() {
        ArrayList<ColumnMeta> outputSchema = new ArrayList<>();
        outputSchema.add(new ColumnMeta("delete", "numberOfDeletedRows", ValueType.INTEGER, 0, 0));
        return outputSchema;
    }

    public int getDeletedCount() {
        return deletedCount;
    }
}