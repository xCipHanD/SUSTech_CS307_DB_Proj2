package edu.sustech.cs307.physicalOperator;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.exception.ExceptionTypes;
import edu.sustech.cs307.index.InMemoryOrderedIndex;
import edu.sustech.cs307.record.Record;
import edu.sustech.cs307.record.RecordFileHandle;
import edu.sustech.cs307.record.RID;
import edu.sustech.cs307.system.DBManager;
import edu.sustech.cs307.tuple.TableTuple;
import edu.sustech.cs307.tuple.Tuple;
import edu.sustech.cs307.meta.ColumnMeta;
import edu.sustech.cs307.meta.TableMeta;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;
import edu.sustech.cs307.value.Value; // Required for Entry<Value, RID>

public class InMemoryIndexScanOperator implements PhysicalOperator {
    private InMemoryOrderedIndex index;
    private DBManager dbManager;
    private String tableName;
    private TableMeta tableMeta;
    private RecordFileHandle fileHandle;
    private Iterator<Entry<Value, RID>> iterator;
    private Record currentRecord;
    private RID currentRID;
    private boolean isOpen = false;

    public InMemoryIndexScanOperator(InMemoryOrderedIndex index, DBManager dbManager) throws DBException {
        this.index = index;
        this.dbManager = dbManager;
        if (index == null) {
            throw new DBException(
                    ExceptionTypes.InvalidOperation("Index cannot be null for InMemoryIndexScanOperator"));
        }
        this.tableName = index.getTableName();
        if (this.tableName == null) {
            throw new DBException(ExceptionTypes
                    .InvalidOperation("Table name not available from index for InMemoryIndexScanOperator"));
        }
        this.tableMeta = dbManager.getMetaManager().getTable(this.tableName);
        if (this.tableMeta == null) {
            throw new DBException(ExceptionTypes.TableDoesNotExist(this.tableName));
        }
    }

    @Override
    public boolean hasNext() throws DBException {
        if (!isOpen || iterator == null) {
            return false;
        }
        return iterator.hasNext();
    }

    @Override
    public void Begin() throws DBException {
        if (index == null) {
            throw new DBException(ExceptionTypes.InvalidOperation("Index not set for InMemoryIndexScanOperator"));
        }
        this.fileHandle = dbManager.getRecordManager().OpenFile(this.tableName);
        this.iterator = index.getFullIterator(); // Iterate over all entries in the index
        this.isOpen = true;
        this.currentRecord = null;
        this.currentRID = null;
    }

    @Override
    public void Next() throws DBException {
        if (!isOpen || iterator == null || !iterator.hasNext()) {
            currentRecord = null;
            currentRID = null;
            return;
        }
        Entry<Value, RID> entry = iterator.next();
        this.currentRID = entry.getValue();
        if (this.currentRID == null) { // Should not happen if index is consistent
            currentRecord = null;
            throw new DBException(ExceptionTypes.RecordNotFound("Null RID found in index for table " + tableName));
        }
        currentRecord = fileHandle.GetRecord(this.currentRID);
    }

    @Override
    public Tuple Current() {
        if (!isOpen || currentRecord == null || currentRID == null || tableMeta == null || tableName == null) {
            return null;
        }
        return new TableTuple(this.tableName, this.tableMeta, this.currentRecord, this.currentRID);
    }

    @Override
    public void Close() throws DBException {
        if (!isOpen) {
            return;
        }
        if (fileHandle != null) {
            dbManager.getRecordManager().CloseFile(fileHandle);
        }
        isOpen = false;
        iterator = null;
        currentRecord = null;
        currentRID = null;
        fileHandle = null;
    }

    @Override
    public ArrayList<ColumnMeta> outputSchema() {
        return tableMeta != null ? tableMeta.columns_list : new ArrayList<>();
    }
}
