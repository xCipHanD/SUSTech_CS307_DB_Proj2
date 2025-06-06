package edu.sustech.cs307.physicalOperator;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.meta.ColumnMeta;
import edu.sustech.cs307.meta.TableMeta;
import edu.sustech.cs307.record.RID;
import edu.sustech.cs307.record.Record;
import edu.sustech.cs307.record.RecordFileHandle;
import edu.sustech.cs307.system.DBManager;
import edu.sustech.cs307.tuple.TableTuple;
import edu.sustech.cs307.tuple.Tuple;
import edu.sustech.cs307.index.Index;
import edu.sustech.cs307.index.BPlusTreeIndex;
import edu.sustech.cs307.value.Value;
import org.pmw.tinylog.Logger;

import java.util.ArrayList;
import java.util.List;

public class IndexScanOperator implements PhysicalOperator {
    private String tableName;
    private String columnName; // The column on which the index is defined and used for searching
    private DBManager dbManager;
    private TableMeta tableMeta;
    private RecordFileHandle fileHandle;
    private Record currentRecord;
    private Value searchKey; // The specific key value to search for in the index
    private boolean isOpen = false;
    private Index index; // Holds the actual index structure (e.g., BPlusTreeIndex)
    private List<RID> matchingRIDs;
    private int currentRIDIndex;

    // Constructor for an exact match query using an index
    public IndexScanOperator(String tableName, String columnName, Value searchKey, DBManager dbManager, Index index) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.searchKey = searchKey;
        this.dbManager = dbManager;
        this.index = index; // Assign the provided index
        try {
            this.tableMeta = dbManager.getMetaManager().getTable(tableName);
        } catch (DBException e) {
            Logger.error("Failed to get table metadata for IndexScanOperator: " + e.getMessage());
            // Optionally, rethrow or handle more gracefully
        }
    }

    @Override
    public boolean hasNext() {
        if (!isOpen) {
            return false;
        }
        return currentRIDIndex < matchingRIDs.size();
    }

    @Override
    public void Begin() throws DBException {
        try {
            fileHandle = dbManager.getRecordManager().OpenFile(tableName);
            // The index is now passed via constructor, so we use it directly.
            if (this.index == null) {
                // Fallback or error if no index is provided.
                // For now, let's assume an index is always provided if this operator is chosen.
                // Alternatively, try to fetch it here if not passed (though less clean).
                // this.index = dbManager.getIndexManager().getIndex(tableName, columnName); //
                // Hypothetical
                throw new DBException(edu.sustech.cs307.exception.ExceptionTypes
                        .InvalidOperation("IndexScanOperator requires an Index."));
            }

            matchingRIDs = findMatchingRIDs(); // Use the provided index to find RIDs
            currentRIDIndex = 0;
            isOpen = true;
        } catch (DBException e) {
            Logger.error("Failed to begin index scan: " + e.getMessage());
            isOpen = false;
            throw e;
        }
    }

    // Uses the associated index to find RIDs matching the searchKey.
    private List<RID> findMatchingRIDs() throws DBException {
        if (this.index != null && this.searchKey != null) {
            // Assuming the Index interface has a search method.
            // Our BPlusTreeIndex (simulated with TreeMap) has search(Value key).
            return this.index.search(this.searchKey);
        }
        // Fallback to an empty list if no index or searchKey is available,
        // though Begin() should ideally prevent this state.
        Logger.warn("Index or searchKey is null in findMatchingRIDs for table " + tableName);
        return new ArrayList<>();
    }

    @Override
    public void Next() throws DBException {
        if (!isOpen || !hasNext()) {
            currentRecord = null;
            return;
        }

        try {
            RID rid = matchingRIDs.get(currentRIDIndex++);
            currentRecord = fileHandle.GetRecord(rid);
        } catch (DBException e) {
            Logger.error("Failed to get next record: " + e.getMessage());
            currentRecord = null;
            throw e;
        }
    }

    @Override
    public Tuple Current() {
        if (!isOpen || currentRecord == null) {
            return null;
        }

        RID rid = matchingRIDs.get(currentRIDIndex - 1);
        return new TableTuple(tableName, tableMeta, currentRecord, rid);
    }

    @Override
    public void Close() {
        if (!isOpen) {
            return;
        }

        try {
            dbManager.getRecordManager().CloseFile(fileHandle);
        } catch (DBException e) {
            Logger.error("Failed to close file handle: " + e.getMessage());
        }

        fileHandle = null;
        currentRecord = null;
        matchingRIDs = null;
        isOpen = false;
    }

    @Override
    public ArrayList<ColumnMeta> outputSchema() {
        return tableMeta != null ? tableMeta.columns_list : new ArrayList<>();
    }
}
