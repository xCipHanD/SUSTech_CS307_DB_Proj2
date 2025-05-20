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
import edu.sustech.cs307.value.Value;
import org.pmw.tinylog.Logger;

import java.util.ArrayList;
import java.util.List;

public class IndexScanOperator implements PhysicalOperator {
    private String tableName;
    private String columnName;
    private DBManager dbManager;
    private TableMeta tableMeta;
    private RecordFileHandle fileHandle;
    private Record currentRecord;
    private Value searchKey;
    private boolean isOpen = false;
    private Index index;
    private List<RID> matchingRIDs;
    private int currentRIDIndex;

    public IndexScanOperator(String tableName, String columnName, Value searchKey, DBManager dbManager) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.searchKey = searchKey;
        this.dbManager = dbManager;
        try {
            this.tableMeta = dbManager.getMetaManager().getTable(tableName);
        } catch (DBException e) {
            Logger.error("Failed to get table metadata: " + e.getMessage());
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

            // 在实际应用中，你会从索引管理器中获取索引实例
            // 这里简化处理，假设我们有一个可用的索引
            // 从索引中查找匹配searchKey的所有RIDs
            matchingRIDs = findMatchingRIDs();
            currentRIDIndex = 0;
            isOpen = true;
        } catch (DBException e) {
            Logger.error("Failed to begin index scan: " + e.getMessage());
            isOpen = false;
            throw e; // 重新抛出异常以便上层处理
        }
    }

    private List<RID> findMatchingRIDs() {
        // 这里应该使用真实的索引查找实现
        // 现在只是返回一个空列表作为占位符
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
