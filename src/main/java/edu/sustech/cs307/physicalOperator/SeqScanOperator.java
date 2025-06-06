package edu.sustech.cs307.physicalOperator;

import edu.sustech.cs307.meta.ColumnMeta;

import edu.sustech.cs307.system.DBManager;
import edu.sustech.cs307.tuple.TableTuple;
import edu.sustech.cs307.tuple.Tuple;
import edu.sustech.cs307.meta.TableMeta;
import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.exception.ExceptionTypes;
import edu.sustech.cs307.record.RID;
import edu.sustech.cs307.record.RecordPageHandle;
import edu.sustech.cs307.record.BitMap;
import edu.sustech.cs307.record.Record;
import edu.sustech.cs307.record.RecordFileHandle;
import org.pmw.tinylog.Logger;

import java.util.ArrayList;

public class SeqScanOperator implements PhysicalOperator {
    private String tableName;
    private DBManager dbManager;
    private TableMeta tableMeta;
    private RecordFileHandle fileHandle;
    private Record currentRecord;
    private RID currentRID; // 添加这个字段来保存当前记录的正确RID

    private int currentPageNum;
    private int currentSlotNum;
    private int totalPages;
    private int recordsPerPage;
    private boolean isOpen = false;

    public SeqScanOperator(String tableName, DBManager dbManager) throws DBException {
        this.tableName = tableName;
        this.dbManager = dbManager;
        try {
            this.tableMeta = dbManager.getMetaManager().getTable(tableName);
            if (this.tableMeta == null) {
                throw new DBException(ExceptionTypes.TableDoesNotExist(tableName));
            }
        } catch (DBException e) {
            Logger.error("Failed to get table metadata: " + e.getMessage());
            throw e; // 重新抛出异常以便上层处理
        }
    }

    @Override
    public boolean hasNext() throws DBException {
        if (!isOpen)
            return false;

        try {
            // Check if current page and slot are valid, and if there are more records
            if (currentPageNum <= totalPages) {
                while (currentPageNum <= totalPages) {
                    RecordPageHandle pageHandle = fileHandle.FetchPageHandle(currentPageNum);
                    while (currentSlotNum < recordsPerPage) {
                        if (BitMap.isSet(pageHandle.bitmap, currentSlotNum)) {
                            return true; // Found next record
                        }
                        currentSlotNum++;
                    }
                    currentPageNum++;
                    currentSlotNum = 0; // Reset slot num for new page
                }
            }
        } catch (DBException e) {
            Logger.error("Error checking for next record: " + e.getMessage());
            throw e; // 传播异常以便上层处理
        }
        return false; // No more records
    }

    @Override
    public void Begin() throws DBException {
        try {
            fileHandle = dbManager.getRecordManager().OpenFile(tableName);
            totalPages = fileHandle.getFileHeader().getNumberOfPages();
            recordsPerPage = fileHandle.getFileHeader().getNumberOfRecordsPrePage();
            currentPageNum = 1; // Start from first page
            currentSlotNum = 0; // Start from first slot
            isOpen = true;
        } catch (DBException e) {
            Logger.error("Failed to begin scan: " + e.getMessage());
            isOpen = false;
            throw e; // 重新抛出异常以便上层处理
        }
    }

    @Override
    public void Next() throws DBException {
        if (!isOpen)
            return;

        try {
            if (hasNext()) { // Advance to the next record
                RID rid = new RID(currentPageNum, currentSlotNum);
                currentRecord = fileHandle.GetRecord(rid);
                currentRID = rid; // 更新currentRID为当前记录的RID
                currentSlotNum++;
                if (currentSlotNum >= recordsPerPage) {
                    currentPageNum++;
                    currentSlotNum = 0;
                }

                // 修改从readonly到false，因为如果页面被修改，我们需要将其标记为脏页
                fileHandle.UnpinPageHandle(currentPageNum, false);
            } else {
                currentRecord = null;
            }
        } catch (DBException e) {
            Logger.error("Error getting next record: " + e.getMessage());
            currentRecord = null;
            throw e; // 传播异常以便上层处理
        }
    }

    @Override
    public Tuple Current() {
        if (!isOpen || currentRecord == null) {
            return null;
        }
        return new TableTuple(tableName, tableMeta, currentRecord, currentRID);
    }

    @Override
    public void Close() {
        if (!isOpen)
            return;

        try {
            if (fileHandle != null) {
                dbManager.getRecordManager().CloseFile(fileHandle);
            }
        } catch (DBException e) {
            Logger.error("Failed to close file handle: " + e.getMessage());
            // 在Close方法中不需要抛出异常，因为这通常在finally块中调用
        }

        fileHandle = null;
        currentRecord = null;
        isOpen = false;
    }

    @Override
    public ArrayList<ColumnMeta> outputSchema() {
        return tableMeta != null ? tableMeta.columns_list : new ArrayList<>();
    }

    public RecordFileHandle getFileHandle() {
        return fileHandle;
    }
}
