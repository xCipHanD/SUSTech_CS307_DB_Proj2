package Lab11;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.meta.MetaManager;
import edu.sustech.cs307.meta.TableMeta;
import edu.sustech.cs307.record.*;
import edu.sustech.cs307.record.Record;
import edu.sustech.cs307.storage.BufferPool;
import edu.sustech.cs307.storage.DiskManager;
import edu.sustech.cs307.system.DBManager;
import edu.sustech.cs307.system.RecordManager;
import edu.sustech.cs307.tuple.TableTuple;
import edu.sustech.cs307.value.Value;
import org.pmw.tinylog.Logger;

import java.util.HashMap;
import java.util.Map;

public class ScanExercise {

    public static void main(String[] args) {

        try {
            Map<String, Integer> disk_manager_meta = new HashMap<>(
                    DiskManager.read_disk_manager_meta());

            DiskManager diskManager = new DiskManager("CS307-DB", disk_manager_meta);
            BufferPool bufferPool = new BufferPool(256 * 512, diskManager);
            RecordManager recordManager = new RecordManager(diskManager, bufferPool);
            MetaManager metaManager = new MetaManager("CS307-DB/meta");
            DBManager dbManager = new DBManager(diskManager, bufferPool, recordManager, metaManager);

            RecordFileHandle fileHandle = dbManager.getRecordManager().OpenFile("t");
            int pageCount = fileHandle.getFileHeader().getNumberOfPages();
            int recordsPerPage = fileHandle.getFileHeader().getNumberOfRecordsPrePage();
            System.out.println("Page Count: " + pageCount);
            System.out.println("Records Per Page: " + recordsPerPage);
            TableMeta tableMeta = metaManager.getTable("t");
            for (int i = 1; i <= pageCount; i++) {
                RecordPageHandle pageHandle = fileHandle.FetchPageHandle(i);
                for (int j = 0; j < recordsPerPage; j++) {
                    if (BitMap.isSet(pageHandle.bitmap, j)) {
                        RID rid = new RID(i, j);
                        Record record = fileHandle.GetRecord(rid);
                        TableTuple tableTuple = new TableTuple("t", tableMeta, record, rid);
                        Value[] values = tableTuple.getValues();
                        System.out.print("Page " + i + ", Slot " + j + ", Values: ");
                        for (Value value : values) {
                            System.out.print(value.toString() + " ");
                        }
                        System.out.println();
                    }
                }
            }
        } catch (DBException e) {
            Logger.error(e.getMessage());
            Logger.error("An error occurred during initializing. Exiting....");
        }
    }
}
