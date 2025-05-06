package Lab11;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.meta.MetaManager;
import edu.sustech.cs307.record.RecordFileHandle;
import edu.sustech.cs307.record.RecordPageHandle;
import edu.sustech.cs307.record.RecordPageHeader;
import edu.sustech.cs307.storage.BufferPool;
import edu.sustech.cs307.storage.DiskManager;
import edu.sustech.cs307.system.DBManager;
import edu.sustech.cs307.system.RecordManager;
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
            // TODO: complete the code here
            for (int i = 0; i < pageCount; i++) {
                RecordPageHandle pageHandle = fileHandle.FetchPageHandle(i);
                

            }


        } catch (DBException e) {
            Logger.error(e.getMessage());
            Logger.error("An error occurred during initializing. Exiting....");
        }
    }
}
