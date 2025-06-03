package edu.sustech.cs307.system;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.exception.ExceptionTypes;
import edu.sustech.cs307.meta.ColumnMeta;
import edu.sustech.cs307.meta.MetaManager;
import edu.sustech.cs307.meta.TableMeta;
import edu.sustech.cs307.storage.BufferPool;
import edu.sustech.cs307.storage.DiskManager;
import org.apache.commons.lang3.StringUtils;
import org.pmw.tinylog.Logger;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DBManager {
    private final MetaManager metaManager;
    private final DiskManager diskManager;
    private final BufferPool bufferPool;
    private final RecordManager recordManager;
    private final IndexManager indexManager; // 添加IndexManager
    private final IndexSynchronizer indexSynchronizer; // 添加索引同步器

    public DBManager(DiskManager diskManager, BufferPool bufferPool, RecordManager recordManager,
            MetaManager metaManager) {
        this.diskManager = diskManager;
        this.bufferPool = bufferPool;
        this.recordManager = recordManager;
        this.metaManager = metaManager;
        this.indexManager = new IndexManager(metaManager, recordManager); // 传入RecordManager引用
        this.indexSynchronizer = new IndexSynchronizer(indexManager, metaManager); // 初始化IndexSynchronizer
    }

    public BufferPool getBufferPool() {
        return bufferPool;
    }

    public RecordManager getRecordManager() {
        return recordManager;
    }

    public DiskManager getDiskManager() {
        return diskManager;
    }

    public MetaManager getMetaManager() {
        return metaManager;
    }

    public IndexManager getIndexManager() {
        return indexManager;
    }

    public IndexSynchronizer getIndexSynchronizer() {
        return indexSynchronizer;
    }

    public boolean isDirExists(String dir) {
        File file = new File(dir);
        return file.exists() && file.isDirectory();
    }

    /**
     * Displays a formatted table listing all available tables in the database.
     * The output is presented in a bordered ASCII table format with centered table
     * names.
     * Each table name is displayed in a separate row within the ASCII borders.
     */
    public void showTables() {
        Set<String> tableNames = metaManager.getTableNames();
        int maxLength = tableNames.stream().mapToInt(String::length).max().orElse(16);
        int columnWidth = Math.max(maxLength, 16);
        String border = "+" + "-".repeat(columnWidth) + "+";
        Logger.info(border);
        Logger.info("|" + StringUtils.center("TABLES", columnWidth) + "|");
        Logger.info(border);
        for (String tableName : tableNames) {
            String centeredTableName = StringUtils.center(tableName, columnWidth);
            Logger.info("|" + centeredTableName + "|");
        }
        Logger.info(border);
    }

    public void descTable(String table_name) throws DBException {
        if (!isTableExists(table_name)) {
            throw new DBException(ExceptionTypes.TableDoesNotExist(table_name));
        }
        TableMeta tableMeta = metaManager.getTable(table_name);
        java.util.List<ColumnMeta> colList = tableMeta.columns_list;

        int maxColumnNameLength = colList.stream().mapToInt(cm -> cm.name.length()).max().orElse(16);
        int maxColumnTypeLength = colList.stream().mapToInt(cm -> cm.type.toString().length()).max().orElse(16);

        int columnNameWidth = Math.max(maxColumnNameLength, 16);
        int columnTypeWidth = Math.max(maxColumnTypeLength, 16);

        String border = "+" + "-".repeat(columnNameWidth) + "+" + "-".repeat(columnTypeWidth) + "+";
        Logger.info(border);
        Logger.info("|" + StringUtils.center("FIELD", columnNameWidth) + "|"
                + StringUtils.center("TYPE", columnTypeWidth) + "|");

        for (ColumnMeta columnMeta : colList) {
            String columnName = columnMeta.name;
            String columnType = columnMeta.type.toString().toUpperCase();
            String centeredColumnName = StringUtils.center(columnName, columnNameWidth);
            String centeredColumnType = StringUtils.center(columnType, columnTypeWidth);
            Logger.info("|" + centeredColumnName + "|" + centeredColumnType + "|");
        }
        Logger.info(border);
    }

    /**
     * Creates a new table in the database with specified name and column metadata.
     * This method sets up both the table metadata and the physical storage
     * structure.
     *
     * @param table_name The name of the table to be created
     * @param columns    List of column metadata defining the table structure
     * @throws DBException If there is an error during table creation
     */
    public void createTable(String table_name, ArrayList<ColumnMeta> columns) throws DBException {
        TableMeta tableMeta = new TableMeta(
                table_name, columns);
        metaManager.createTable(tableMeta);
        String table_folder = String.format("%s/%s", diskManager.getCurrentDir(), table_name); // vulnerable to path
        File file_folder = new File(table_folder);
        if (!file_folder.exists()) {
            file_folder.mkdirs();
        }
        int record_size = 0;
        for (var col : columns) {
            record_size += col.len;
        }
        String data_file = String.format("%s/%s", table_name, "data"); // vulnerable to path
        recordManager.CreateFile(data_file, record_size);
    }

    /**
     * Drops a table from the database by removing its metadata and associated
     * files.
     *
     * @param table_name The name of the table to be dropped
     * @throws DBException If the table directory does not exist or encounters IO
     *                     errors during deletion
     */
    public void dropTable(String table_name) throws DBException {
        if (!isTableExists(table_name)) {
            throw new DBException(ExceptionTypes.TableDoesNotExist(table_name));
        }
        // remove table from meta manager
        metaManager.dropTable(table_name);
        // remove table from disk manager
        String table_folder = String.format("%s/%s", diskManager.getCurrentDir(), table_name); // vulnerable to path
        File file_folder = new File(table_folder);
        if (!file_folder.exists()) {
            throw new DBException(ExceptionTypes.TableDoesNotExist(table_name));
        }
        try {
            deleteDirectory(file_folder);
        } catch (DBException e) {
            throw new DBException(ExceptionTypes.BadIOError("File deletion failed: " + file_folder.getAbsolutePath()));
        }
    }

    /**
     * Recursively deletes a directory and all its contents.
     * If the given file is a directory, it first deletes all its entries
     * recursively.
     * Finally deletes the file/directory itself.
     *
     * @param file The file or directory to be deleted
     * @throws DBException If deletion of any file or directory fails
     */
    private void deleteDirectory(File file) throws DBException {
        if (file.isDirectory()) {
            File[] entries = file.listFiles();
            if (entries != null) {
                for (File entry : entries) {
                    deleteDirectory(entry);
                }
            }
        }
        if (!file.delete()) {
            throw new DBException(ExceptionTypes.BadIOError("File deletion failed: " + file.getAbsolutePath()));
        }
    }

    /**
     * Checks if a table exists in the database.
     *
     * @param table the name of the table to check
     * @return true if the table exists, false otherwise
     */
    public boolean isTableExists(String table) {
        return metaManager.getTableNames().contains(table);
    }

    /**
     * Closes the database manager and performs cleanup operations.
     * This method flushes all pages in the buffer pool, dumps disk manager
     * metadata,
     * and saves meta manager state to JSON format.
     *
     * @throws DBException if an error occurs during the closing process
     */
    public void closeDBManager() throws DBException {
        this.bufferPool.FlushAllPages("");
        DiskManager.dump_disk_manager_meta(this.diskManager);
        this.metaManager.saveToJson();
    }

    /**
     * 获取数据库中所有表的名称列表（用于HTTP API）
     * 
     * @return 包含所有表名的列表
     */
    public List<String> getTableNamesList() {
        return new ArrayList<>(metaManager.getTableNames());
    }

    /**
     * 获取指定表的列信息（用于HTTP API）
     * 
     * @param table_name 表名
     * @return 包含列信息的列表，每个元素是包含列名和类型的Map
     * @throws DBException 如果表不存在
     */
    public List<Map<String, Object>> getTableColumns(String table_name) throws DBException {
        if (!isTableExists(table_name)) {
            throw new DBException(ExceptionTypes.TableDoesNotExist(table_name));
        }
        TableMeta tableMeta = metaManager.getTable(table_name);
        List<ColumnMeta> colList = tableMeta.columns_list;

        List<Map<String, Object>> columns = new ArrayList<>();
        for (ColumnMeta columnMeta : colList) {
            Map<String, Object> columnInfo = new HashMap<>();
            columnInfo.put("Field", columnMeta.name);
            columnInfo.put("Type", columnMeta.type.toString().toUpperCase());

            String indexValue = "";

            if (tableMeta.getIndexNameToColumn() != null) {
                for (Map.Entry<String, String> entry : tableMeta.getIndexNameToColumn().entrySet()) {
                    if (entry.getValue().equals(columnMeta.name)) {
                        indexValue = entry.getKey(); // 返回索引名称
                        break;
                    }
                }
            }

            if (indexValue.isEmpty() && tableMeta.isPrimaryKey(columnMeta.name)) {
                indexValue = "primary key";
            }

            columnInfo.put("Index", indexValue);
            columns.add(columnInfo);
        }
        return columns;
    }
}
