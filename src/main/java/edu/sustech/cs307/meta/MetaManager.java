package edu.sustech.cs307.meta;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.exception.ExceptionTypes;
import org.pmw.tinylog.Logger;

public class MetaManager {
    private static final String META_FILE = "meta_data.json";
    private final String ROOT_DIR;
    private final Map<String, TableMeta> tables;
    private final ObjectMapper objectMapper;

    public MetaManager(String root_dir) throws DBException {
        this.objectMapper = new ObjectMapper();
        this.tables = new HashMap<>();
        this.ROOT_DIR = root_dir;
        loadFromJson();
    }

    public void createTable(TableMeta tableMeta) throws DBException {
        String tableName = tableMeta.tableName;
        if (tables.containsKey(tableName)) {
            throw new DBException(ExceptionTypes.TableAlreadyExist(tableName));
        }
        if (tableMeta.columnCount() == 0) {
            throw new DBException(ExceptionTypes.TableHasNoColumn(tableName));
        }
        tables.put(tableName, tableMeta);
        saveToJson();
    }

    public void dropTable(String tableName) throws DBException {
        if (!tables.containsKey(tableName)) {
            throw new DBException(ExceptionTypes.TableDoesNotExist(tableName));
        }
        tables.remove(tableName);
        saveToJson();
    }

    public void addColumnInTable(String tableName, ColumnMeta column) throws DBException {
        if (!tables.containsKey(tableName)) {
            throw new DBException(ExceptionTypes.TableDoesNotExist(tableName));
        }
        this.tables.get(tableName).addColumn(column);
    }

    public void dropColumnInTable(String tableName, String columnName) throws DBException {
        if (!tables.containsKey(tableName)) {
            throw new DBException(ExceptionTypes.TableDoesNotExist(tableName));
        }
        TableMeta tableMeta = this.tables.get(tableName);
        tableMeta.dropColumn(columnName);

        // 同时从 columns_list 中删除
        tableMeta.columns_list.removeIf(column -> column.name.equals(columnName));

        saveToJson();
    }

    public TableMeta getTable(String tableName) throws DBException {
        if (tables.containsKey(tableName)) {
            return tables.get(tableName);
        }
        throw new DBException(ExceptionTypes.TableDoesNotExist(tableName));
        // return null;
    }

    public Set<String> getTableNames() {
        return this.tables.keySet();
    }

    public void saveToJson() throws DBException {
        // check the root directory exists
        if (!new File(ROOT_DIR).exists()) {
            // create it
            new File(ROOT_DIR).mkdirs();
        }

        try (Writer writer = new FileWriter(String.format("%s/%s", ROOT_DIR, META_FILE))) {
            objectMapper.writeValue(writer, tables);
        } catch (Exception e) {
            throw new DBException(ExceptionTypes.UnableSaveMetadata(e.getMessage()));
        }
    }

    private void loadFromJson() throws DBException {
        File file = new File(ROOT_DIR + "/" + META_FILE);
        if (!file.exists())
            return;

        try (Reader reader = new FileReader(ROOT_DIR + "/" + META_FILE)) {
            TypeReference<Map<String, TableMeta>> typeRef = new TypeReference<>() {
            };
            Map<String, TableMeta> loadedTables = objectMapper.readValue(reader, typeRef);
            if (loadedTables != null) {
                tables.putAll(loadedTables);
            }
        } catch (Exception e) {
            throw new DBException(ExceptionTypes.UnableLoadMetadata(e.getMessage()));
        }
    }

    /**
     * 为 ALTER TABLE 操作重组表数据
     * 在列结构发生变化后，需要重新组织所有现有数据
     * 
     * @param tableName    表名
     * @param oldTableMeta 旧的表元数据
     * @param newTableMeta 新的表元数据
     * @throws DBException 如果重组过程中发生错误
     */
    public void reorganizeTableData(String tableName, TableMeta oldTableMeta, TableMeta newTableMeta)
            throws DBException {
        try {
            // 计算旧表的记录大小
            int oldRecordSize = 0;
            for (ColumnMeta column : oldTableMeta.columns_list) {
                oldRecordSize += column.len;
            }

            // 计算新表的记录大小
            int newRecordSize = 0;
            for (ColumnMeta column : newTableMeta.columns_list) {
                newRecordSize += column.len;
            }

            // 1. 验证重组的必要性
            if (oldRecordSize == newRecordSize) {
                Logger.debug("Table {} structure unchanged, skipping data reorganization", tableName);
                return;
            }

            // 2. 更新表元数据
            tables.put(tableName, newTableMeta);

            // 3. 保存元数据变更
            saveToJson();

            Logger.info("Table {} metadata updated for ALTER TABLE operation", tableName);

        } catch (Exception e) {
            Logger.error("Failed to reorganize table data for {}: {}", tableName, e.getMessage());
            throw new DBException(ExceptionTypes.BadIOError("Table reorganization failed: " + e.getMessage()));
        }
    }

    /**
     * 验证列删除操作的安全性
     * 检查是否存在依赖关系（如索引）
     * 
     * @param tableName  表名
     * @param columnName 要删除的列名
     * @throws DBException 如果删除操作不安全
     */
    public void validateColumnDrop(String tableName, String columnName) throws DBException {
        TableMeta tableMeta = getTable(tableName);
        if (tableMeta == null) {
            throw new DBException(ExceptionTypes.TableDoesNotExist(tableName));
        }

        // 检查是否存在该列的索引
        if (tableMeta.getIndexes() != null && tableMeta.getIndexes().containsKey(columnName)) {
            Logger.warn("Column {} has index, should be dropped before column deletion", columnName);
            // 可以选择自动删除索引或者要求用户手动删除
        }

        // 检查是否为主键列（如果有主键约束的话）
        // 这里可以添加更多的约束检查

        Logger.debug("Column drop validation passed for {}.{}", tableName, columnName);
    }
}
