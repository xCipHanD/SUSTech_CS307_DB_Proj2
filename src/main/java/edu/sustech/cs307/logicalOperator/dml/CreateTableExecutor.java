package edu.sustech.cs307.logicalOperator.dml;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.exception.ExceptionTypes;
import edu.sustech.cs307.meta.ColumnMeta;
import edu.sustech.cs307.meta.TableMeta;
import edu.sustech.cs307.system.DBManager;
import edu.sustech.cs307.value.Value;
import edu.sustech.cs307.value.ValueType;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import org.pmw.tinylog.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class CreateTableExecutor implements DMLExecutor {

    private final CreateTable createTableStmt;
    private final DBManager dbManager;
    private final String sql;

    public CreateTableExecutor(CreateTable createTable, DBManager dbManager, String sql) {
        this.createTableStmt = createTable;
        this.dbManager = dbManager;
        this.sql = sql;
    }

    @Override
    public void execute() throws DBException {
        String table = createTableStmt.getTable().getName();
        ArrayList<ColumnMeta> colMapping = new ArrayList<>();
        int offset = 0;
        if (null == createTableStmt.getColumnDefinitions()) {
            throw new DBException(ExceptionTypes.TableHasNoColumn(table));
        }

        String primaryKeyColumnName = null;

        for (var col : createTableStmt.getColumnDefinitions()) {
            String colName = col.getColumnName();
            boolean isPrimaryKey = false;
            if (col.getColumnSpecs() != null) {
                for (String spec : col.getColumnSpecs()) {
                    if ("PRIMARY_KEY".equalsIgnoreCase(spec)) {
                        isPrimaryKey = true;
                        break;
                    }
                }
            }

            if (colName.isEmpty() || colName.length() > 16) {
                throw new DBException(
                        ExceptionTypes.InvalidSQL(sql, String.format("INVALID COLUMN NAME = %s", colName)));
            }

            if (isPrimaryKey) {
                if (primaryKeyColumnName != null) {
                    throw new DBException(ExceptionTypes.InvalidSQL(sql,
                            "Only one column can be set as primary key/index in current implementation"));
                }
                primaryKeyColumnName = colName;
            }

            ColDataType colType = col.getColDataType();
            if (colType.getDataType().equalsIgnoreCase("char")) {
                colMapping.add(new ColumnMeta(table, colName, ValueType.CHAR, Value.CHAR_SIZE, offset));
                offset += Value.CHAR_SIZE;
            } else if (colType.getDataType().equalsIgnoreCase("int")
                    || colType.getDataType().equalsIgnoreCase("integer")) {
                colMapping.add(new ColumnMeta(table, colName, ValueType.INTEGER, Value.INT_SIZE, offset));
                offset += Value.INT_SIZE;
            } else if (colType.getDataType().equalsIgnoreCase("float")) {
                colMapping.add(new ColumnMeta(table, colName, ValueType.FLOAT, Value.FLOAT_SIZE, offset));
                offset += Value.FLOAT_SIZE;
            } else if (colType.getDataType().equalsIgnoreCase("double")) {
                colMapping.add(new ColumnMeta(table, colName, ValueType.DOUBLE, Value.DOUBLE_SIZE, offset));
                offset += Value.DOUBLE_SIZE;
            } else {
                throw new DBException(ExceptionTypes.UnsupportedCommand(
                        String.format("CREATE TABLE %s with unsupported type %s", table, colType.getDataType())));
            }
        }

        // 创建表
        dbManager.createTable(table, colMapping);

        String indexColumnName = primaryKeyColumnName;

        if (indexColumnName != null) {
            try {
                // 获取刚创建的表的元数据
                TableMeta tableMeta = dbManager.getMetaManager().getTable(table);
                if (tableMeta != null) {
                    // 为选定的列添加索引定义
                    Map<String, TableMeta.IndexType> indexes = tableMeta.getIndexes();
                    if (indexes == null) {
                        indexes = new HashMap<>();
                    }
                    indexes.put(indexColumnName, TableMeta.IndexType.BTREE);
                    tableMeta.setIndexes(indexes);

                    // 保存元数据
                    dbManager.getMetaManager().saveToJson();

                    try {
                        dbManager.getIndexManager().createIndex(table, indexColumnName);
                        Logger.info("Successfully created table: {} with B+Tree index on column: {}", 
                                table, indexColumnName);
                    } catch (DBException indexException) {
                        Logger.warn("Failed to create actual B+Tree index for {}.{}: {}", 
                                table, indexColumnName, indexException.getMessage());
                        Logger.info("Table {} created with index metadata but no actual index instance", table);
                    }

                    if (primaryKeyColumnName != null) {
                        Logger.info("Successfully created table: {} with index on primary key column: {}",
                                table, indexColumnName);
                    } else {
                        Logger.info("Successfully created table: {} with index on first column: {}",
                                table, indexColumnName);
                    }
                }
            } catch (DBException e) {
                Logger.warn("Failed to create index definition for {}.{}: {}", table, indexColumnName, e.getMessage());
                Logger.info("Successfully created table: {} (without index)", table);
            }
        } else {
            Logger.info("Successfully created table: {}", table);
        }
    }

}
