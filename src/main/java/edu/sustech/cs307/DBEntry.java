package edu.sustech.cs307;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.logicalOperator.LogicalOperator;
import edu.sustech.cs307.meta.ColumnMeta;
import edu.sustech.cs307.meta.MetaManager;
import edu.sustech.cs307.optimizer.LogicalPlanner;
import edu.sustech.cs307.optimizer.PhysicalPlanner;
import edu.sustech.cs307.physicalOperator.PhysicalOperator;
import edu.sustech.cs307.storage.BufferPool;
import edu.sustech.cs307.storage.DiskManager;
import edu.sustech.cs307.system.DBManager;
import edu.sustech.cs307.system.RecordManager;
import edu.sustech.cs307.tuple.Tuple;
import edu.sustech.cs307.value.Value;

import org.apache.commons.lang3.StringUtils;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.terminal.TerminalBuilder;
import org.pmw.tinylog.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class DBEntry {
    public static final String DB_NAME = "CS307-DB";
    // for now, we use 256 * 512 * 4096 bytes = 512MB as the pool size
    public static final int POOL_SIZE = 256 * 512;

    public static void printHelp() {
        Logger.info("Type 'exit' to exit the program.");
        Logger.info("Type 'help' to see this message again.");
    }

    public static void main(String[] args) throws DBException {
        Logger.getConfiguration().formatPattern("{date: HH:mm:ss.SSS} {level}: {message}").activate();

        Logger.info("Hello, This is CS307-DB!");
        Logger.info("Initializing...");
        DBManager dbManager = null;
        try {
            Map<String, Integer> disk_manager_meta = new HashMap<>(DiskManager.read_disk_manager_meta());
            DiskManager diskManager = new DiskManager(DB_NAME, disk_manager_meta);
            BufferPool bufferPool = new BufferPool(POOL_SIZE, diskManager);
            RecordManager recordManager = new RecordManager(diskManager, bufferPool);
            MetaManager metaManager = new MetaManager(DB_NAME + "/meta");
            dbManager = new DBManager(diskManager, bufferPool, recordManager, metaManager);

        } catch (DBException e) {
            Logger.error(e.getMessage());
            Logger.error("An error occurred during initializing. Exiting....");
            return;
        }
        String sql = "";
        boolean running = true;
        // initialize LineReader once to retain buffer across iterations
        LineReader scanner;
        try {
            scanner = LineReaderBuilder.builder()
                    .terminal(
                            TerminalBuilder.builder().dumb(true).build())
                    .build();
        } catch (Exception e) {
            Logger.error("Failed to initialize input reader: " + e.getMessage());
            return;
        }
        try {
            while (running) {
                try {
                    StringBuilder sqlBuilder = new StringBuilder();
                    boolean isNewLine = true;
                    while (true) {
                        String prompt = isNewLine ? "CS307-DB> " : "...> ";
                        String line = scanner.readLine(prompt).trim();
                        if (line.isEmpty() && sqlBuilder.length() == 0) {
                            continue;
                        }
                        sqlBuilder.append(line).append(" ");
                        if (line.endsWith(";")) {
                            break;
                        }
                        isNewLine = false;
                    }
                    sql = sqlBuilder.toString().trim();
                    Logger.info("Executing: " + sql);
                    if (sql.equalsIgnoreCase("exit;")) {
                        running = false;
                        Logger.info("Exiting CS307-DB. Goodbye!");
                        continue;
                    } else if (sql.equalsIgnoreCase("help;")) {
                        printHelp();
                        continue;
                    }
                } catch (Exception e) {
                    Logger.error("Input error: " + e.getMessage());
                    Logger.error("Please try again.");
                    continue;
                }
                try {
                    LogicalOperator operator = LogicalPlanner.resolveAndPlan(dbManager, sql);
                    if (operator == null) {
                        continue;
                    }
                    PhysicalOperator physicalOperator = PhysicalPlanner.generateOperator(dbManager, operator);
                    if (physicalOperator == null) {
                        Logger.info("No physical operator generated.");
                        Logger.info(operator);
                        continue;
                    }
                    // Initialize and prepare the operator
                    physicalOperator.Begin();
                    // Print header line and columns
                    Logger.info(getStartEndLine(physicalOperator.outputSchema(), true));
                    Logger.info(getHeaderString(physicalOperator.outputSchema()));
                    Logger.info(getSperator(physicalOperator.outputSchema()));
                    while (physicalOperator.hasNext()) {
                        physicalOperator.Next();
                        Tuple tuple = physicalOperator.Current();
                        Logger.info(getRecordString(tuple, physicalOperator.outputSchema())); // MODIFIED CALL
                        Logger.info(getSperator(physicalOperator.outputSchema())); // MODIFIED CALL
                    }
                    physicalOperator.Close();
                    dbManager.getBufferPool().FlushAllPages("");
                } catch (DBException e) {
                    Logger.error("Execution error: " + e.getMessage());
                    Logger.error("Please check your SQL syntax or database state.");
                    Logger.error(Arrays.toString(e.getStackTrace()));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            // persist the disk manager
            dbManager.getBufferPool().FlushAllPages("");
            Logger.error("Some error occurred. Exiting after persistdata...");
        } finally {
            if (dbManager != null) {
                try {
                    dbManager.closeDBManager();
                } catch (DBException e2) {
                    Logger.error("Error closing DBManager: " + e2.getMessage());
                }
            }
        }
    }

    private static String getHeaderString(ArrayList<ColumnMeta> columnMetas) {
        StringBuilder header = new StringBuilder("|");
        if (columnMetas.isEmpty()) {
            // For empty schema, use a fixed width consistent with other methods
            header.append(StringUtils.center("(empty)", 15, ' ')).append("|");
            return header.toString();
        }
        for (var entry : columnMetas) {
            String tabcol = entry.tableName == null ? entry.name : entry.tableName + "." + entry.name;
            int columnWidth = Math.max(tabcol.length(), 15); // Dynamic width based on header text
            String centeredText = StringUtils.center(tabcol, columnWidth, ' '); // Use dynamic columnWidth
            header.append(centeredText).append("|");
        }
        return header.toString();
    }

    private static String getRecordString(Tuple tuple, ArrayList<ColumnMeta> columnMetas) throws DBException {
        Value[] values = tuple == null ? null : tuple.getValues();
        StringBuilder sb = new StringBuilder("|");

        if (columnMetas.isEmpty()) {
            if (values != null && values.length == 1 && values[0] != null && !values[0].isNull()) {
                sb.append(StringUtils.center(values[0].toString(), 15, ' ')).append("|");
            } else {
                sb.append(StringUtils.center("0", 15, ' ')).append("|");
            }
            return sb.toString();
        }

        for (int i = 0; i < columnMetas.size(); i++) {
            ColumnMeta cm = columnMetas.get(i);
            String tabcol = cm.tableName == null ? cm.name : cm.tableName + "." + cm.name;
            int columnWidth = Math.max(tabcol.length(), 15);
            String cellContent;
            if (tuple == null || values == null || i >= values.length || values[i] == null || values[i].isNull()
                    || values[i].value == null) {
                cellContent = "(null)";
            } else {
                cellContent = values[i].toString();
            }
            sb.append(StringUtils.center(cellContent, columnWidth, ' ')).append("|");
        }
        return sb.toString();
    }

    private static String getSperator(ArrayList<ColumnMeta> columnMetas) {
        StringBuilder line = new StringBuilder("+");
        if (columnMetas.isEmpty()) {
            line.append(StringUtils.repeat("─", 15)).append("+");
            return line.toString();
        }
        for (var entry : columnMetas) {
            String tabcol = entry.tableName == null ? entry.name : entry.tableName + "." + entry.name;
            int columnWidth = Math.max(tabcol.length(), 15);
            line.append(StringUtils.repeat("─", columnWidth));
            line.append("+");
        }
        return line.toString();
    }

    private static String getStartEndLine(ArrayList<ColumnMeta> columnMetas, boolean header) {
        StringBuilder end_line;
        if (header) {
            end_line = new StringBuilder("┌");
        } else {
            end_line = new StringBuilder("└");
        }
        if (columnMetas.isEmpty()) {
            end_line.append(StringUtils.repeat("─", 15));
            if (header) {
                end_line.append("┐");
            } else {
                end_line.append("┘");
            }
            return end_line.toString();
        }
        for (int i = 0; i < columnMetas.size(); i++) {
            ColumnMeta entry = columnMetas.get(i);
            String tabcol = entry.tableName == null ? entry.name : entry.tableName + "." + entry.name;
            int columnWidth = Math.max(tabcol.length(), 15);
            end_line.append(StringUtils.repeat("─", columnWidth));
            if (i < columnMetas.size() - 1) {
                if (header) {
                    end_line.append("┬");
                } else {
                    end_line.append("┴");
                }
            }
        }
        if (header) {
            end_line.append("┐");
        } else {
            end_line.append("┘");
        }
        return end_line.toString();
    }
}
