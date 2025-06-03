package edu.sustech.cs307.logicalOperator.dml;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.exception.ExceptionTypes;
import edu.sustech.cs307.index.BPlusTreeIndex;
import edu.sustech.cs307.index.Index;
import edu.sustech.cs307.system.DBManager;

/**
 * 执行器用于显示指定表和列的B+树结构
 */
public class ShowBTreeExecutor implements DMLExecutor {
    private final String tableName;
    private final String columnName;
    private final DBManager dbManager;
    private String displayResult;

    public ShowBTreeExecutor(String tableName, String columnName, DBManager dbManager) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.dbManager = dbManager;
        this.displayResult = "";
    }

    @Override
    public void execute() throws DBException {
        if (!dbManager.isTableExists(tableName)) {
            throw new DBException(ExceptionTypes.TableDoesNotExist(tableName));
        }

        Index index = dbManager.getIndexManager().getIndex(tableName, columnName);

        if (index == null) {
            throw new DBException(ExceptionTypes.UnsupportedCommand(
                    String.format(
                            "No index exists for table '%s' column '%s'. Please create an index first using CREATE INDEX.",
                            tableName, columnName)));
        }

        if (!(index instanceof BPlusTreeIndex)) {
            throw new DBException(ExceptionTypes.UnsupportedCommand(
                    String.format("Index for table '%s' column '%s' is not a B+ Tree index. Index type: %s",
                            tableName, columnName, index.getClass().getSimpleName())));
        }

        BPlusTreeIndex bTreeIndex = (BPlusTreeIndex) index;

        if (bTreeIndex.getRoot() == null) {
            throw new DBException(ExceptionTypes.UnsupportedCommand(
                    String.format("B+ Tree index for table '%s' column '%s' is empty. No data to display.",
                            tableName, columnName)));
        }

        this.displayResult = generateBTreeDisplay(bTreeIndex);
    }

    /**
     * 生成B+树的显示字符串
     */
    private String generateBTreeDisplay(BPlusTreeIndex bTreeIndex) {
        StringBuilder result = new StringBuilder();

        result.append("╔══════════════════════════════════════════════════════════════════════════════╗\n");
        result.append("║                          B+ Tree Structure Display                           ║\n");
        result.append("╠══════════════════════════════════════════════════════════════════════════════╣\n");
        result.append(String.format("║ Table: %-20s Column: %-20s                     ║\n",
                tableName, columnName));
        result.append("╚══════════════════════════════════════════════════════════════════════════════╝\n");
        result.append("\n");

        if (bTreeIndex.getRoot() == null) {
            result.append("┌─────────────────────┐\n");
            result.append("│   B+ Tree is Empty  │\n");
            result.append("└─────────────────────┘\n");
        } else {
            result.append("B+ Tree Structure:\n");
            result.append("==================\n");

            String treeStructure = bTreeIndex.getTreeString();
            result.append(treeStructure);

            result.append("\n");

            boolean isValid = bTreeIndex.validateTree();
            if (isValid) {
                result.append("✓ B+ Tree structure is VALID\n");
            } else {
                result.append("✗ B+ Tree structure has VALIDATION ERRORS\n");
            }
        }

        result.append("\n");
        result.append("Use 'SHOW BTREE <table_name> <column_name>;' to display other B+ trees.\n");

        return result.toString();
    }

    /**
     * 获取格式化的显示结果
     */
    public String getDisplayResult() {
        return displayResult;
    }
}