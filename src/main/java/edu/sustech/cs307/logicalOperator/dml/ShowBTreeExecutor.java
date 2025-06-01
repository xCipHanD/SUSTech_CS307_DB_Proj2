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
        // 检查表是否存在
        if (!dbManager.isTableExists(tableName)) {
            throw new DBException(ExceptionTypes.TableDoesNotExist(tableName));
        }

        // 获取指定表和列的索引，但不创建新的索引
        Index index = dbManager.getIndexManager().getIndex(tableName, columnName);

        if (index == null) {
            // 如果索引不存在，直接报错，不尝试创建
            throw new DBException(ExceptionTypes.UnsupportedCommand(
                    String.format(
                            "No index exists for table '%s' column '%s'. Please create an index first using CREATE INDEX.",
                            tableName, columnName)));
        }

        // 检查索引是否为B+树类型
        if (!(index instanceof BPlusTreeIndex)) {
            throw new DBException(ExceptionTypes.UnsupportedCommand(
                    String.format("Index for table '%s' column '%s' is not a B+ Tree index. Index type: %s",
                            tableName, columnName, index.getClass().getSimpleName())));
        }

        BPlusTreeIndex bTreeIndex = (BPlusTreeIndex) index;

        // 检查B+树是否有数据
        if (bTreeIndex.getRoot() == null) {
            throw new DBException(ExceptionTypes.UnsupportedCommand(
                    String.format("B+ Tree index for table '%s' column '%s' is empty. No data to display.",
                            tableName, columnName)));
        }

        // 生成B+树显示结果
        this.displayResult = generateBTreeDisplay(bTreeIndex);
    }

    /**
     * 生成B+树的显示字符串
     */
    private String generateBTreeDisplay(BPlusTreeIndex bTreeIndex) {
        StringBuilder result = new StringBuilder();

        // 添加标题和边框
        result.append("╔══════════════════════════════════════════════════════════════════════════════╗\n");
        result.append("║                          B+ Tree Structure Display                           ║\n");
        result.append("╠══════════════════════════════════════════════════════════════════════════════╣\n");
        result.append(String.format("║ Table: %-20s Column: %-20s                     ║\n",
                tableName, columnName));
        result.append("╚══════════════════════════════════════════════════════════════════════════════╝\n");
        result.append("\n");

        // 显示B+树结构
        if (bTreeIndex.getRoot() == null) {
            result.append("┌─────────────────────┐\n");
            result.append("│   B+ Tree is Empty  │\n");
            result.append("└─────────────────────┘\n");
        } else {
            result.append("B+ Tree Structure:\n");
            result.append("==================\n");

            // 使用BPlusTreeIndex的getTreeString()方法
            String treeStructure = bTreeIndex.getTreeString();
            result.append(treeStructure);

            result.append("\n");

            // 显示树的验证状态
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