package edu.sustech.cs307.physicalOperator;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.meta.ColumnMeta;
import edu.sustech.cs307.meta.TableMeta;
import edu.sustech.cs307.system.DBManager;
import edu.sustech.cs307.tuple.Tuple;
import edu.sustech.cs307.index.Index;
import edu.sustech.cs307.value.Value;
import edu.sustech.cs307.value.ValueType;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;

import org.pmw.tinylog.Logger;

import java.util.ArrayList;

/**
 * 混合扫描器：优先使用索引扫描，如果索引不可用或为空则回退到顺序扫描
 */
public class HybridScanOperator implements PhysicalOperator {
    private final String tableName;
    private final String columnName;
    private final Value searchKey;
    private final DBManager dbManager;
    private final Index index;

    private PhysicalOperator currentOperator; // 当前实际使用的扫描器
    private boolean usingIndexScan;
    private boolean isOpen = false;

    public HybridScanOperator(String tableName, String columnName, Value searchKey,
            DBManager dbManager, Index index) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.searchKey = searchKey;
        this.dbManager = dbManager;
        this.index = index;
        this.usingIndexScan = false;
    }

    /**
     * 智能选择扫描策略，考虑表大小和查询类型
     */
    private boolean shouldUseIndexScan(DBManager dbManager, String tableName, Value searchKey) throws DBException {
        try {
            TableMeta tableMeta = dbManager.getMetaManager().getTable(tableName);
            long fileSize = dbManager.getDiskManager().GetFileSize(tableName + "/data");
            if (fileSize < 10 * 1024) {
                Logger.info("Table {} is small ({}KB), using SeqScan directly", tableName, fileSize / 1024);
                return false;
            }
            if (searchKey == null) {
                return false;
            }
            return true;
        } catch (Exception e) {
            Logger.warn("Failed to determine scan strategy, falling back to SeqScan: {}", e.getMessage());
            return false;
        }
    }

    @Override
    public void Begin() throws DBException {
        if (isOpen) {
            return;
        }

        boolean useIndex = shouldUseIndexScan(dbManager, tableName, searchKey);

        if (index != null && useIndex) {
            try {
                if (searchKey != null) {
                    IndexScanOperator indexScan = new IndexScanOperator(tableName, columnName, searchKey, dbManager,
                            index);
                    indexScan.Begin();

                    currentOperator = indexScan;
                    usingIndexScan = true;
                    Logger.info("Using IndexScanOperator for table {} on column {} with search key {}",
                            tableName, columnName, searchKey);
                } else {
                    currentOperator = new SeqScanOperator(tableName, dbManager);
                    currentOperator.Begin();
                    usingIndexScan = false;
                    Logger.info(
                            "Using SeqScanOperator for full table scan on table {} (B+Tree index not suitable for full scan)",
                            tableName);
                }
            } catch (DBException e) {
                Logger.warn("Index operation failed for table {} on column {}: {}, falling back to SeqScan",
                        tableName, columnName, e.getMessage());
                currentOperator = createSeqScanWithFilter();
                currentOperator.Begin();
                usingIndexScan = false;
            }
        } else {
            currentOperator = createSeqScanWithFilter();
            currentOperator.Begin();
            usingIndexScan = false;
            Logger.info("Using SeqScan for table {} (small table or no suitable index)", tableName);
        }

        isOpen = true;
    }

    private PhysicalOperator createSeqScanWithFilter() throws DBException {
        SeqScanOperator seqScan = new SeqScanOperator(tableName, dbManager);

        if (searchKey != null && columnName != null) {
            EqualsTo equalsTo = new EqualsTo();
            Column column = new Column(columnName);

            Expression valueExpr;
            if (searchKey.type == ValueType.CHAR) {
                valueExpr = new StringValue((String) searchKey.value);
            } else if (searchKey.type == ValueType.INTEGER) {
                valueExpr = new LongValue((Long) searchKey.value);
            } else if (searchKey.type == ValueType.FLOAT) {
                valueExpr = new DoubleValue((Float) searchKey.value);
            } else if (searchKey.type == ValueType.DOUBLE) {
                valueExpr = new DoubleValue((Double) searchKey.value);
            } else {
                // 对于不支持的类型，返回未过滤的顺序扫描
                return seqScan;
            }

            equalsTo.setLeftExpression(column);
            equalsTo.setRightExpression(valueExpr);

            return new FilterOperator(seqScan, equalsTo);
        }

        return seqScan;
    }

    @Override
    public boolean hasNext() throws DBException {
        if (!isOpen || currentOperator == null) {
            return false;
        }
        return currentOperator.hasNext();
    }

    @Override
    public void Next() throws DBException {
        if (!isOpen || currentOperator == null) {
            return;
        }
        currentOperator.Next();
    }

    @Override
    public Tuple Current() {
        if (!isOpen || currentOperator == null) {
            return null;
        }
        return currentOperator.Current();
    }

    @Override
    public void Close() throws DBException {
        if (!isOpen) {
            return;
        }

        if (currentOperator != null) {
            currentOperator.Close();
            currentOperator = null;
        }

        isOpen = false;
    }

    @Override
    public ArrayList<ColumnMeta> outputSchema() {
        if (currentOperator != null) {
            return currentOperator.outputSchema();
        }

        // 回退方案：从DBManager获取表结构
        try {
            TableMeta tableMeta = dbManager.getMetaManager().getTable(tableName);
            return tableMeta != null ? tableMeta.columns_list : new ArrayList<>();
        } catch (DBException e) {
            Logger.error("Failed to get table metadata for output schema: " + e.getMessage());
            return new ArrayList<>();
        }
    }

    public boolean isUsingIndexScan() {
        return usingIndexScan;
    }
}