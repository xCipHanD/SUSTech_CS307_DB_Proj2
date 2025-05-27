package edu.sustech.cs307.physicalOperator;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.meta.ColumnMeta;
import edu.sustech.cs307.meta.TableMeta;
import edu.sustech.cs307.system.DBManager;
import edu.sustech.cs307.tuple.Tuple;
import edu.sustech.cs307.index.Index;
import edu.sustech.cs307.value.Value;
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

    @Override
    public void Begin() throws DBException {
        if (isOpen) {
            return;
        }

        // 如果有索引可用
        if (index != null) {
            try {
                if (searchKey != null) {
                    // 有搜索键，使用索引进行精确查找
                    IndexScanOperator indexScan = new IndexScanOperator(tableName, columnName, searchKey, dbManager,
                            index);
                    indexScan.Begin();

                    // 检查索引扫描是否有结果
                    if (indexScan.hasNext()) {
                        currentOperator = indexScan;
                        usingIndexScan = true;
                        Logger.info("Using IndexScanOperator for table {} on column {} with search key {}",
                                tableName, columnName, searchKey);
                    } else {
                        // 索引扫描没有结果，关闭并回退到顺序扫描
                        indexScan.Close();
                        currentOperator = createSeqScanWithFilter();
                        currentOperator.Begin();
                        usingIndexScan = false;
                        Logger.info("Index scan returned no results, falling back to SeqScan for table {}", tableName);
                    }
                } else {
                    // 没有搜索键，进行全表扫描，优先使用索引
                    if (index instanceof edu.sustech.cs307.index.InMemoryOrderedIndex) {
                        // 对于InMemoryOrderedIndex，使用专门的全表扫描操作符
                        currentOperator = new InMemoryIndexScanOperator(
                                (edu.sustech.cs307.index.InMemoryOrderedIndex) index, dbManager);
                        currentOperator.Begin();
                        usingIndexScan = true;
                        Logger.info("Using InMemoryIndexScanOperator for full table scan on table {}", tableName);
                    } else {
                        // 对于其他类型的索引，回退到顺序扫描
                        currentOperator = new SeqScanOperator(tableName, dbManager);
                        currentOperator.Begin();
                        usingIndexScan = false;
                        Logger.info(
                                "Using SeqScanOperator for full table scan on table {} (B+Tree index not suitable for full scan)",
                                tableName);
                    }
                }
            } catch (DBException e) {
                Logger.warn("Index operation failed for table {} on column {}: {}, falling back to SeqScan",
                        tableName, columnName, e.getMessage());
                // 索引操作失败，回退到顺序扫描
                currentOperator = createSeqScanWithFilter();
                currentOperator.Begin();
                usingIndexScan = false;
            }
        } else {
            // 没有索引，直接使用顺序扫描
            currentOperator = createSeqScanWithFilter();
            currentOperator.Begin();
            usingIndexScan = false;
            Logger.info("No index available, using SeqScan for table {}", tableName);
        }

        isOpen = true;
    }

    private PhysicalOperator createSeqScanWithFilter() throws DBException {
        SeqScanOperator seqScan = new SeqScanOperator(tableName, dbManager);

        if (searchKey != null && columnName != null) {
            // 创建等值过滤条件
            net.sf.jsqlparser.expression.operators.relational.EqualsTo equalsTo = new net.sf.jsqlparser.expression.operators.relational.EqualsTo();
            net.sf.jsqlparser.schema.Column column = new net.sf.jsqlparser.schema.Column(columnName);

            // 根据搜索键类型创建对应的表达式
            net.sf.jsqlparser.expression.Expression valueExpr;
            if (searchKey.type == edu.sustech.cs307.value.ValueType.CHAR) {
                valueExpr = new net.sf.jsqlparser.expression.StringValue((String) searchKey.value);
            } else if (searchKey.type == edu.sustech.cs307.value.ValueType.INTEGER) {
                valueExpr = new net.sf.jsqlparser.expression.LongValue((Long) searchKey.value);
            } else if (searchKey.type == edu.sustech.cs307.value.ValueType.FLOAT) {
                valueExpr = new net.sf.jsqlparser.expression.DoubleValue((Float) searchKey.value);
            } else if (searchKey.type == edu.sustech.cs307.value.ValueType.DOUBLE) {
                valueExpr = new net.sf.jsqlparser.expression.DoubleValue((Double) searchKey.value);
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