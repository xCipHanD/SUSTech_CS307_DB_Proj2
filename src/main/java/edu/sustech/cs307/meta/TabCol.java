package edu.sustech.cs307.meta;

public class TabCol {
    private String tableName;
    private String columnName;

    public TabCol(String tableName, String columnName) {
        this.tableName = tableName;
        this.columnName = columnName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getColumnName() {
        return columnName;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null || getClass() != obj.getClass())
            return false;
        TabCol tabCol = (TabCol) obj;
        return (tableName == null ? tabCol.tableName == null : tableName.equals(tabCol.tableName)) &&
                (columnName == null ? tabCol.columnName == null : columnName.equals(tabCol.columnName));
    }

    @Override
    public int hashCode() {
        int result = tableName == null ? 0 : tableName.hashCode();
        result = 31 * result + (columnName == null ? 0 : columnName.hashCode());
        return result;
    }
}
