package edu.buffalo.www.cse4562;

public class Schema {
    String columnName;
    String tableName;

    public String getAliasName() {
        return aliasName;
    }

    public void setAliasName(String aliasName) {
        this.aliasName = aliasName;
    }

    String DataType;
    Integer position;
    String aliasName = null;

    public Integer getPosition() {
        return position;
    }

    public void setPosition(Integer position) {
        this.position = position;
    }

    public Schema(String tableName, String columnName, String dataType, Integer position){
        this.columnName = columnName;
        this.tableName = tableName;
        this.DataType = dataType;
        this.position = position;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setDataType(String dataType) {
        this.DataType = dataType;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getDataType() {
        return DataType;
    }

}
