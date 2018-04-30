package edu.buffalo.www.cse4562;

public class ColumnType {
    String dataType;
    Integer position;

    public ColumnType(String dataType, Integer positon){
        this.dataType = dataType;
        this.position = positon;
    }
    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public Integer getPosition() {
        return position;
    }

    public void setPosition(Integer position) {
        this.position = position;
    }


}
