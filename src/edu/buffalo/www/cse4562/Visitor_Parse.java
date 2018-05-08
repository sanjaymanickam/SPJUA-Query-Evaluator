package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;
import net.sf.jsqlparser.statement.select.Select;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

public class Visitor_Parse {
    public static void ret_type(Statement statement)
    {
        if(statement instanceof Select)
        {
            Select select = (Select) statement;
            Select_Visitor.ret_type(select.getSelectBody());
        }
        else if(statement instanceof CreateTable)
        {
            CreateTable createTable = (CreateTable) statement;
            List<ColumnDefinition> columnDefinitions = createTable.getColumnDefinitions();
            LinkedHashMap<String,String> tableDetails = new LinkedHashMap<>();
            LinkedHashMap<String,ColumnDefinition> tableColDetails = new LinkedHashMap<>();
            List<Index> indices = createTable.getIndexes();
            ArrayList<ArrayList<String>> primaryIndex = new ArrayList<>();
            ArrayList<ArrayList<String>> foreignIndex = new ArrayList<>();
            for(Integer i=0;i<columnDefinitions.size();i++)
            {
                ColumnDefinition colDef = (ColumnDefinition) columnDefinitions.get(i);
                tableDetails.put(colDef.getColumnName(),colDef.getColDataType().getDataType());
                tableColDetails.put(columnDefinitions.get(i).getColumnName(),columnDefinitions.get(i));
                List<String> indexList = colDef.getColumnSpecStrings();
                if(indexList!=null){
                    if(indexList.get(0).equals("PRIMARY")){
                        ArrayList<String> primary = new ArrayList<>();
                        primary.add(colDef.getColumnName());
                        primary.add(i.toString());
                        primaryIndex.add(primary);
                    }
                    if(indexList.get(0).equals("REFERENCES")){
                        ArrayList<String> secondary = new ArrayList<>();
                        secondary.add(colDef.getColumnName());
                        secondary.add(i.toString());
                        foreignIndex.add(secondary);
                    }
                    Data_Storage.indexColumns.add(colDef.getColumnName());
                }
                if(indices!=null && indices.get(0).getColumnsNames().contains(columnDefinitions.get(i).getColumnName())){
                    ArrayList<String> primary = new ArrayList<>();
                    primary.add(colDef.getColumnName());
                    primary.add(i.toString());
                    primaryIndex.add(primary);
                    Data_Storage.indexColumns.add(colDef.getColumnName());
                }

            }
            Data_Storage.primaryKey.put(createTable.getTable().getName(),primaryIndex);
            Data_Storage.foreignKey.put(createTable.getTable().getName(),foreignIndex);
            Data_Storage.tables.put(createTable.getTable().getName(),tableDetails);
        }
    }
}
