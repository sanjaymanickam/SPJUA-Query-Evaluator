package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Select;

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
            for(int i=0;i<columnDefinitions.size();i++)
            {
                tableDetails.put(columnDefinitions.get(i).getColumnName().toString(),columnDefinitions.get(i).getColDataType().toString());
            }
            Data_Storage.tables.put(createTable.getTable().getName(),tableDetails);
        }
    }
}
