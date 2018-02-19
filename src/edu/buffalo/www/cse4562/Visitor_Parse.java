package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.replace.Replace;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.delete.*;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.*;
import net.sf.jsqlparser.statement.insert.*;
import net.sf.jsqlparser.statement.drop.*;

import java.util.HashMap;
import java.util.List;

public class Visitor_Parse implements StatementVisitor {
    @Override
    public void visit(Select select) {
        SelectVisitor s_visit = new Select_Visitor();
        select.getSelectBody().accept(s_visit);
    }

    @Override
    public void visit(Delete delete) {
    }

    @Override
    public void visit(Update update) {
    }

    @Override
    public void visit(Insert insert) {
    }

    @Override
    public void visit(Replace replace) {
    }

    @Override
    public void visit(Drop drop) {
    }

    @Override
    public void visit(Truncate trunc) {
    }

    @Override
    public void visit(CreateTable createTable) {
        System.out.println("Table name from visitor is :" + createTable.getTable().getName());
        List<ColumnDefinition> columns = createTable.getColumnDefinitions();
        HashMap<String, String> tableDetails = new HashMap<>();
        String[] columnArray = new String[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            ColumnDefinition cols = columns.get(i);
            tableDetails.put(cols.getColumnName(), cols.getColDataType().getDataType());
            columnArray[i] = cols.getColumnName();
        }
        Data_Storage.tables.put(createTable.getTable().getName(), tableDetails);
        Data_Storage.tableColumns.put(createTable.getTable().getName(), columnArray);
        Data_Storage.tablename = createTable.getTable().getName();
    }
}

