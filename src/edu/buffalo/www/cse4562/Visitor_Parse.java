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

import javax.xml.crypto.Data;
import java.io.File;
import java.util.HashMap;
import java.util.List;

public class Visitor_Parse implements StatementVisitor {
    @Override
    public void visit(Select select) {
        Select_Visitor s_visit = new Select_Visitor();
        select.getSelectBody().accept(s_visit);
        String table_name = s_visit.retTableName();
        Data_Storage.tablename = table_name;
        StringBuilder str = new StringBuilder(Data_Storage.dataDir.toString()).append("/").append(table_name).append(".dat");
        if (table_name != null) {
            Data_Storage.oper = new File_Iterator(new File(str.toString()));
        }
        if (s_visit.retExpr() != null) {
            Data_Storage.oper = new Eval_Iterator(Data_Storage.oper, s_visit.retExpr(), table_name);
        }
        String cols[] = Data_Storage.oper.readOneTuple();
        while (cols != null) {
            for (int i = 0; i < cols.length; i++) {
                if (Data_Storage.star_flag == 1) {
                    System.out.print(cols[i]);
                    if (cols.length != i + 1)
                        System.out.print("|");
                }
                else {
                    if(Data_Storage.selectedColumns.contains(Data_Storage.tableColumns.get(Data_Storage.tablename)[i])) {
                        System.out.print(cols[i]);
                        if (Data_Storage.selectedColumns.size() != i + 1)
                            System.out.print("|");
                    }
                }
            }
            System.out.println();
            cols = Data_Storage.oper.readOneTuple();
        }
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
//        System.out.println("Table name from visitor is :" + createTable.getTable().getName());
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
//       Data_Storage.tablename = createTable.getTable().getName();
    }
}

