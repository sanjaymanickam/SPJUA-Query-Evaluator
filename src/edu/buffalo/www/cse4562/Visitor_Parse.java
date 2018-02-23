package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Column;
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

import javax.swing.text.html.HTMLDocument;
import javax.xml.crypto.Data;
import java.io.File;
import java.util.*;

public class Visitor_Parse implements StatementVisitor {
    List<String> schema;
    @Override
    public void visit(Select select) {
            for (String s : Data_Storage.selectedColumns)
                Data_Storage.tables.get(Data_Storage.tablename).remove(s);
        Data_Storage.selectedColumns.removeAll(Data_Storage.selectedColumns);
        Select_Visitor s_visit = new Select_Visitor();
        select.getSelectBody().accept(s_visit);
        String table_name = s_visit.retTableName();
        schema = s_visit.retSchema();
        Data_Storage.tablename = table_name;
//        StringBuilder str = new StringBuilder(Data_Storage.dataDir.toString()).append("/").append(table_name).append(".dat");
//        if (table_name != null) {
//            Data_Storage.oper = new File_IteratorInteface(new File(str.toString()));
//        }
//        if (s_visit.retExpr() != null) {
//            Data_Storage.oper = new Eval_IteratorInteface(Data_Storage.oper,schema,s_visit.retExpr(), table_name);
//        }
//        if(s_visit.retSelectExpr()!=null) {
//            Data_Storage.oper = new Eval_IteratorInteface(Data_Storage.oper,schema,s_visit.retSelectExpr(), table_name);
//        }
        ArrayList<String> cols = Data_Storage.oper.readOneTuple();
        Iterator iter = schema.iterator();
        ArrayList<String> schema_list = new ArrayList<>(Data_Storage.tableColumns.get(table_name));
        for(String s: Data_Storage.selectedColumns)
        {
            schema_list.add(s);
        }
//        StringBuilder to_output = new StringBuilder();
//        while(cols!=null) {
//            to_output = new StringBuilder();
//            while (iter.hasNext()) {
//                String to_check = iter.next().toString();
//                if (schema_list.contains(to_check)) {
//                    to_output.append(cols.get(schema_list.indexOf(to_check)));
//                    if(iter.hasNext())
//                        to_output.append("|");
//                }
//            }
//            System.out.println(to_output.toString());
//            iter = schema.iterator();
//            cols = Data_Storage.oper.readOneTuple();
//        }
        while(cols!=null) {
            while (iter.hasNext()) {
                String to_check = iter.next().toString();
                String to_print = cols.get(schema_list.indexOf(to_check)).toString();
                if (schema_list.contains(to_check)) {
                    if(Data_Storage.tables.get(table_name).get(to_check) == "STRING" || Data_Storage.tables.get(table_name).get(to_check) == "VARCHAR" ||Data_Storage.tables.get(table_name).get(to_check) == "CHAR")
                            System.out.print(new StringValue(to_print));
                    else
                            System.out.print(to_print);
                    if(iter.hasNext())
                        System.out.print("|");
                }
            }
            System.out.println();
            iter = schema.iterator();
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
        ArrayList<String> columnArray = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            ColumnDefinition cols = columns.get(i);
            tableDetails.put(cols.getColumnName(), cols.getColDataType().getDataType());
            columnArray.add(cols.getColumnName());
        }
        Data_Storage.tables.put(createTable.getTable().getName(), tableDetails);
        Data_Storage.tableColumns.put(createTable.getTable().getName(), columnArray);
        Data_Storage.tablename = createTable.getTable().getName();
    }
}

