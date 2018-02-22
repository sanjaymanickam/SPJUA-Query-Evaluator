package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class From_Visitor implements FromItemVisitor {
    static String tableName;

    static ArrayList<String> schema;
    @Override
    public void visit(SubJoin subJoin) {
        System.out.println("IN SUB-JOIN");
    }

    @Override
    public void visit(SubSelect subSelect) {
        Select_Visitor select_visitor = new Select_Visitor();
        subSelect.getSelectBody().accept(select_visitor);
        schema = Data_Storage.tableColumns.get(tableName);
    }

    @Override
    public void visit(Table table) {
//       System.out.println("The Table being used in this query : " + table.getName());
        tableName = table.getName();
//       System.out.println("SCHEMA "+table.getSchemaName());
        schema = Data_Storage.tableColumns.get(table.getName());
//        Data_Storage.tablename = table.getName();
    }

    public String retTableName() {
        return tableName;
    }
    public List<String> retSchema() {return schema;}
}
