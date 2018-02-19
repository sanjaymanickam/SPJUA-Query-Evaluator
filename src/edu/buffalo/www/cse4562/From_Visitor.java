package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;

public class From_Visitor implements FromItemVisitor {
    String tableName;
    @Override
    public void visit(SubJoin subJoin) {
        System.out.println("IN SUB-JOIN");
    }

    @Override
    public void visit(SubSelect subSelect) {

    }

    @Override
    public void visit(Table table) {
        System.out.println("The Table being used in this query : " + table.getName());
        tableName = table.getName();
    }

    public String retTableName() {
        return tableName;
    }
}
