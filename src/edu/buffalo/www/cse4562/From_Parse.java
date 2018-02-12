package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;

public class From_Parse implements FromItemVisitor {
    @Override
    public void visit(SubJoin subJoin) {

    }

    @Override
    public void visit(SubSelect subSelect) {

    }

    @Override
    public void visit(Table table) {
        System.out.println(table.getName() + " ");
    }
}
