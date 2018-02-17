package edu.buffalo.www.cse4562;


import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;

public class From_Parse implements FromItemVisitor {
    @Override
    public void visit(SubJoin subJoin) {
    		System.out.println("JON");
    }

    @Override
    public void visit(SubSelect subSelect) {
    		System.out.println("Here?");
    		SelectVisitor s_visit = new Select_Parse();
            subSelect.getSelectBody().accept(s_visit);
    }

    @Override
    public void visit(Table table) {
    		
        System.out.println("Table Name "+ table.getName() + " ");
    }
}
