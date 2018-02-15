package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.statement.select.*;

import java.util.Iterator;
import java.util.List;

public class Select_Parse implements SelectVisitor {
    @Override
    public void visit(PlainSelect plainSelect) {
        List l = plainSelect.getSelectItems();
        Iterator t = l.iterator();
        while (t.hasNext()) {
            System.out.println(t.next() + " ");
        }
        FromItemVisitor from_Item = new From_Parse();
        plainSelect.getFromItem().accept(from_Item);
//        ExpressionVisitor expr_Item = new Expr_Parse();
//        plainSelect.getWhere().accept(expr_Item);
    }

    @Override
    public void visit(Union union) {

    }
}
