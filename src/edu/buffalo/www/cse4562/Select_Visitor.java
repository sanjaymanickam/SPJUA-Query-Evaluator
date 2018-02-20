package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.*;

import javax.xml.crypto.Data;
import java.io.File;
import java.util.Iterator;
import java.util.List;

public class Select_Visitor implements SelectVisitor {
    String table_name;
    Expression expr = null;
    @Override
    public void visit(PlainSelect plainSelect) {
        From_Visitor from_visitor = new From_Visitor();
        plainSelect.getFromItem().accept(from_visitor);
        table_name = from_visitor.retTableName();
        Data_Storage.tablename = table_name;
        Data_Storage.star_flag = 0;
        SelectItemVisitor select_Item = new SelectItem_Visitor();
        List<SelectItem> columns = plainSelect.getSelectItems();
//        System.out.println("The Selected Columns are : " + columns);
        Data_Storage.selectedColumns.clear();
        if (columns.size() == 1 && columns.get(0).toString().equals("*")) {
//           System.out.println("Flag Set");
            Data_Storage.star_flag = 1;
        } else {
            for (SelectItem col : columns) {
                col.accept(select_Item);
            }
        }
        Expr_Visitor expr_visitor = new Expr_Visitor();
        if (plainSelect.getWhere() != null) {
            plainSelect.getWhere().accept(expr_visitor);
//            System.out.println("The Expression is : " + expr_visitor.getExpr());
            expr = expr_visitor.getExpr();
        }

    }

    @Override
    public void visit(Union union) {

    }

    public String retTableName() {
        return table_name;
    }

    public Expression retExpr() {
        return expr;
    }
}
