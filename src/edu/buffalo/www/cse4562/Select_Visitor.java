package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.*;

import javax.xml.crypto.Data;
import java.io.File;
import java.util.Iterator;
import java.util.List;

public class Select_Visitor implements SelectVisitor {
    @Override
    public void visit(PlainSelect plainSelect) {
        String table_name;
        From_Visitor from_visitor = new From_Visitor();
        plainSelect.getFromItem().accept(from_visitor);
        table_name = from_visitor.retTableName();
        StringBuilder str = new StringBuilder(Data_Storage.dataDir.toString()).append("/").append(table_name).append(".csv");
        edu.buffalo.www.cse4562.Iterator oper = new File_Iterator(new File(str.toString()));
        SelectItemVisitor select_Item = new SelectItem_Visitor();
        List<SelectItem> columns = plainSelect.getSelectItems();
        System.out.println("The Selected Columns are : " + columns);
        for (SelectItem col : columns) {
            col.accept(select_Item);
        }
        Expr_Visitor expr_visitor = new Expr_Visitor();
        if (plainSelect.getWhere() != null) {
            plainSelect.getWhere().accept(expr_visitor);
            System.out.println("The Expression is : " + expr_visitor.getExpr());
            oper = new Eval_Iterator(oper, expr_visitor.getExpr(), table_name);
        }
        String cols[] = oper.readOneTuple();
        while (cols != null) {
            for (int i = 0; i < cols.length; i++) {
                if (Data_Storage.selectedColumns.contains(Data_Storage.tableColumns.get(Data_Storage.tablename)[i])) {
                    System.out.print(cols[i] + " | ");
                }
            }
            System.out.println();
            cols = oper.readOneTuple();
        }
    }

    @Override
    public void visit(Union union) {

    }
}
