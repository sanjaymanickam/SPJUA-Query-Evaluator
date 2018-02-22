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
    static String table_name;
    Expression expr = null;
    Expression sel_expr=null;
    List<String> schema;
    @Override
    public void visit(PlainSelect plainSelect) {
        From_Visitor from_visitor = new From_Visitor();
        plainSelect.getFromItem().accept(from_visitor);
        table_name = from_visitor.retTableName();
        schema = from_visitor.retSchema();
        StringBuilder str = new StringBuilder(Data_Storage.dataDir.toString()).append("/").append(table_name).append(".dat");
        System.out.println(Data_Storage.subsel_flag);
        if (Data_Storage.subsel_flag==0 && table_name != null) {
            Data_Storage.oper = new File_IteratorInteface(new File(str.toString()));
            Data_Storage.subsel_flag = 1;
        }
        Data_Storage.tablename = table_name;
        Data_Storage.star_flag = 0;
//        System.out.println("The Selected Columns are : " + columns);
        Data_Storage.selectedColumns.clear();
        Expr_Visitor expr_visitor = new Expr_Visitor();
        if (plainSelect.getWhere() != null) {
            plainSelect.getWhere().accept(expr_visitor);
//            System.out.println("The Expression is : " + expr_visitor.getExpr());
            expr = expr_visitor.getExpr();
            Data_Storage.oper = new Eval_IteratorInteface(Data_Storage.oper,schema,expr, table_name);
        }
        SelectItem_Visitor select_Item = new SelectItem_Visitor();
        List<SelectItem> columns = plainSelect.getSelectItems();


            for (SelectItem col : columns) {
                col.accept(select_Item);
            }
            sel_expr = select_Item.SelectitemExpr();
        schema = select_Item.retSchema();
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
    public Expression retSelectExpr() {return sel_expr;}
    public List<String> retSchema(){return schema;}
}
