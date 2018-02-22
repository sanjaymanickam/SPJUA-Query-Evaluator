package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;

import java.util.ArrayList;
import java.util.List;


public class SelectItem_Visitor implements SelectItemVisitor {
    net.sf.jsqlparser.expression.Expression expr;
    List<String> schema = new ArrayList<>();
    @Override
    public void visit(AllColumns allColumns) {
        schema = Data_Storage.tableColumns.get(new Select_Visitor().retTableName());
        expr = null;
    }

    @Override
    public void visit(AllTableColumns allTableColumns) {
        System.out.println("IN ALL TABLE COLUMNS");
    }

    @Override
    public void visit(SelectExpressionItem selectExpressionItem) {
        String columnName = selectExpressionItem.getExpression().toString();
        if (columnName.indexOf(".") != -1) {
            columnName = columnName.split("\\.")[1];
        }
        else if(selectExpressionItem.getAlias() != null) {
//            System.out.println("Alias is " + selectExpressionItem.getAlias());
            schema.add(selectExpressionItem.getAlias());
            Select_Visitor s_visit = new Select_Visitor();
            Data_Storage.tableColumns.get(s_visit.retTableName()).add(selectExpressionItem.getAlias());
            Expr_Visitor expr_visitor = new Expr_Visitor();
            selectExpressionItem.getExpression().accept(expr_visitor);
            expr = expr_visitor.getExpr();
            Data_Storage.oper = new Eval_IteratorInteface(Data_Storage.oper,schema,expr,s_visit.retTableName());

        }
        else {
            schema.add(columnName);
            expr = null;
        }

    }
    public Expression SelectitemExpr()
    {
        return expr;
    }
    public List<String> retSchema() {return schema;}
}
