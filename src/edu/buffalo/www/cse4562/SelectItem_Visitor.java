package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

import javax.xml.crypto.Data;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.StringTokenizer;

public class SelectItem_Visitor {
    static Expression expr;
    public static void ret_type(SelectItem stmt)
    {
        if(stmt instanceof AllColumns)
        {
            Data_Storage.all_flag = 1;
        }
        else if(stmt instanceof AllTableColumns)
        {
            AllTableColumns allTableColumns = (AllTableColumns)stmt;
            String tableName = allTableColumns.getTable().getName();
            if(Data_Storage.table_alias.containsKey(tableName))
            {
                tableName = Data_Storage.table_alias.get(tableName);
            }
            LinkedHashMap<String,String> linkedHashMap = Data_Storage.tables.get(tableName);
            Iterator key_iterator = linkedHashMap.keySet().iterator();
            while(key_iterator.hasNext())
            {
                String to_append = key_iterator.next().toString();
                Data_Storage.selectedColumns.add(new Column(new Table(tableName),to_append));
                Data_Storage.project_array.add(to_append);
            }
        }
        else if(stmt instanceof SelectExpressionItem)
        {
            SelectExpressionItem selectExpressionItem = (SelectExpressionItem) stmt;
            String columnName = selectExpressionItem.getExpression().toString();
            if(selectExpressionItem.getExpression() instanceof Function)
            {
                Data_Storage.selectedColumns.add(new Column(new Table(),columnName));
                Data_Storage.aggregateflag = 1;
                Function func = (Function) selectExpressionItem.getExpression();
                System.out.println(func.getName());
                ExpressionList exprList = func.getParameters();
                Iterator itr = exprList.getExpressions().iterator();
                while(itr.hasNext()){
                    Expression agg_expr = (Expression) itr.next();
                    handleExpression(agg_expr);
                }
                Data_Storage.aggregate_operations.add(func);
            }
            else if(selectExpressionItem.getAlias()!= null)
            {
                if(columnName.indexOf(".") != -1)
                {
                    String tableName;
                    StringTokenizer strtok = new StringTokenizer(columnName,".");
                    tableName = strtok.nextElement().toString();
                    String colName = strtok.nextElement().toString();
                    Data_Storage.project_array.add(colName);
                }
                expr = selectExpressionItem.getExpression();
                Expr_Visitor expr_visitor = new Expr_Visitor();
                expr.accept(expr_visitor);
                if(expr_visitor.getExpr()!=null)
                {
                    Data_Storage.alias_table.put(columnName,selectExpressionItem.getAlias());
                    Data_Storage.selectedColumns.add(new Column(new Table(Data_Storage.current_schema.get(selectExpressionItem.getExpression().toString())),selectExpressionItem.getAlias()));
                }
            }
            else if(columnName.indexOf(".") != -1)
            {
                String tableName;
                StringTokenizer strtok = new StringTokenizer(columnName,".");
                tableName = strtok.nextElement().toString();
                String colName = strtok.nextElement().toString();
                Data_Storage.project_array.add(colName);
//                Data_Storage.alias_table.put(columnName,colName);
                Data_Storage.selectedColumns.add(new Column(new Table(tableName),colName));

            }
            else
            {
                String tableName = Data_Storage.current_schema.get(selectExpressionItem.getExpression().toString());
                Data_Storage.project_array.add(columnName);
                Data_Storage.selectedColumns.add(new Column(new Table(tableName),columnName));
            }
        }
    }
    public static void handleExpression(Expression agg_expr){
        if(agg_expr instanceof Column){
            Column col = (Column) agg_expr;
            if(!Data_Storage.aggregate.contains(col)){
                Data_Storage.aggregate.add(col);
            }
            if(!Data_Storage.project_array.contains(col.getColumnName())){
                Data_Storage.project_array.add(col.getColumnName());
            }
            String colName = col.getColumnName();
            String tableName = Data_Storage.current_schema.get(colName);
            Data_Storage.columns_needed_for_aggregate.put(new Column(new Table(tableName),colName).toString(),tableName);
            return;
        }
        if(agg_expr instanceof BinaryExpression){
            handleExpression(((BinaryExpression) agg_expr).getLeftExpression());
            handleExpression(((BinaryExpression) agg_expr).getRightExpression());
        }

    }
}
