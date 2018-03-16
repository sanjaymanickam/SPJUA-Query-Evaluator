package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

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

        }
        else if(stmt instanceof SelectExpressionItem)
        {
            SelectExpressionItem selectExpressionItem = (SelectExpressionItem) stmt;;
            String columnName = selectExpressionItem.getExpression().toString();
            if(selectExpressionItem.getAlias()!= null)
            {
                expr = selectExpressionItem.getExpression();
                Expr_Visitor expr_visitor = new Expr_Visitor();
                expr.accept(expr_visitor);
                if(expr_visitor.getExpr()!=null)
                {
                    Data_Storage.alias_table.put(selectExpressionItem.getAlias(), columnName);
                    Data_Storage.selectedColumns.put(selectExpressionItem.getAlias(), Data_Storage.current_schema.get(selectExpressionItem.getExpression().toString()));
                    Optimize opt = new Optimize();
                    //opt.updateProjectColumns(columnName, Data_Storage.current_schema.get(columnName));
                }
            }
            else if(columnName.indexOf(".") != -1)
            {
                String tableName;
                StringTokenizer strtok = new StringTokenizer(columnName,".");
                tableName = strtok.nextElement().toString();
                String colName = strtok.nextElement().toString();
                Data_Storage.alias_table.put(columnName,colName);
                Data_Storage.selectedColumns.put(columnName,tableName);
                Optimize opt = new Optimize();
                //opt.updateProjectColumns(colName,tableName);

            }
            else
            {
                String tableName = Data_Storage.current_schema.get(selectExpressionItem.getExpression().toString());
                Data_Storage.selectedColumns.put(tableName+"."+columnName, tableName);
                Optimize opt = new Optimize();
                //opt.updateProjectColumns(columnName, Data_Storage.current_schema.get(columnName));
            }
        }
    }
}
