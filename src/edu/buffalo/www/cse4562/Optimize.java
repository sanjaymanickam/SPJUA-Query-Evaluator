package edu.buffalo.www.cse4562;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.schema.Column;
import java.util.ArrayList;


public class Optimize {
    Iterator_Interface after_join_recursive_iter;
    public void optimize()
    {

        Iterator_Interface to_iter = Data_Storage.oper;
        while(to_iter!= null)
        {
            if(to_iter instanceof JoinIterator_Interface)
            {

            }
            else if(to_iter instanceof EvalIterator_Interface)
            {
                if(to_iter.getChild() instanceof JoinIterator_Interface)
                {
                    EvalIterator_Interface evalIterator_interface = (EvalIterator_Interface) to_iter;
                    Expression expr = evalIterator_interface.condition;
                    while(expr!=null)
                    {
                        if(expr instanceof AndExpression)
                        {
                            AndExpression andExpression = (AndExpression) expr;
                            evaluate(andExpression.getRightExpression(), "Optimize");
                            expr = andExpression.getLeftExpression();
                        }
                        else if(expr instanceof OrExpression)
                        {
                            OrExpression orExpression = (OrExpression) expr;
                            evaluate(orExpression.getRightExpression(), "Optimize");
                            expr = orExpression.getLeftExpression();
                        }
                        else
                        {
                            evaluate(expr, "Optimize");
                            expr = null;
                        }
                        Data_Storage.oper = to_iter.getChild();
                    }
                }
            }
            else if(to_iter instanceof ProjectionIterator_Interface)
            {

            }
        to_iter = to_iter.getChild();
        }
    }
    public void evaluate(Expression expression, String from)
    {
        if(expression instanceof BinaryExpression) {
            if(((BinaryExpression) expression).getLeftExpression() instanceof Column)
            {
                Column col = (Column) ((BinaryExpression) expression).getLeftExpression();
                String tableName = col.getTable().getName();
                System.out.println(tableName);
                System.out.println(col.getColumnName());
                if(from.equals("Optimize")){
                    if(!Data_Storage.operator_map.containsKey(tableName))
                    {
                        after_join_recursive_iter = new EvalIterator_Interface(new FileIterator_Interface(tableName),expression);
                        Data_Storage.operator_map.put(tableName,after_join_recursive_iter);
                    }
                    else {
                        after_join_recursive_iter = Data_Storage.operator_map.get(tableName);
                        after_join_recursive_iter = new EvalIterator_Interface(after_join_recursive_iter, expression);
                        Data_Storage.operator_map.replace(tableName, after_join_recursive_iter);
                    }
                }
                else {
                    updateProjectColumns(col.getColumnName(),tableName);
                }
            }
        }
    }
    public void updateProjectColumns(String columnName, String tableName) {
        ArrayList<String> temp_list = Data_Storage.project_columns.get(tableName);
        if (!temp_list.contains(columnName)) {
            temp_list.add(columnName);
            Data_Storage.project_columns.replace(tableName, temp_list);
        }
    }
}
