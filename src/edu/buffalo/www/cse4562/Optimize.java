package edu.buffalo.www.cse4562;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.schema.Column;
import java.io.File;

public class Optimize {
    int flag=0;
    public void optimize()
    {
        Iterator_Interface to_overload = null;
        Iterator_Interface iter = Data_Storage.oper;
        while(iter!=null) {
            if (iter instanceof EvalIterator_Interface) {
                if(iter.getChild() instanceof JoinIterator_Interface) {
                    EvalIterator_Interface evalIterator_interface = (EvalIterator_Interface) iter;
                    Expression expr_right = evalIterator_interface.condition;
                    while (expr_right != null) {
                        if (expr_right instanceof AndExpression) {
                            if (((AndExpression) expr_right).getRightExpression() != null) {
                                if (((AndExpression) expr_right).getRightExpression() instanceof GreaterThan) {
                                    GreaterThan greaterThan = (GreaterThan) ((AndExpression) expr_right).getRightExpression();
                                    if(((AndExpression) expr_right).getLeftExpression() instanceof Column) {
                                        greater_than(greaterThan, ((Column) ((AndExpression) expr_right).getLeftExpression()).getTable().getName());
                                    }
                                }
                            }
                            expr_right = ((AndExpression) expr_right).getLeftExpression();
                        }
                        else
                        {
                            if(expr_right instanceof GreaterThan)
                            {
                                GreaterThan greaterThan = (GreaterThan)expr_right;
                                if(greaterThan.getLeftExpression() instanceof Column)
                                {
                                    Column col = (Column) greaterThan.getLeftExpression();
                                    greater_than(greaterThan, col.getTable().getName());
                                }
                            }
                            expr_right = null;
                        }
                    }
                    System.out.println(evalIterator_interface.condition);
                    System.out.println("EVAL ITERATOR INTERFACE");
                }
            } else if (iter instanceof JoinIterator_Interface) {
                System.out.println("JOIN ITERATOR INTERFACE");
            } else if (iter instanceof FileIterator_Interface) {
                System.out.println("FILE ITERATOR INTERFACE");
            }
            iter = iter.getChild();
        }
        System.out.println();
    }

    private void greater_than(GreaterThan greaterThan, String table_name) {
        Iterator_Interface to_overload;
        if(!Data_Storage.operator_map.containsKey(table_name))
        {
            to_overload = new EvalIterator_Interface(new FileIterator_Interface(new File("data/" + table_name + ".dat")), greaterThan);
            Data_Storage.operator_map.put(table_name,to_overload);
        }
        else
        {
            to_overload = Data_Storage.operator_map.get(table_name);
            to_overload = new EvalIterator_Interface(to_overload,greaterThan);
            Data_Storage.operator_map.put(table_name,to_overload);
        }
    }
}
