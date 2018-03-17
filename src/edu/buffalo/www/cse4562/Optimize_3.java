package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.Join;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Optimize_3 {
    List<Expression> expressionList = new ArrayList<>();
    List<Iterator_Interface> joins = new ArrayList<>();
    Iterator_Interface to_ret = null;
    Iterator_Interface expr_before_join = null;
    Iterator_Interface join_iter = null;
    ProjectionIterator_Interface projectionIterator_interface = null;
    Expression and_expr = null;
    public Iterator_Interface optimize()
    {
        to_ret = Data_Storage.oper;
        while(Data_Storage.oper!= null)
        {
            if(Data_Storage.oper instanceof ProjectionIterator_Interface)
            {
                projectionIterator_interface = (ProjectionIterator_Interface)Data_Storage.oper;
            }
            else if(Data_Storage.oper instanceof EvalIterator_Interface)
            {
                expressionList.add(((EvalIterator_Interface) Data_Storage.oper).condition);
            }
            else if(Data_Storage.oper instanceof Join2IteratorInterface)
            {
                Iterator_Interface temp_iter = ((Join2IteratorInterface) Data_Storage.oper).iter2;
                if(temp_iter instanceof FileIterator_Interface) {
                    joins.add(temp_iter);
                }
                else if(temp_iter instanceof Join2IteratorInterface)
                {
                    joins.add(temp_iter);
                }
                temp_iter = ((Join2IteratorInterface) Data_Storage.oper).iter1;
                if(temp_iter instanceof FileIterator_Interface) {
                    joins.add(temp_iter);
                }
                else if(temp_iter instanceof Join2IteratorInterface)
                {
                    joins.add(temp_iter);
                }

            }
            Data_Storage.oper = Data_Storage.oper.getChild();
        }
        Iterator expression_iterator = expressionList.iterator();
        Iterator join_iterator = joins.iterator();
//        while(expression_iterator.hasNext())
//            System.out.println(expression_iterator.next().toString());
//        while(join_iterator.hasNext()) {
//            System.out.println(((FileIterator_Interface)join_iterator.next()).new_file);
//        }
        Iterator it = expressionList.iterator();
        while(it.hasNext())
        {
            Expression expr =(Expression) it.next();
            while(expr!=null)
            {
                if(expr instanceof BinaryExpression) {
                    if (expr instanceof OrExpression) {
                        if (expr_before_join == null)
                            expr_before_join = new EvalIterator_Interface(join_iter, expr);
                    } else if (expr instanceof AndExpression) {
                        if (((AndExpression) expr).getRightExpression() instanceof OrExpression) {
                            if (expr_before_join == null)
                                expr_before_join = new EvalIterator_Interface(join_iter, ((AndExpression) expr).getRightExpression());
                            else
                                expr_before_join = new EvalIterator_Interface(expr_before_join, ((AndExpression) expr).getRightExpression());
                        } else if (((AndExpression) expr).getRightExpression() instanceof BinaryExpression) {
                            BinaryExpression binaryExpression = (BinaryExpression) ((AndExpression) expr).getRightExpression();
                            bin_eval(binaryExpression);
                        }
                    }
                    else
                    {

                        BinaryExpression binaryExpression = (BinaryExpression) expr;
                        bin_eval(binaryExpression);
                    }
                    expr =((BinaryExpression)expr).getLeftExpression();
                }
                else
                {
                    expr = null;
                }
            }
            for(int i=0;i<joins.size();i++)
            {
                if(joins.get(i) instanceof Join2IteratorInterface)
                {
                    joins.set(i,new Optimize_3().optimize());
                }
            }
            for(int i=joins.size()-1;i>=0;i--)
            {
                if(join_iter==null) {
                    join_iter = new Join2IteratorInterface(joins.get(i), joins.get((i--) - 1));
                }
                else
                    join_iter = new Join2IteratorInterface(join_iter,joins.get(i));
            }
        }
        if(join_iter!=null) {
            if (expr_before_join == null)
                expr_before_join = join_iter;
            return new ProjectionIterator_Interface(projectionIterator_interface.selectedColumns, expr_before_join);
        }
        else
        {
            return to_ret;
        }
    }

    private void bin_eval(BinaryExpression binaryExpression) {
        if (binaryExpression.getLeftExpression() instanceof Column && binaryExpression.getRightExpression() instanceof Column) {
                if(expr_before_join==null)
                    expr_before_join = new EvalIterator_Interface(join_iter,binaryExpression);
                else
                    expr_before_join = new EvalIterator_Interface(expr_before_join,binaryExpression);
        } else if (binaryExpression.getLeftExpression() instanceof Column) {
            Column col = (Column) binaryExpression.getLeftExpression();
            String file_name = col.getTable().getName();
            for (int i = 0; i < joins.size(); i++) {
                if(joins.get(i) instanceof FileIterator_Interface) {
                    FileIterator_Interface fileIterator_interface = (FileIterator_Interface) joins.get(i);
                    if (fileIterator_interface.new_file.equals(file_name)) {
                        joins.set(i, new EvalIterator_Interface(fileIterator_interface, binaryExpression));
                    }
                }
            }
        }
    }

}
