package edu.buffalo.www.cse4562;


import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.Join;

import java.util.ArrayList;
import java.util.List;

public class Optimize_2 {
    OrExpression or = new OrExpression();
    List<Expression> eval_before_join = new ArrayList<>();
    List<Expression> eval_after_join = new ArrayList<>();
    List<Iterator_Interface> joins = new ArrayList<>();
    public void optimize()
    {
        while(Data_Storage.oper!=null)
        {
            if(Data_Storage.oper instanceof ProjectionIterator_Interface)
            {

            }
            else if(Data_Storage.oper instanceof EvalIterator_Interface)
            {
               if(Data_Storage.oper.getChild() instanceof Join2IteratorInterface)
               {

                   Expression expr = ((EvalIterator_Interface) Data_Storage.oper).condition;
                   Iterator_Interface file1_iter = (((Join2IteratorInterface) Data_Storage.oper.getChild()).iter1);
                   Iterator_Interface file2_iter = (((Join2IteratorInterface) Data_Storage.oper.getChild()).iter2);
                   String file2name = ((FileIterator_Interface)file2_iter).new_file;
                   String file1name = null;
                   if(file1_iter instanceof FileIterator_Interface)
                        file1name = ((FileIterator_Interface) file1_iter).new_file;
                   while(expr!=null)
                   {
                       if(expr instanceof AndExpression)
                       {
//                           AndExpression andExpression = (AndExpression) expr;
//                           Boolean bool = evaluate(andExpression.getRightExpression(),file1name,file2name);
//                           if(bool)
//                           {
//                                Data_Storage.oper.setChild(new EquiJoinIterator_Interface(file1name,file2name,andExpression.getRightExpression()));
//                           }

//                           expr = andExpression.getLeftExpression();
                       }
                       else if(expr instanceof OrExpression)
                       {
                           OrExpression orExpression = (OrExpression) expr;
//                           evaluate(orExpression.getRightExpression());
                           expr = orExpression.getLeftExpression();
                       }
                       else
                       {
                           Boolean bool = evaluate(expr,file1name,file1name);
                           expr = null;
                       }
                   }

               }
            }
            else if(Data_Storage.oper instanceof JoinIterator_Interface)
            {

            }
            Data_Storage.oper = Data_Storage.oper.getChild();
        }
    }
    public boolean evaluate(Expression expr,String file1,String file2)
    {
        int flag = 0;
        if(expr instanceof BinaryExpression)
        {
            if(((BinaryExpression) expr).getRightExpression() instanceof Column && ((BinaryExpression) expr).getLeftExpression() instanceof Column)
            {
                if(expr instanceof EqualsTo)
                {
                    EqualsTo equalsTo = (EqualsTo) expr;
                    if((((Column)equalsTo.getLeftExpression()).getTable().getName().equals(file1)||((Column)equalsTo.getRightExpression()).getTable().getName().equals(file1)) &&
                            (((Column)equalsTo.getLeftExpression()).getTable().getName().equals(file2)||((Column)equalsTo.getRightExpression()).getTable().getName().equals(file2)))
                    {
                        flag=1;
                    }
                }
            }
        }
        if(flag==1)
        {
            return  true;
        }
        else {
            return false;
        }
    }
    public void expression_eval(Expression expr)
    {
        if(expr instanceof BinaryExpression)
        {
            if(((BinaryExpression) expr).getRightExpression() instanceof Column && ((BinaryExpression) expr).getLeftExpression() instanceof Column)
            {

            }
            else if(((BinaryExpression) expr).getRightExpression() instanceof Column)
            {
                Column col = (Column) ((BinaryExpression) expr).getRightExpression();
                Iterator_Interface temp_iter;
                temp_iter = new FileIterator_Interface(col.getTable().getName());
                temp_iter = new EvalIterator_Interface(temp_iter,expr);
                eval_after_join.add(expr);
            }
        }
    }
}
