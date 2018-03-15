package edu.buffalo.www.cse4562;


import net.sf.jsqlparser.expression.Expression;

public class Optimize_2 {
    public void optimize()
    {
        while(Data_Storage.oper!=null)
        {
            if(Data_Storage.oper instanceof ProjectionIterator_Interface)
            {

            }
            else if(Data_Storage.oper instanceof EvalIterator_Interface)
            {
               if(Data_Storage.oper.getChild() instanceof JoinIterator_Interface)
               {

//                    Expression expr = ((EvalIterator_Interface) Data_Storage.oper).condition;



               }
            }
            else if(Data_Storage.oper instanceof JoinIterator_Interface)
            {

            }
        }
    }
    public void evaluate(Expression expr)
    {
        while(expr!=null)
        {

        }
    }
}
