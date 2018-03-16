package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.Expression;

import javax.xml.crypto.Data;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Optimize_3 {
    List<Expression> expressionList = new ArrayList<>();
    List<Iterator_Interface> joins = new ArrayList<>();
    public Iterator_Interface optimize()
    {
        while(Data_Storage.oper!= null)
        {
            if(Data_Storage.oper instanceof ProjectionIterator_Interface)
            {

            }
            else if(Data_Storage.oper instanceof EvalIterator_Interface)
            {
                expressionList.add(((EvalIterator_Interface) Data_Storage.oper).condition);
            }
            else if(Data_Storage.oper instanceof Join2IteratorInterface)
            {
                Iterator_Interface temp_iter = ((Join2IteratorInterface) Data_Storage.oper).iter2;
                joins.add(temp_iter);
                temp_iter = ((Join2IteratorInterface) Data_Storage.oper).iter1;
                if(temp_iter instanceof FileIterator_Interface) {
                    joins.add(temp_iter);
                }

            }
            Data_Storage.oper = Data_Storage.oper.getChild();
        }
        Iterator expression_iterator = expressionList.iterator();
        Iterator join_iterator = joins.iterator();
        while(expression_iterator.hasNext())
            System.out.println(expression_iterator.next().toString());
        while(join_iterator.hasNext()) {
            System.out.println(((FileIterator_Interface)join_iterator.next()).new_file);
        }
    return null;
    }
}
