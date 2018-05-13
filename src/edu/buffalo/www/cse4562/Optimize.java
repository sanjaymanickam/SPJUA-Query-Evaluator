package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.schema.Column;

import java.util.*;

public class Optimize {
    ProjectionIterator_Interface to_project = null;
    AggregateProjection to_aggr = null;
    Expression expr_to_iterate = null;
    List<Expression> expressions_list = new ArrayList<>();
    LinkedHashMap<String,Iterator_Interface> join_list = new LinkedHashMap<>();
    Expression before_expression;
    List<Expression> before_expression_list = new ArrayList<>();
    List<String> join_name = new ArrayList<>();
    public Iterator_Interface optimize(Iterator_Interface to_optimize) {
        Data_Storage.oper = to_optimize;
        Iterator_Interface old_itereator = to_optimize;
        //To separate all the Iterator Interface's
        while(Data_Storage.oper!=null)
        {
            if(Data_Storage.oper instanceof AggregateProjection)
            {
                to_aggr = (AggregateProjection) Data_Storage.oper;
            }
            if(Data_Storage.oper instanceof ProjectionIterator_Interface)
            {
                to_project = (ProjectionIterator_Interface) Data_Storage.oper;
            }
            else if(Data_Storage.oper instanceof EvalIterator_Interface)
            {
                expr_to_iterate = ((EvalIterator_Interface) Data_Storage.oper).condition;
            }
            else if(Data_Storage.oper instanceof JoinIteratorInterface)
            {
                JoinIteratorInterface temp_join = (JoinIteratorInterface) Data_Storage.oper;
                if(temp_join.iter2 instanceof FileIterator_Interface)
                {
                    join_list.put(temp_join.table2,temp_join.iter2);
                    if(temp_join.table2!=null)
                        join_name.add(temp_join.table2);
                }
                if(temp_join.iter1 instanceof FileIterator_Interface)
                {
                    join_list.put(temp_join.table1,temp_join.iter1);
                    if(temp_join.iter1 instanceof FileIterator_Interface)
                        join_name.add(temp_join.table1);
                    break;
                }
            }
            Data_Storage.oper = Data_Storage.oper.getChild();
        }
        //To split the expression into base expressions
        while(expr_to_iterate!=null)
        {
            if(expr_to_iterate instanceof BinaryExpression)
            {
                BinaryExpression binaryExpression = (BinaryExpression) expr_to_iterate;
                if(binaryExpression instanceof OrExpression)
                {
                    add_join(binaryExpression);
                }
                else if(binaryExpression instanceof AndExpression)
                {
                    AndExpression andExpression = (AndExpression)binaryExpression;
                    expressions_list.add(andExpression.getRightExpression());
                    add_join(andExpression.getRightExpression());
                }
                else
                {
                    if(binaryExpression.getLeftExpression() instanceof Column && binaryExpression.getRightExpression() instanceof Column)
                        before_expression_list.add(binaryExpression);
                    else
                        expressions_list.add(binaryExpression);
                }
                expr_to_iterate = binaryExpression.getLeftExpression();
            }
            else if(expr_to_iterate instanceof Column)
                break;
        }
        if(Data_Storage.tableSize.size()==2)
            if (Data_Storage.tableSize.get(join_name.get(0)) > Data_Storage.tableSize.get(join_name.get(1)))
                Collections.swap(join_name, 0, 1);
        Iterator before_iter = before_expression_list.iterator();
//        while(before_iter.hasNext())
//        {
//            BinaryExpression binaryExpression = (BinaryExpression)before_iter.next();
//            if(binaryExpression instanceof OrExpression)
//            {
//                Column col1 = (Column)((BinaryExpression)binaryExpression.getLeftExpression()).getLeftExpression();
//                Column col2 = (Column)((BinaryExpression)binaryExpression.getRightExpression()).getLeftExpression();
//                if(col1.getTable().getName().equals(col2.getTable().getName()))
//                {
//                    int join_index = join_name.indexOf(col1.getTable().getName());
//                    join_list.replace(col1.getTable().getName(),new EvalIterator_Interface(join_list.get(join_index),binaryExpression));
//                }
//                before_expression_list.remove(binaryExpression);
//                break;
//            }
//        }
        Iterator_Interface join_iter = null;
        Iterator expr_iter = before_expression_list.iterator();
        for(int i=join_list.size()-1;i>0;i--) {
            String table1, table2;

                table2 = join_name.get(i);
                table1 = join_name.get(i-1);
                int count = 0;
                int flag=0;
                while(expr_iter.hasNext())
                {
                    Expression expr = (Expression) expr_iter.next();
                    Column col[] = split(expr);
                    String checktable1,checktable2;
                    checktable1 = col[0].getTable().getName();
                    checktable2 = col[1].getTable().getName();
                    if(Data_Storage.table_alias.containsKey(checktable1))
                        checktable1 = Data_Storage.table_alias.get(checktable1);
                    if(Data_Storage.table_alias.containsKey(checktable2))
                        checktable2 = Data_Storage.table_alias.get(checktable2);
                    if((table1.equals(checktable1) || table1.equals(checktable2)) && (table2.equals(checktable1) || table2.equals(checktable2)))
                    {
                        flag=1;
                    }
                    count++;
                }
                if(join_iter==null)
                    join_iter = new JoinIteratorInterface(join_list.get(table1),join_list.get(table2));
                else
                    join_iter = new JoinIteratorInterface(join_iter,join_list.get(table1));
                if(flag == 1)
                    join_iter = new IndexNestedLoopJoin(((JoinIteratorInterface)join_iter).iter1,((JoinIteratorInterface)join_iter).iter2,before_expression_list.get(count-1));
                flag = 0;
        }
        Iterator_Interface to_send = null;
        if(to_project!=null)
            to_send = new ProjectionIterator_Interface(to_project.selectedColumns,join_iter);
        if(to_aggr!=null && to_project!=null) {
            to_send = new AggregateProjection(to_send, to_aggr.selectedColumns);
        }
        else {
            to_send = new AggregateProjection(join_iter,to_aggr.selectedColumns);
        }
        return to_send;
    }
    public void add_join(Expression expr)
    {
        BinaryExpression binaryExpression = (BinaryExpression) expr;
        if(binaryExpression.getRightExpression() instanceof Column && binaryExpression.getLeftExpression() instanceof Column) {
            before_expression_list.add(expr);
        }
        else if(binaryExpression instanceof OrExpression)
        {
            BinaryExpression binaryExpression1 = (BinaryExpression) binaryExpression.getLeftExpression();
            BinaryExpression binaryExpression2 = (BinaryExpression) binaryExpression.getRightExpression();
            String table_name1 = ((Column)binaryExpression1.getLeftExpression()).getTable().getName();
            if(Data_Storage.table_alias.containsKey(table_name1))
                table_name1 = Data_Storage.table_alias.get(table_name1);
            if (!join_list.containsKey(table_name1)) {
                join_list.put(table_name1, new EvalIterator_Interface(new FileIterator_Interface(table_name1, Data_Storage.table_alias.get(table_name1),true), expr));
            } else {
                join_list.replace(table_name1, new EvalIterator_Interface(join_list.get(table_name1), expr));
            }
        }
        else
        {
            Column col = (Column) binaryExpression.getLeftExpression();
            String table_name = col.getTable().getName();
            if(Data_Storage.table_alias.containsKey(table_name))
                table_name = Data_Storage.table_alias.get(table_name);
            if (!join_list.containsKey(table_name)) {
                join_list.put(table_name, new EvalIterator_Interface(new FileIterator_Interface(table_name, Data_Storage.table_alias.get(table_name),true), expr));
            } else {
                join_list.replace(table_name, new EvalIterator_Interface(join_list.get(table_name), expr));
            }
        }
    }
    public Column[] split(Expression expr)
    {
        return new Column[]{(Column)((BinaryExpression)expr).getLeftExpression(),(Column)((BinaryExpression)expr).getRightExpression()};
    }
}
