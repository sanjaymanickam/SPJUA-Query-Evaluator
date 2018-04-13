package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.schema.Column;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Optimize {

    public Iterator_Interface optimize(Iterator_Interface to_optimize) {
        List<Expression> expressionList = new ArrayList<>();
        List<Iterator_Interface> joins = new ArrayList<>();
        Iterator_Interface to_ret = null;
        Iterator_Interface expr_before_join = null;
        List<Expression> beforeExpressionList = new ArrayList<>();
        Iterator_Interface join_iter = null;
        ArrayList<String> right_table = new ArrayList<>();
        ArrayList<Column> join_names1 = new ArrayList<>();
        ArrayList<Column> join_names2 = new ArrayList<>();
        ArrayList<String> join_name1_table  = new ArrayList<>();
        ArrayList<String> join_name2_table = new ArrayList<>();
        ArrayList<Expression> join_condition = new ArrayList<>();
        ProjectionIterator_Interface projectionIterator_interface = null;
        ArrayList<String> hashjoin_names = new ArrayList<>();
        to_ret = to_optimize;
        Data_Storage.oper = to_optimize;
        while (Data_Storage.oper != null) {
            if (Data_Storage.oper instanceof ProjectionIterator_Interface) {
                if(projectionIterator_interface==null)
                    projectionIterator_interface = (ProjectionIterator_Interface) Data_Storage.oper;
                else {
                    joins.add(Data_Storage.oper);
                    break;
                }
            } else if (Data_Storage.oper instanceof EvalIterator_Interface) {
                expressionList.add(((EvalIterator_Interface) Data_Storage.oper).condition);
            } else if (Data_Storage.oper instanceof JoinIteratorInterface) {
                Iterator_Interface temp_iter = ((JoinIteratorInterface) Data_Storage.oper).iter2;
                joins.add(((JoinIteratorInterface) Data_Storage.oper).iter2);
                if((((JoinIteratorInterface) Data_Storage.oper).iter2) instanceof FileIterator_Interface)
                {
                    hashjoin_names.add(((FileIterator_Interface)((JoinIteratorInterface) Data_Storage.oper).iter2).new_file);
                }
                if (!(((JoinIteratorInterface) Data_Storage.oper).iter1 instanceof JoinIteratorInterface)) {
                    joins.add(((JoinIteratorInterface) Data_Storage.oper).iter1);
                    if((((JoinIteratorInterface) Data_Storage.oper).iter1) instanceof FileIterator_Interface)
                    {
                        hashjoin_names.add(((FileIterator_Interface)((JoinIteratorInterface) Data_Storage.oper).iter1).new_file);
                    }
                    break;
                }
            }

            Data_Storage.oper = Data_Storage.oper.getChild();
        }
        Iterator it = expressionList.iterator();
        while (it.hasNext()) {
            Expression expr = (Expression) it.next();
            while (expr != null) {
                if (expr instanceof BinaryExpression) {
                    if(((BinaryExpression) expr).getRightExpression() instanceof BinaryExpression)
                    {
                        if(((BinaryExpression) ((BinaryExpression) expr).getRightExpression()).getLeftExpression() instanceof Column)
                        {
                            Column col =(Column)((BinaryExpression) ((BinaryExpression) expr).getRightExpression()).getLeftExpression();
//                            add_projections(col.getColumnName());
                        }
                        if(((BinaryExpression) ((BinaryExpression) expr).getRightExpression()).getRightExpression() instanceof Column)
                        {
                            Column col =(Column)((BinaryExpression) ((BinaryExpression) expr).getRightExpression()).getRightExpression();
//                            add_projections(col.getColumnName());
                        }
                    }
                    if(((BinaryExpression) expr).getRightExpression() instanceof Column )
                    {
                        Column col = (Column) ((BinaryExpression) expr).getRightExpression();
//                        add_projections(col.getColumnName());
                    }
                    if(((BinaryExpression) expr).getLeftExpression() instanceof  Column)
                    {
                        Column col = (Column) ((BinaryExpression) expr).getLeftExpression();
//                        add_projections(col.getColumnName());
                    }

                    if (expr instanceof OrExpression) {
                        BinaryExpression binaryExpression_left = (BinaryExpression) ((OrExpression) expr).getLeftExpression();
                        BinaryExpression binaryExpression_right = (BinaryExpression) ((OrExpression) expr).getRightExpression();
//                        if(binaryExpression_left.getLeftExpression() instanceof Column && binaryExpression_left.getRightExpression() instanceof Column)
//                        {
//                            Column left =(Column) binaryExpression_left.getLeftExpression();
//                            Column right = (Column) binaryExpression_right.getLeftExpression();
//                            if(left.getTable().getName().equals(right.getTable().getName()))
//                            {
//                                for (int i = 0; i < joins.size(); i++) {
//                                    if (joins.get(i) instanceof FileIterator_Interface) {
//                                        FileIterator_Interface fileIterator_interface = (FileIterator_Interface) joins.get(i);
//                                        if (fileIterator_interface.new_file.equals(left.getTable().getName())) {
//                                            joins.set(i, new EvalIterator_Interface(fileIterator_interface, expr));
//                                            break;
//                                        }
//                                    }
//                                }
//                            }
//                        }
                        beforeExpressionList.add(expr);
                    } else if (expr instanceof AndExpression) {
                        if (((AndExpression) expr).getRightExpression() instanceof OrExpression) {
                            Expression temp = expr;
                            OrExpression new_expr = (OrExpression) ((AndExpression) expr).getRightExpression();
                            BinaryExpression binaryExpression_left = (BinaryExpression) new_expr.getLeftExpression();
                            BinaryExpression binaryExpression_right = (BinaryExpression) new_expr.getRightExpression();
                            if (binaryExpression_left.getLeftExpression() instanceof Column && binaryExpression_right.getLeftExpression() instanceof Column) {
                                Column left = (Column) binaryExpression_left.getLeftExpression();
                                Column right = (Column) binaryExpression_right.getLeftExpression();
                                if (left.getTable().getName().equals(right.getTable().getName())) {
                                    for (int i = 0; i < joins.size(); i++) {
                                        if(hashjoin_names.get(i).equals(left.getTable().getName())) {
                                            if (joins.get(i) instanceof FileIterator_Interface) {
                                                FileIterator_Interface fileIterator_interface = (FileIterator_Interface) joins.get(i);
                                                joins.set(i, new EvalIterator_Interface(fileIterator_interface, new_expr));
                                                break;
                                            }
                                            else if(joins.get(i) instanceof EvalIterator_Interface){
                                                joins.set(i,new EvalIterator_Interface(joins.get(i),new_expr));
                                            }
                                        }
                                    }
                                }
                            }
                            else {
                                beforeExpressionList.add(((AndExpression) expr).getRightExpression());
                            }
//                            while(temp!=null)
//                            {
//                                if(temp instanceof BinaryExpression)
//                                {
//                                    BinaryExpression binaryExpression = (BinaryExpression) temp;
//                                    if(binaryExpression.getLeftExpression() instanceof Column)
//                                    {
//                                        Column col = (Column) binaryExpression.getLeftExpression();
////                                        add_projections(col.getColumnName());
//                                    }
//                                    if(binaryExpression.getRightExpression() instanceof Column)
//                                    {
//                                        Column col = (Column) binaryExpression.getRightExpression();
////                                        add_projections(col.getColumnName());
//                                    }
//                                    temp = ((BinaryExpression) temp).getLeftExpression();
//                                }
//                                else
//                                {
//                                    temp = null;
//                                }


                        } else if (((AndExpression) expr).getRightExpression() instanceof BinaryExpression) {
                            BinaryExpression binaryExpression = (BinaryExpression) ((AndExpression) expr).getRightExpression();
                            if (binaryExpression.getLeftExpression() instanceof Column && binaryExpression.getRightExpression() instanceof Column) {
                                beforeExpressionList.add(binaryExpression);
                            } else if (binaryExpression.getLeftExpression() instanceof Column) {
                                Column col = (Column) binaryExpression.getLeftExpression();
                                String file_name = col.getTable().getName();
                                String col_name = col.getColumnName();
                                if(file_name == null)
                                {
                                    if(Data_Storage.current_schema.containsKey(col.getColumnName()))
                                    {
                                        file_name = Data_Storage.current_schema.get(col.getColumnName());
                                    }
                                }
                                if (Data_Storage.table_alias.containsKey(file_name))
                                    file_name = Data_Storage.table_alias.get(file_name);
                                for (int i = 0; i < joins.size(); i++) {
                                    if (joins.get(i) instanceof FileIterator_Interface) {
                                        FileIterator_Interface fileIterator_interface = (FileIterator_Interface) joins.get(i);
                                        if (fileIterator_interface.new_file.equals(file_name)) {
                                            joins.set(i, new EvalIterator_Interface(fileIterator_interface, binaryExpression));
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    } else {

                        BinaryExpression binaryExpression = (BinaryExpression) expr;
                        if (binaryExpression.getLeftExpression() instanceof Column && binaryExpression.getRightExpression() instanceof Column) {
                            beforeExpressionList.add(binaryExpression);
                        } else if (binaryExpression.getLeftExpression() instanceof Column) {
                            Column col = (Column) binaryExpression.getLeftExpression();
                            String file_name = col.getTable().getName();
                            String col_name = col.getColumnName();
                            if(file_name == null)
                            {
                                if(Data_Storage.current_schema.containsKey(col.getColumnName()))
                                {
                                    file_name = Data_Storage.current_schema.get(col.getColumnName());
                                }
                            }
                            if (Data_Storage.table_alias.containsKey(file_name))
                                file_name = Data_Storage.table_alias.get(file_name);
                            for (int i = 0; i < joins.size(); i++) {
                                if (joins.get(i) instanceof FileIterator_Interface) {
                                    FileIterator_Interface fileIterator_interface = (FileIterator_Interface) joins.get(i);

                                    if (fileIterator_interface.new_file.equals(file_name)) {
                                        joins.set(i, new EvalIterator_Interface(fileIterator_interface, binaryExpression));
                                    }
                                }
                            }
                        }
                    }
                    expr = ((BinaryExpression) expr).getLeftExpression();
                } else {
                    expr = null;
                }
            }
        }
        Iterator before_join_iterator = beforeExpressionList.iterator();
        for (int i = 0; i < joins.size(); i++) {
            if (joins.get(i) instanceof ProjectionIterator_Interface) {
                joins.set(i, new Optimize().optimize(joins.get(i)));
            }
        }
        while(before_join_iterator.hasNext())
        {
            Expression expr =(Expression) before_join_iterator.next();
            if(expr instanceof BinaryExpression)
            {
                if(((BinaryExpression) expr).getLeftExpression() instanceof Column && ((BinaryExpression) expr).getRightExpression() instanceof Column)
                {
                    Column left_col = (Column)((BinaryExpression) expr).getLeftExpression();
                    Column right_col = (Column)((BinaryExpression) expr).getRightExpression();
                    join_names1.add(left_col);
                    join_name1_table.add(left_col.getTable().getName());
                    join_names2.add(right_col);
                    join_name2_table.add(right_col.getTable().getName());
                    join_condition.add(expr);
                }
            }
        }
        ArrayList<String> table_list = new ArrayList<>(Data_Storage.tables.keySet());
        for(int t=0;t<joins.size();t++) {
            Expression expr;
            List<Expression> expr_list = new ArrayList<>();
            Iterator_Interface iter = joins.get(t);
            String tableName = table_list.get(t);
            Column col2=null,col1=null;
            for(int j = 0;j<join_names1.size();j++)
            {
                col1 = join_names1.get(j);
                col2 = join_names2.get(j);
                if(col1.getTable().getName().equals(tableName)||col2.getTable().getName().equals(tableName))
                {
                    expr_list.add(join_condition.get(j));
                }
            }
            if(expr_list.size()>1)
            {
                expr = new AndExpression();
                ((AndExpression) expr).setLeftExpression(expr_list.get(0));
                ((AndExpression) expr).setRightExpression(expr_list.get(0));
            }
//            joins.set(hashjoin_names.indexOf(tableName),null);
        }
        if(joins.size()==1)
        {
            join_iter = joins.get(0);
        }
        else {
            for (int i = joins.size() - 1; i >= 0; i--) {
                if (join_iter == null) {
                    join_iter = new JoinIteratorInterface(joins.get(i), joins.get((i--) - 1));
                    for(int t = 0;t<join_name1_table.size();t++)
                    {
                        if((join_name1_table.get(t).equals(hashjoin_names.get(t+1))&&join_name2_table.get(t).equals(hashjoin_names.get(t))) ||
                                (join_name2_table.get(t).equals(hashjoin_names.get(t+1))&&join_name1_table.get(t).equals(hashjoin_names.get(t))))
                        {
                            join_iter = new EvalIterator_Interface(join_iter, join_condition.get(i));
                            break;
                        }
                    }
//                    if(join_name1_table.contains(hashjoin_names.get(i)))
//                    {
//                        if(join_name2_table.get(join_name1_table.indexOf(hashjoin_names.get(i))).equals(hashjoin_names.get(i+1))) {
//                            int pos = join_name1_table.indexOf(hashjoin_names.get(i));
//
//                        }
//                    }
//                    else if(join_name2_table.contains(hashjoin_names.get(i)))
//                    {
//                        if(join_name1_table.get(join_name2_table.indexOf(hashjoin_names.get(i))).equals(hashjoin_names.get(i+1))) {
//                            int pos = join_name2_table.indexOf(hashjoin_names.get(i));
//                            join_iter = new EvalIterator_Interface(join_iter, join_condition.get(pos));
//                        }
//                    }
                } else {
                    join_iter = new JoinIteratorInterface(join_iter, joins.get(i));
                    if(join_name1_table.contains(hashjoin_names.get(i))||join_name2_table.contains(hashjoin_names.get(i)))
                    {
                        int pos = join_name1_table.contains(hashjoin_names.get(i))? join_name1_table.indexOf(hashjoin_names.get(i)):join_name2_table.indexOf(hashjoin_names.get(i));
                        join_iter = new EvalIterator_Interface(join_iter,join_condition.get(pos));
                    }
                }
            }
        }
        Iterator expr_before_iter = beforeExpressionList.iterator();
//        while (expr_before_iter.hasNext()) {
//            if(expr_before_iter.)
//            {
//
//            }
//            else {
//                if (expr_before_join == null) {
//                    expr_before_join = new EvalIterator_Interface(join_iter, (Expression) expr_before_iter.next());
//                } else {
//                    expr_before_join = new EvalIterator_Interface(expr_before_join, (Expression) expr_before_iter.next());
//                }
//            }
//        }
        if (join_iter != null) {
            if (expr_before_join == null)
                expr_before_join = join_iter;
            return new ProjectionIterator_Interface(projectionIterator_interface.selectedColumns, expr_before_join);
        } else {
            return to_ret;
        }
    }
}
