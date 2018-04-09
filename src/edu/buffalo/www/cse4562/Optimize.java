package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.schema.Column;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Optimize {


    void add_projections(String col_name)
    {
        Data_Storage.project_array.add(col_name);
    }
    public Iterator_Interface optimize(Iterator_Interface to_optimize) {
        List<Expression> expressionList = new ArrayList<>();
        List<Iterator_Interface> joins = new ArrayList<>();
        Iterator_Interface to_ret = null;
        Iterator_Interface expr_before_join = null;
        List<Expression> beforeExpressionList = new ArrayList<>();
        Iterator_Interface join_iter = null;
        ProjectionIterator_Interface projectionIterator_interface = null;
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
                if (!(((JoinIteratorInterface) Data_Storage.oper).iter1 instanceof JoinIteratorInterface)) {
                    joins.add(((JoinIteratorInterface) Data_Storage.oper).iter1);
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
                            add_projections(col.getColumnName());
                        }
                        if(((BinaryExpression) ((BinaryExpression) expr).getRightExpression()).getRightExpression() instanceof Column)
                        {
                            Column col =(Column)((BinaryExpression) ((BinaryExpression) expr).getRightExpression()).getRightExpression();
                            add_projections(col.getColumnName());
                        }
                    }
                    if(((BinaryExpression) expr).getRightExpression() instanceof Column )
                    {
                        Column col = (Column) ((BinaryExpression) expr).getRightExpression();
                        add_projections(col.getColumnName());
                    }
                    if(((BinaryExpression) expr).getLeftExpression() instanceof  Column)
                    {
                        Column col = (Column) ((BinaryExpression) expr).getLeftExpression();
                        add_projections(col.getColumnName());
                    }

                    if (expr instanceof OrExpression) {
                        beforeExpressionList.add(expr);
                    } else if (expr instanceof AndExpression) {
                        if (((AndExpression) expr).getRightExpression() instanceof OrExpression) {
                            beforeExpressionList.add(((AndExpression) expr).getRightExpression());
                        } else if (((AndExpression) expr).getRightExpression() instanceof BinaryExpression) {
                            BinaryExpression binaryExpression = (BinaryExpression) ((AndExpression) expr).getRightExpression();
                            if (binaryExpression.getLeftExpression() instanceof Column && binaryExpression.getRightExpression() instanceof Column) {
                                beforeExpressionList.add(binaryExpression);
                            } else if (binaryExpression.getLeftExpression() instanceof Column) {
                                Column col = (Column) binaryExpression.getLeftExpression();
                                String file_name = col.getTable().getName();
                                String col_name = col.getColumnName();
                                //add_projections(file_name,col_name);
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
                            //add_projections(file_name,col_name);
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
        for (int i = 0; i < joins.size(); i++) {
            if (joins.get(i) instanceof ProjectionIterator_Interface) {
                joins.set(i, new Optimize().optimize(joins.get(i)));
            }
        }
        if(joins.size()==1)
        {
            join_iter = joins.get(0);
        }
        else {
            for (int i = joins.size() - 1; i >= 0; i--) {
                if (join_iter == null) {
                    join_iter = new JoinIteratorInterface(joins.get(i), joins.get((i--) - 1));
                } else
                    join_iter = new JoinIteratorInterface(join_iter, joins.get(i));
            }
        }
        Iterator expr_before_iter = beforeExpressionList.iterator();
        while (expr_before_iter.hasNext()) {
            if (expr_before_join == null) {
                expr_before_join = new EvalIterator_Interface(join_iter, (Expression) expr_before_iter.next());
            } else
                expr_before_join = new EvalIterator_Interface(expr_before_join, (Expression) expr_before_iter.next());
        }
        if (join_iter != null) {
            if (expr_before_join == null)
                expr_before_join = join_iter;
            return new ProjectionIterator_Interface(projectionIterator_interface.selectedColumns, expr_before_join);
        } else {
            return to_ret;
        }
    }
}
