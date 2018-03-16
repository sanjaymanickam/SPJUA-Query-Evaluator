package edu.buffalo.www.cse4562;

import com.sun.tools.corba.se.idl.constExpr.Or;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.*;

import javax.xml.crypto.Data;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Select_Visitor {
    static SelectBody selectBody;
    public static void ret_type(SelectBody stmt)
    {
        selectBody = stmt;
        Expression expression;

        if(stmt instanceof PlainSelect)
        {
            Data_Storage.limit = new Long("0");
            Data_Storage.orderBy_sort = new ArrayList<>();
            Data_Storage.orderBy = new ArrayList<>();
            PlainSelect plainSelect = (PlainSelect) stmt;

            From_Visitor.ret_type(plainSelect.getFromItem());
            List<Join> joins = plainSelect.getJoins();
            Iterator_Interface main_from_item_iter = Data_Storage.oper;
            Iterator_Interface join_iter = null;
            if(joins!=null)
            {
                Iterator it = joins.iterator();
                while(it.hasNext()) {
//                     Join_Visitor.ret_type(Data_Storage.oper,(Join) it.next());
                    Join join = (Join) it.next();
                    From_Visitor.ret_type(join.getRightItem());
                    if(join_iter==null) {
                        join_iter = new Join2IteratorInterface(main_from_item_iter, Data_Storage.oper);
                    }else {
                        join_iter = new Join2IteratorInterface(join_iter,Data_Storage.oper);
                    }
                }
            }
            Expr_Visitor expr = new Expr_Visitor();
            if(plainSelect.getWhere()!=null) {
                plainSelect.getWhere().accept(expr);
                expression = expr.getExpr();
                System.out.println("EXPRESSION : "+expression);
                Data_Storage.oper = new EvalIterator_Interface(join_iter,expression);
                Expression expr_temp = expression;
//                Optimize opt = new Optimize();
//                while(expr_temp != null){
//                    if(expr_temp instanceof AndExpression)
//                    {
//                        AndExpression andExpression = (AndExpression) expr_temp;
//                        opt.evaluate(andExpression.getRightExpression(), "Project");
//                        expr_temp = andExpression.getLeftExpression();
//                    }
//                    else if(expr_temp instanceof OrExpression)
//                    {
//                        OrExpression orExpression = (OrExpression) expr_temp;
//                        opt.evaluate(orExpression.getRightExpression(), "Project");
//                        expr_temp = orExpression.getLeftExpression();
//                    }
//                    else
//                    {
//                        opt.evaluate(expr_temp, "Project");
//                        expr_temp = null;
//                    }

//                }
            }
            else
            {
                Data_Storage.oper = join_iter;
            }
            if(plainSelect.getLimit() != null){
                Data_Storage.limit = plainSelect.getLimit().getRowCount();
            }
            if(plainSelect.getOrderByElements() !=null){
                List<OrderByElement> orderBy = plainSelect.getOrderByElements();
                Iterator orderby_iter = orderBy.iterator();
                while(orderby_iter.hasNext()){
                    OrderByElement o = (OrderByElement) orderby_iter.next();
                    if(o instanceof OrderByElement){
                        Data_Storage.orderBy_sort.add(String.valueOf(o.isAsc()));
                        if(o.getExpression() instanceof Column){
                            Data_Storage.orderBy.add((Column) o.getExpression());
                        }
                    }
                }


            }
            List<SelectItem> sel_items = plainSelect.getSelectItems();
            for(SelectItem col : sel_items)
            {
                SelectItem_Visitor.ret_type(col);
            }
            Data_Storage.oper = new ProjectionIterator_Interface(Data_Storage.oper);
        }
        else if(stmt instanceof Union)
        {
            Union union = (Union) stmt;
            List<PlainSelect> plainSelects = union.getPlainSelects();
        }
    }
    public Iterator_Interface getChild()
    {
        return Data_Storage.oper;
    }
}
