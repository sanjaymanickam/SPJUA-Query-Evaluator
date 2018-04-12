package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.*;

import javax.xml.crypto.Data;
import java.util.*;

public class Select_Visitor {
    static SelectBody selectBody;
    public static void ret_type(SelectBody stmt)
    {
        selectBody = stmt;
        Expression expression;

        if(stmt instanceof PlainSelect)
        {
            Data_Storage.limit = Long.parseLong("0");
            Data_Storage.orderBy_sort = new ArrayList<>();
            Data_Storage.orderBy = new ArrayList<>();
            PlainSelect plainSelect = (PlainSelect) stmt;
            From_Visitor.ret_type(plainSelect.getFromItem());
            List<Join> joins = plainSelect.getJoins();
            Iterator_Interface main_from_item_iter = Data_Storage.oper;
            Iterator_Interface join_iter = null;
            if(joins!=null)
            {
                Data_Storage.join = 1;
                Iterator it = joins.iterator();
                while(it.hasNext()) {
                    Join join = (Join) it.next();
                    From_Visitor.ret_type(join.getRightItem());
                    if(join_iter==null) {
                        join_iter = new JoinIteratorInterface(main_from_item_iter, Data_Storage.oper);
                    }else {
                        join_iter = new JoinIteratorInterface(join_iter,Data_Storage.oper);
                    }
                }
                Data_Storage.oper = join_iter;
            }
            else
            {
                join_iter = Data_Storage.oper;
            }
            Expr_Visitor expr = new Expr_Visitor();
            if(plainSelect.getWhere()!=null) {
                plainSelect.getWhere().accept(expr);
                expression = expr.getExpr();
                add_to_project_array(expression);
                Data_Storage.oper = new EvalIterator_Interface(join_iter,expression);
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
                            Column col = (Column) o.getExpression();
                            Data_Storage.orderBy.add(col);
                            Data_Storage.project_array.add(col.getColumnName());
                        }
                    }
                }
             if(plainSelect.getGroupByColumnReferences() != null){
                    Data_Storage.groupbyflag = 1;
                Data_Storage.groupByColumn = plainSelect.getGroupByColumnReferences();
             }
            }
            List<SelectItem> sel_items = plainSelect.getSelectItems();
            Data_Storage.selectedColumns.clear();
            for(SelectItem col : sel_items)
            {
                SelectItem_Visitor.ret_type(col);
            }
            Data_Storage.oper = new ProjectionIterator_Interface(Data_Storage.projectionColumns,Data_Storage.oper);
        }
        else if(stmt instanceof Union)
        {
            Union union = (Union) stmt;
            List<PlainSelect> plainSelects = union.getPlainSelects();
        }
    }
    static void add_to_project_array(Expression agg_expr)
    {
        if(agg_expr instanceof Column){
            Column col = (Column) agg_expr;
            if(!Data_Storage.project_array.contains(col)){
                Data_Storage.project_array.add(col.getColumnName());
            }
            return;
        }
        if(agg_expr instanceof BinaryExpression){
            add_to_project_array(((BinaryExpression) agg_expr).getLeftExpression());
            add_to_project_array(((BinaryExpression) agg_expr).getRightExpression());
        }
    }
    public Iterator_Interface getChild()
    {
        return Data_Storage.oper;
    }
}
