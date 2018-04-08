package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.*;

import java.util.*;

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
                Data_Storage.join = 1;
                Iterator it = joins.iterator();
                while(it.hasNext()) {
                    Join join = (Join) it.next();
                    From_Visitor.ret_type(join.getRightItem());
                    if(join_iter==null) {
                        join_iter = new Join2IteratorInterface(main_from_item_iter, Data_Storage.oper);
                    }else {
                        join_iter = new Join2IteratorInterface(join_iter,Data_Storage.oper);
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
                            Data_Storage.orderBy.add((Column) o.getExpression());
                        }
                    }
                }


            }
            List<SelectItem> sel_items = plainSelect.getSelectItems();
            Data_Storage.selectedColumns.clear();
            for(SelectItem col : sel_items)
            {

                SelectItem_Visitor.ret_type(col);
            }
            LinkedHashMap<String,String> new_hashmap = new LinkedHashMap<>(Data_Storage.selectedColumns);
            Data_Storage.oper = new ProjectionIterator_Interface(new_hashmap,Data_Storage.oper);
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
