package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.*;

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
            PlainSelect plainSelect = (PlainSelect) stmt;
            From_Visitor.ret_type(plainSelect.getFromItem());
            List<Join> joins = plainSelect.getJoins();
            if(joins!=null)
            {
                Iterator it = joins.iterator();
                while(it.hasNext())
                     Join_Visitor.ret_type((Join) it.next(),Data_Storage.oper);
            }
            Expr_Visitor expr = new Expr_Visitor();
            if(plainSelect.getWhere()!=null) {
                plainSelect.getWhere().accept(expr);
                expression = expr.getExpr();
                System.out.println("EXPRESSION : "+expression);
                Data_Storage.oper = new EvalIterator_Interface(Data_Storage.oper,expression);
            }
            else
            {
                expression = null;
            }
            List<SelectItem> sel_items = plainSelect.getSelectItems();
            for(SelectItem col : sel_items)
            {
                SelectItem_Visitor.ret_type(col);
            }
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
