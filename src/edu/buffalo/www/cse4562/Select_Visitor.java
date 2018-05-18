package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
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
            Boolean aggregateOperations = false;
            Data_Storage.groupbyflag = 0;
            Data_Storage.aggregateflag = 0;
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
                    if(((FileIterator_Interface) Data_Storage.oper).new_file.equals(((FileIterator_Interface) main_from_item_iter).new_file)){
                        Data_Storage.selfJoin = 1;
                    }
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
            if(plainSelect.getGroupByColumnReferences() != null){
                    Data_Storage.groupbyflag = 1;
                    Data_Storage.groupByColumn = plainSelect.getGroupByColumnReferences();
                    aggregateOperations = true;
            }
            ArrayList<SelectExpressionItem> sel_items = (ArrayList) plainSelect.getSelectItems();
            Data_Storage.selectedColumns.clear();
            Data_Storage.finalColumns.clear();
            Data_Storage.projectionColumns.clear();

            if(!aggregateOperations){
                for(SelectItem s : sel_items){
                    if(s instanceof SelectExpressionItem){
                        SelectExpressionItem sExp = (SelectExpressionItem)s;
                        if(sExp.getExpression() instanceof Function){
                            aggregateOperations = true;
                            break;
                        }
                    }

                }
            }
            if(aggregateOperations){
                Data_Storage.oper =  new AggregateProjection(Data_Storage.oper,sel_items);
                //Handle groupBy, Aggregation and Projection together
            }else{
                Data_Storage.oper = new ProjectionIterator_Interface(sel_items,Data_Storage.oper);
            }

            handleSelectionItems(sel_items);
            for(SelectItem col : sel_items)
            {
                SelectItem_Visitor.ret_type(col);
            }
            if(plainSelect.getOrderByElements() != null){
                Data_Storage.oper = new Sort(Data_Storage.oper, plainSelect.getOrderByElements());
            }

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
            Data_Storage.projectionCols.add(col.getColumnName());
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

    public static void handleSelectionItems(ArrayList<SelectExpressionItem> selItems){
        for(SelectItem selItem : selItems){
            if(selItem instanceof AllColumns){

            }
            else if(selItem instanceof AllTableColumns){
                AllTableColumns at = (AllTableColumns) selItem;
                String tableName = at.getTable().getName();
                if(Data_Storage.table_alias.containsKey(tableName)){
                    tableName = Data_Storage.table_alias.get(tableName);
                }
                Iterator colIter = Data_Storage.tables.get(tableName).keySet().iterator();
                while (colIter.hasNext()){
                    Data_Storage.projectionCols.add(colIter.next().toString());
                }
            }
            else if(selItem instanceof SelectExpressionItem){
                SelectExpressionItem selExper = (SelectExpressionItem) selItem;
                Expression expr = (Expression) selExper.getExpression();
                if(expr instanceof Column){
                    Column col = (Column)expr;
                    Data_Storage.projectionCols.add(col.getColumnName());
                }
                else if(expr instanceof Function){
                    Function func = (Function) expr;
                    Expression aggExpr = (Expression) func.getParameters().getExpressions().get(0);
                    handleExpression(aggExpr);
                }
            }
        }
    }
    public static void handleExpression(Expression agg_expr){
        if(agg_expr instanceof Column){
            Column col = (Column) agg_expr;
            Data_Storage.projectionCols.add(col.getColumnName());
            return;
        }
        if(agg_expr instanceof BinaryExpression){
            handleExpression(((BinaryExpression) agg_expr).getLeftExpression());
            handleExpression(((BinaryExpression) agg_expr).getRightExpression());
        }

    }
}
