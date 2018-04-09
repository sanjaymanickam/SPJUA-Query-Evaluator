package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

public class Aggregation {
    public static LinkedHashMap<String, ArrayList<String>> aggregate(ArrayList<ArrayList<String>> tuple, ArrayList<Column> schema) {
        Iterator it = Data_Storage.aggregate_operations.iterator();
        while(it.hasNext())
        {
            Function func = (Function) it.next();
            String func_name = func.getName();
            List<Expression> list_expr =  func.getParameters().getExpressions();

        }
        return null;
    }
}
