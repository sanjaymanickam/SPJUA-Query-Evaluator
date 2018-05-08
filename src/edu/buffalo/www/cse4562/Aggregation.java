package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

import java.sql.SQLException;
import java.util.*;

public class Aggregation {

    public static LinkedHashMap<String, ArrayList<String>> aggregate(ArrayList<ArrayList<String>> result, LinkedHashMap<String, ArrayList<ArrayList<String>>> tuple, ArrayList<Column> schema) {
        LinkedHashMap<String, ArrayList<String>> result_tosend = new LinkedHashMap<>();
        Iterator iter_key = tuple.keySet().iterator();
        buildSchema();
        while (iter_key.hasNext()) //groups
        {
            LinkedHashMap<Function, Double[]> groupHash = new LinkedHashMap<>();
            String key = iter_key.next().toString();
            Iterator iter_tuple = tuple.get(key).iterator();
            while (iter_tuple.hasNext()) {
                ArrayList<String> temp_array = (ArrayList) iter_tuple.next();
                Iterator iter_func = Data_Storage.aggregate_operations.iterator();
                while (iter_func.hasNext()) {
                    Column finalCol = null;
                    //SelectExpressionItem selitem = (SelectExpressionItem) iter_func.next();
                    Function func = (Function) iter_func.next();
                    String func_name = func.getName();
                    //Expression condition = func.getParameters().getExpressions().get(0);
                    Double curr_val= GroupByAggregation.evaluate(temp_array, schema, "as", func);
                    if(groupHash.containsKey(func)){
                        if(func_name.equals("SUM")){
                            groupHash.get(func)[0] = groupHash.get(func)[0] + curr_val;
                        }else{
                            groupHash.get(func)[0] = groupHash.get(func)[0] + curr_val;
                            groupHash.get(func)[1] = groupHash.get(func)[1] + 1;
                        }

                    }else{
                        if(func_name.equals("SUM")){
                            Double[] temp = new Double[1];
                            temp[0] = curr_val;
                            groupHash.put(func,temp);
                        }else{
                            Double[] temp = new Double[2];
                            temp[0] = curr_val;
                            temp[1] = 1.0;
                            groupHash.put(func,temp);
                        }

                    }
                }
            }
            Iterator project_iter = Data_Storage.finalColumns.iterator();
            ArrayList<String> to_send = new ArrayList<>();
            while(project_iter.hasNext()){
                Column col = null;
                SelectExpressionItem selItem = (SelectExpressionItem) project_iter.next();
                if(selItem.getExpression() instanceof Column){
                    int pos = schema.indexOf((Column)selItem.getExpression());
                    to_send.add(tuple.get(key).get(0).get(pos));

                }else{
                    Function func = (Function) selItem.getExpression();
                    if(func.getName().equals("SUM")){
                        to_send.add(groupHash.get(func)[0].toString());
                    }else{
                        Double val = ((groupHash.get(func)[0]) / (groupHash.get(func)[1].intValue()));
                        to_send.add(val.toString());
                    }
                }
            }
            result_tosend.put(key,to_send);
        }
        return result_tosend;
    }
    public static void buildSchema(){
        Iterator itr = Data_Storage.finalColumns.iterator();
        while(itr.hasNext()){
            Column col = null;
            SelectExpressionItem selItem = (SelectExpressionItem) itr.next();
            if(selItem.getExpression() instanceof Column){
                col = (Column) selItem.getExpression();
            }
            else{
                if(selItem.getAlias() != null){
                    col = new Column(new Table(),selItem.getAlias());
                }else{
                    col = new Column(new Table(),"alias");
                }
            }
            Data_Storage.finalSchema.add(col);

        }
    }
}