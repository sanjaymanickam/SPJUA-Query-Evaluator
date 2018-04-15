package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.StringTokenizer;

public class Aggregation{
    static Double SUM = 0.0,AVG =0.0;
    static Integer COUNT = 0;
    static long total = 0;

    public static LinkedHashMap<String, ArrayList<String>> aggregate(ArrayList<ArrayList<String>> result,LinkedHashMap<String,ArrayList<ArrayList<String>>> tuple, ArrayList<Column> schema) {
         LinkedHashMap<String,ArrayList<String>> result_tosend = new LinkedHashMap<>();
        Iterator iter_key = tuple.keySet().iterator();
        while(iter_key.hasNext()) //groups
        {
            String key = iter_key.next().toString();
            Iterator iter_func = Data_Storage.finalColumns.iterator();
            while(iter_func.hasNext())
            {
                String columnName = "";
                Column finalCol = null;
                SelectExpressionItem selitem = (SelectExpressionItem) iter_func.next();
                if(selitem.getExpression() instanceof Column){
                    Column col = (Column) selitem.getExpression();
                    finalCol = col;
                    columnName = col.getColumnName();
                    int position = schema.indexOf(col);
                    if(result_tosend.containsKey(key))
                    {
                        result_tosend.get(key).add(tuple.get(key).get(0).get(position));
                        //result_tosend.get(key).schema.add(new Column(new Table(),func.toString()));
                    }
                    else {
                        ArrayList<String> temp = new ArrayList<>();
                        temp.add(tuple.get(key).get(0).get(position));
                        //ArrayList<Column> col_array = new ArrayList<>();
                        ///col_array.add(new Column(new Table(),func.toString()));
                        result_tosend.put(key, temp);
                    }
                }else if(selitem.getExpression() instanceof Function){
                    if(selitem.getAlias() != null){
                        columnName = selitem.getAlias();
                        finalCol = new Column(new Table(null),selitem.getAlias());
                    }else{
                        finalCol = new Column(new Table(null),"alias");
                    }
                    Function func = (Function) selitem.getExpression();
                    String oper_to_perform = func.getName();
                    Expression condition = func.getParameters().getExpressions().get(0);
                    Iterator iter_tuple = tuple.get(key).iterator();
                    while(iter_tuple.hasNext()) //within each group
                    {
                        ArrayList<String> temp_array =(ArrayList)iter_tuple.next();
                        if (condition instanceof Column) {
                            aggregate_func(Double.parseDouble(temp_array.get(schema.indexOf((Column)condition))), oper_to_perform);
                        }
                        else {
                            Eval eval = new Eval() {
                                @Override
                                public PrimitiveValue eval(Column column) {
                                    String col_name = column.getColumnName();
                                    String tableName = null;
                                    String origtableName = null;
                                    if (Data_Storage.alias_table.containsKey(col_name))
                                        col_name = Data_Storage.alias_table.get(col_name);
                                    if (column.getTable().getName() == null)
                                        tableName = Data_Storage.current_schema.get(col_name);
                                    else
                                        tableName = column.getTable().getName();
                                    if (Data_Storage.table_alias.containsKey(tableName)) {
                                        origtableName = Data_Storage.table_alias.get(tableName);
                                    } else {
                                        origtableName = tableName;
                                    }

                                    int position = schema.indexOf(new Column(new Table(origtableName), col_name));
                                    if(position == -1){
                                        position = schema.indexOf(new Column(new Table(origtableName), col_name.split("_")[1]));
                                    }
                                    String data_type_table = origtableName;
                                   if(Data_Storage.table_alias.get(origtableName) != null){
                                       data_type_table = Data_Storage.table_alias.get(origtableName);
                                   }

                                   String data_type = Data_Storage.tables.get(data_type_table).get(col_name);
                                   if(data_type == null){
                                       data_type = Data_Storage.tables.get(data_type_table).get(col_name.split("_")[1]);
                                   }
                                    if (data_type.equals("INTEGER")) {
                                        return new LongValue(temp_array.get(position));
                                    } else if (data_type.equals("STRING") || data_type.equals("VARCHAR") | data_type.equals("CHAR")) {
                                        return new StringValue(temp_array.get(position));
                                    } else if (data_type.equals("DOUBLE")) {
                                        return new DoubleValue(temp_array.get(position));
                                    } else if (data_type.equals("DATE")) {
                                        return new DateValue(temp_array.get(position));
                                    } else {
                                        return null;
                                    }
                                }
                            };
                            try {
                                //test.clear();
                                long starttime = System.nanoTime();
                                PrimitiveValue pr = eval.eval(condition);
                                long endtime = System.nanoTime();
                                total = total+(endtime-starttime);
                                if (pr == BooleanValue.FALSE) {
                                    tuple = null;
                                } else {
                                    if (pr != BooleanValue.TRUE && pr != null) {
                                        aggregate_func(Double.parseDouble(pr.toString()), oper_to_perform);
                                    }
                                }
                            } catch (SQLException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                    String agg_val = "";
                    if(oper_to_perform.equals("SUM")){
                        agg_val = SUM.toString();
                    }else if(oper_to_perform.equals("AVG")){
                        Double avg = SUM/COUNT;
                        agg_val = avg.toString();
                    }
                    //group_tuple.add(SUM.toString());


                    if(result_tosend.containsKey(key))
                    {
                        result_tosend.get(key).add(agg_val);
                        //result_tosend.get(key).schema.add(new Column(new Table(),func.toString()));
                    }
                    else
                    {
                        ArrayList<String> temp = new ArrayList<>();
                        temp.add(agg_val);
                        //ArrayList<Column> col_array = new ArrayList<>();
                        ///col_array.add(new Column(new Table(),func.toString()));
                        result_tosend.put(key,temp);
                    }
                    SUM = 0.0;
                    COUNT = 0;

                }
                if(!Data_Storage.finalSchema.contains(finalCol)){
                    Data_Storage.finalSchema.add(finalCol);
                }

            }
        }
        System.err.println("Eval TIME : "+total);
        return result_tosend;
    }
    static void aggregate_func(Double primitiveValue, String oper)
    {
        switch (oper)
        {
            case "SUM":
                    SUM = SUM + primitiveValue;
                    break;
            case "AVG":
                    SUM = SUM + primitiveValue;
                    COUNT = COUNT + 1;
                    break;
        }
    }
}
