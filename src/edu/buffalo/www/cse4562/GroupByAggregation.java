package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

import javax.xml.crypto.Data;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;

public class GroupByAggregation {
    public static void groupBy(Tuple tup,ArrayList<Column> schema){

        ArrayList<Integer> positions = new ArrayList<>();
        for(int i=0;i<Data_Storage.groupByColumn.size();i++){
            Column col = Data_Storage.groupByColumn.get(i);
            positions.add(schema.indexOf(col));
        }

            String key = "";
            for(int j=0;j<positions.size();j++){
                if(j == positions.size() - 1){
                    key = key + tup.tuples.get(positions.get(j));
                }else{
                    key = key + tup.tuples.get(positions.get(j))+",";
                }
            }

        Iterator iter = Data_Storage.aggregate_operations.iterator();
                ArrayList<Double[]> sample = new ArrayList<>();
                int count = 0;
                while (iter.hasNext()) {
                    String curr_val = "";
                    Function func = (Function) iter.next();
                    //curr_val = evaluate(tup,schema,key,func);
                    if(Data_Storage.aggregateHash.containsKey(key)){
                        if (func.getName().equals("SUM")) {
                            Data_Storage.aggregateHash.get(key).get(count)[0] = Data_Storage.aggregateHash.get(key).get(count)[0] + Double.parseDouble(curr_val);
                            //temp_array[0] = temp_array[0] + Double.parseDouble(curr_val);
                        } else {
                            Data_Storage.aggregateHash.get(key).get(count)[0] = Data_Storage.aggregateHash.get(key).get(count)[0] + Double.parseDouble(curr_val);
                            Data_Storage.aggregateHash.get(key).get(count)[1] = Data_Storage.aggregateHash.get(key).get(count)[1] + Double.parseDouble("1");
                        }
                    }else{
                        if (func.getName().equals("SUM")) {
                            Double[] temp = new Double[1];
                            temp[0] = Double.parseDouble(curr_val);
                            sample.add(temp);
                            //Data_Storage.aggregateHash.get(key).get(count)[0] = Data_Storage.aggregateHash.get(key).get(count)[0] + Double.parseDouble(curr_val);
                            //temp_array[0] = temp_array[0] + Double.parseDouble(curr_val);
                        } else {
                            Double[] temp = new Double[2];
                            temp[0] = Double.parseDouble(curr_val);
                            temp[1] = Double.parseDouble("1");
                            sample.add(temp);
                        }

                    }

                    /*Double[] temp_array = Data_Storage.aggregateHash.get(key).get(count);
                    temp_array[0] = temp_array[0] + Double.parseDouble(curr_val);
                    temp_array[1] = temp_array[1] + 1;*/
                    //Data_Storage.aggregateHash.put(key, temp_array);
                    count++;
                }
                if(sample.size() > 0){
                    Data_Storage.aggregateHash.put(key,sample);
                }
    }

        static Double evaluate(ArrayList<String> tup, ArrayList<Column> schema, String key, Function func){
        String curr_val = "";
            Expression expr = func.getParameters().getExpressions().get(0);
            if (expr instanceof Column) {
                Column col = (Column) expr;
                int pos;
                if(Data_Storage.positionHash.containsKey(col)) {
                    pos = Data_Storage.positionHash.get(col);
                }else{
                    pos = schema.indexOf(col);
                }
                curr_val = tup.get(pos);

            } else {
                Data_Storage.eval = new Eval() {
                    @Override
                    public PrimitiveValue eval(Column column) {
                        String data_type = "";
                        int position;
                        if(Data_Storage.positionHash.containsKey(column)){
                            //data_type = Data_Storage.dataTypeHash.get(column);
                            position = Data_Storage.positionHash.get(column);
                        }else{
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

                            position = schema.indexOf(new Column(new Table(origtableName), col_name));
                            if (position == -1) {
                                position = schema.indexOf(new Column(new Table(origtableName), col_name.split("_")[1]));
                            }
//                            String data_type_table = origtableName;
//                            if (Data_Storage.table_alias.get(origtableName) != null) {
//                                data_type_table = Data_Storage.table_alias.get(origtableName);
//                            }
//
//                            data_type = Data_Storage.tables.get(data_type_table).get(col_name);
//                            if (data_type == null) {
//                                data_type = Data_Storage.tables.get(data_type_table).get(col_name.split("_")[1]);
//                            }
                            Data_Storage.positionHash.put(column,position);
                        }

//                        if (data_type.equals("INTEGER")) {
//                            return new LongValue(tup.get(position));
//                        } else if (data_type.equals("STRING") || data_type.equals("VARCHAR") || data_type.equals("CHAR")) {
//                            return new StringValue(tup.get(position));
//                        } else if (data_type.equals("DOUBLE")) {
//                            return new DoubleValue(tup.get(position));
//                        } else if (data_type.equals("DATE")) {
//                            return new DateValue(tup.get(position));
//                        } else {
//                            return null;
//                        }
                        return new DoubleValue(tup.get(position));
                    }
                };

                try {
                    PrimitiveValue pr = Data_Storage.eval.eval(expr);
                    curr_val = pr.toString();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
        }
        return Double.parseDouble(curr_val);

    }
}
