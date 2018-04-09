package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.schema.Column;

import java.util.ArrayList;
import java.util.LinkedHashMap;

public class GroupByAggregate {
        static int position1 = -1;
        static int position2 = -1;

        public static LinkedHashMap<String, ArrayList<String>> groupBy(ArrayList<ArrayList<String>> tuple, ArrayList<Column> schema){
            Tuple tup = new Tuple();
            Column col1 = Data_Storage.groupByColumn.get(0);
            Column col2 = null;
            if(Data_Storage.groupByColumn.size() > 1){
                col2 = Data_Storage.groupByColumn.get(1);
            }
            LinkedHashMap<String,ArrayList<String>> resultSet = new LinkedHashMap<>();
            position1 = schema.indexOf(col1);
            if(col2 != null){
                position2 = schema.indexOf(col2);
            }
            for(int i=0;i<tuple.size();i++){
                String key = (col2!=null)? (tuple.get(i).get(position1) + "," +tuple.get(i).get(position2)) : tuple.get(i).get(position1);
                if(resultSet.containsKey(key)){
                    resultSet.get(key).addAll(tuple.get(i));
                }else{
                    resultSet.put(key,tuple.get(i));
                }
            }
            return resultSet;
        }
}
