package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.schema.Column;

import java.util.ArrayList;
import java.util.LinkedHashMap;

public class GroupByAggregate {
        static int position1 = -1;
        static int position2 = -1;
        static int position3 = -1;

        public static LinkedHashMap<String, ArrayList<ArrayList<String>>> groupBy(ArrayList<ArrayList<String>> tuple, ArrayList<Column> schema){
            Tuple tup = new Tuple();
            Column col1 = Data_Storage.groupByColumn.get(0);
            Column col2 = null;
            Column col3 = null;
            if(Data_Storage.groupByColumn.size() > 1){
                col2 = Data_Storage.groupByColumn.get(1);
            }
            if(Data_Storage.groupByColumn.size() > 2){
                col3 = Data_Storage.groupByColumn.get(2);
            }
            LinkedHashMap<String,ArrayList<ArrayList<String>>> resultSet = new LinkedHashMap<>();
            position1 = schema.indexOf(col1);
            if(col2 != null){
                position2 = schema.indexOf(col2);
            }
            if(col3 != null){
                position3 = schema.indexOf(col3);
            }
            for(int i=0;i<tuple.size();i++){
                String key = (col3 != null)?(tuple.get(i).get(position1) + "," +tuple.get(i).get(position2)+"," +tuple.get(i).get(position3)):(col2!=null)? (tuple.get(i).get(position1) + "," +tuple.get(i).get(position2)) : tuple.get(i).get(position1);
                if(resultSet.containsKey(key)){
                    resultSet.get(key).add(tuple.get(i));
                }else{
                     ArrayList<ArrayList<String>> temp = new ArrayList<>();
                     temp.add(tuple.get(i));
                    resultSet.put(key,temp);
                }
            }
            return resultSet;
        }
}
