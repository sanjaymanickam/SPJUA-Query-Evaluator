package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.schema.Column;
import java.util.ArrayList;
import java.util.LinkedHashMap;

public class GroupByAggregate {
    static Integer COUNT = 0;
    static Double SUM = 0.0;

    public static LinkedHashMap<String, ArrayList<ArrayList<String>>> groupBy(ArrayList<ArrayList<String>> tuple, ArrayList<Column> schema){
        Tuple tup = new Tuple();
        ArrayList<Integer> positions = new ArrayList<>();
        LinkedHashMap<String,ArrayList<ArrayList<String>>> resultSet = new LinkedHashMap<>();
        if(Data_Storage.groupbyflag == 0){
            resultSet.put("1",tuple);
            return resultSet;
        }
        for(int i=0;i<Data_Storage.groupByColumn.size();i++){
            Column col = Data_Storage.groupByColumn.get(i);
            positions.add(schema.indexOf(col));
        }

        for(int i=0;i<tuple.size();i++){
            String key = "";
            for(int j=0;j<positions.size();j++){
                if(j == positions.size() - 1){
                    key = key + tuple.get(i).get(positions.get(j));
                }else{
                    key = key + tuple.get(i).get(positions.get(j))+",";
                }
            }



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
