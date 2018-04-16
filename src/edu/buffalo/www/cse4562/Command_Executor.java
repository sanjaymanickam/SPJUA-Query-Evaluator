package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;

import javax.xml.crypto.Data;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.*;
import java.util.concurrent.atomic.DoubleAccumulator;

public class Command_Executor {
    static String prompt = "$> ";
    static LinkedHashMap<String,ArrayList<String>> aggregate_result = new LinkedHashMap<>();
    public static void exec(String[] args)
    {
        System.out.println(prompt);
        System.out.flush();
        Reader in = new InputStreamReader(System.in);
        CCJSqlParser parser = new CCJSqlParser(in);
        int count = 0;
        Statement stmt;
        try {
            while ((stmt = parser.Statement()) != null) {
                ArrayList<ArrayList<String>> result = new ArrayList<>();
                ArrayList<Column> schema = new ArrayList<>();
                Data_Storage.selectedColumns.clear();
                Data_Storage.operator_map.clear();
                Data_Storage.current_schema.clear();
                Data_Storage.oper = null;
                Data_Storage.limit = Long.parseLong("0");
                Data_Storage.orderBy = null;
                Data_Storage.groupbyflag = 0;
                Data_Storage.aggregateflag = 0;
                Data_Storage.finalColumns.clear();
                Data_Storage.hash_flag = 0;
                Data_Storage.projectionColumns.clear();
                Data_Storage.finalSchema.clear();
                Data_Storage.aggregate_operations.clear();
                int schema_flag=0;
                Visitor_Parse.ret_type(stmt);
                if(Data_Storage.oper!=null) {
                    if(Data_Storage.join ==1) {
                        Iterator_Interface iter = new Optimize().optimize(Data_Storage.oper);
                        Data_Storage.oper = iter;
                    }
                    Set<String> temp_set = new HashSet<>();
                    temp_set.addAll(Data_Storage.project_array);
                    Data_Storage.project_array.clear();
                    Data_Storage.project_array.addAll(temp_set);
                    long starttime = System.nanoTime();
                    Tuple tuple = Data_Storage.oper.readOneTuple();
                    while (tuple != null){

                        Tuple temp = new Tuple();
                        temp.tuples.addAll(tuple.tuples);
                        temp.schema.addAll(tuple.schema);
                        result.add(temp.tuples);
                        if(schema_flag==0) {
                            schema = temp.schema;
                            schema_flag=1;
                        }
                       // GroupByAggregation.groupBy(tuple,schema);
                        tuple = Data_Storage.oper.readOneTuple();
                    }
                    long endtime = System.nanoTime();
                    //System.err.println("Projection : Time : "+(endtime-starttime));
                    Column col = new Column();
                    starttime = System.nanoTime();
                    Data_Storage.groupby_resultset = GroupByAggregate.groupBy(result, schema);
                    endtime = System.nanoTime();
                    //System.err.println("Groupby Over : Time : "+(endtime-starttime));
                    starttime = System.nanoTime();
                    if(Data_Storage.aggregateflag==1)
                    {
                       aggregate_result = Aggregation.aggregate(result,Data_Storage.groupby_resultset,schema);
                    }
                    endtime= System.nanoTime();
                    //System.err.println("Aggregate Over : Time : "+((endtime-starttime)));
                    sort(new ArrayList<>(aggregate_result.values()),Data_Storage.finalSchema);

//                    group_by_print(aggregate_result,result,schema);
                }

                System.out.println(prompt);
                System.out.flush();
            }
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
    }
    public static void sort(ArrayList<ArrayList<String>> result, ArrayList<Column> schema){

            if(Data_Storage.orderBy.size() > 0){
                int ct=0;
                Column c = Data_Storage.orderBy.get(ct);
                String tableName =  c.getTable().getName();
                String col_name = c.getColumnName();

                if(Data_Storage.alias_table.containsKey(c.toString())){
                    col_name = Data_Storage.alias_table.get(c.toString());
                }
                if(Data_Storage.table_alias.containsKey(tableName))
                {
                    tableName =Data_Storage.table_alias.get(tableName);
                }else{
                    tableName = Data_Storage.current_schema.get(col_name);
                }

                Column col = new Column(new Table(tableName),col_name);
                int position = schema.indexOf(col);
                String DataType;
                if(tableName != null){
                    DataType = Data_Storage.tables.get(tableName).get(col_name);
                }else{
                    DataType = "DOUBLE";
                }

                if("true".equals(Data_Storage.orderBy_sort.get(ct))){
                    Collections.sort(result, new Comparator<ArrayList<String>>() {
                        @Override
                        public int compare(ArrayList<String> one, ArrayList<String> two) {
                            if(DataType.equals("DOUBLE")){
                                Double value1 = Double.parseDouble(one.get(position));
                                Double value2 = Double.parseDouble(two.get(position));
                                if(value1 == value2){

                                }else if(value1 < value2){
                                    return -1;
                                }else{
                                    return 1;
                                }
                            }
                            return one.get(position).compareTo(two.get(position));
                        }
                    });
                }else{
                    Collections.sort(result, new Comparator<ArrayList<String>>() {
                        @Override
                        public int compare(ArrayList<String> one, ArrayList<String> two) {
                            if(DataType.equals("DOUBLE")){
                                Double value1 = Double.parseDouble(two.get(position));
                                Double value2 = new Double(one.get(position));
                                if(value1 < value2){
                                    return -1;
                                }else{
                                    return 1;
                                }
                            }
                            return two.get(position).compareTo(one.get(position));
                        }
                    });
                }
            }




        int temp_i=0;
        int size_to_iter =  result.size();
        if(Data_Storage.limit > 0 && result.size() > Data_Storage.limit) {
            size_to_iter = Data_Storage.limit.intValue();
        }
            for(int i = 0;i<size_to_iter;i++){
                Iterator itr = result.get(i).iterator();
                while (itr.hasNext()) {
                    Column col = schema.get(temp_i++);
                    String tableName =  col.getTable().getName();
                    String col_name = col.getColumnName();
                    if(Data_Storage.table_alias.containsKey(tableName))
                        tableName =Data_Storage.table_alias.get(tableName);
                    if(Data_Storage.current_schema.containsKey(col_name)){
                        tableName = Data_Storage.current_schema.get(col_name);
                    }
                    Column new_col = new Column(new Table(tableName),col_name);
                    String temp = "DOUBLE";
                    if(tableName != null){
                        temp = Data_Storage.tables.get(new_col.getTable().getName()).get(new_col.getColumnName());
                    }
                    if(temp.equals("DOUBLE"))
                    {
                        DoubleValue d_value = new DoubleValue(itr.next().toString());
                        System.out.print(d_value);
                    }
                    else if(temp.equals("STRING") || temp.equals("VARCHAR") || temp.equals("CHARACTER"))
                    {
                        System.out.print(new StringValue(itr.next().toString()));
                    }
                    else {
                        System.out.print(itr.next());
                    }
                    if (itr.hasNext()) {
                        System.out.print("|");
                    }
                }
                temp_i=0;
                System.out.println();
        }
    }
}