package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;

import java.io.InputStreamReader;
import java.io.Reader;
import java.util.*;

public class Command_Executor {
    static String prompt = "$> ";
    public static void exec(String[] args)
    {
        System.out.println(prompt);
        System.out.flush();
        Reader in = new InputStreamReader(System.in);
        CCJSqlParser parser = new CCJSqlParser(in);
        Statement stmt;
        try {
            while ((stmt = parser.Statement()) != null) {
                ArrayList<ArrayList<String>> result = new ArrayList<>();
                ArrayList<Column> schema = new ArrayList<>();
                Data_Storage.selectedColumns.clear();
                Data_Storage.project_columns.clear();
                Data_Storage.operator_map.clear();
                Data_Storage.current_schema.clear();
                Data_Storage.oper = null;
                Data_Storage.limit = new Long("0");
                Data_Storage.orderBy = null;
//                System.out.println(stmt);
                Visitor_Parse.ret_type(stmt);
                if(Data_Storage.oper!=null) {
                    Iterator_Interface iter = new Optimize_3().optimize(Data_Storage.oper);
                    Data_Storage.oper = iter;
                    Tuple tuple = Data_Storage.oper.readOneTuple();
                    do {
                        Iterator it = tuple.tuples.iterator();
                        result.add(tuple.tuples);
                        schema = tuple.schema;
                        tuple = Data_Storage.oper.readOneTuple();

                    } while (tuple != null);
                    sort(result,schema);
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
    public void project(Tuple tuple){
        Iterator tuple_itr = tuple.tuples.iterator();
        while(tuple_itr.hasNext()){

        }
    }
    public static void sort(ArrayList<ArrayList<String>> result, ArrayList<Column> schema){
        for(int i=0;i<Data_Storage.orderBy.size();i++){
            Column c = Data_Storage.orderBy.get(i);
            String tableName =  c.getTable().getName();
            String col_name = c.getColumnName();
            if(Data_Storage.table_alias.containsKey(tableName))
                tableName =Data_Storage.table_alias.get(tableName);
            Column col = new Column(new Table(tableName),col_name);
            int position = schema.indexOf(col);
            if("true".equals(Data_Storage.orderBy_sort.get(i))){
                Collections.sort(result, new Comparator<ArrayList<String>>() {
                    @Override
                    public int compare(ArrayList<String> one, ArrayList<String> two) {
                        return one.get(position).compareTo(two.get(position));
                    }
                });
            }else{
                Collections.sort(result, new Comparator<ArrayList<String>>() {
                    @Override
                    public int compare(ArrayList<String> one, ArrayList<String> two) {
                        return two.get(position).compareTo(one.get(position));
                    }
                });
            }


        }
        int temp_i=0;
        if(Data_Storage.limit > 0) {
            int i = 0;
            while (i < Data_Storage.limit) {
                Iterator<String> itr = result.get(i).iterator();
                while (itr.hasNext()) {
                    Column col = schema.get(temp_i++);
                    String tableName =  col.getTable().getName();
                    String col_name = col.getColumnName();
                    if(Data_Storage.table_alias.containsKey(tableName))
                        tableName =Data_Storage.table_alias.get(tableName);
                    Column new_col = new Column(new Table(tableName),col_name);
                    String temp = Data_Storage.tables.get(new_col.getTable().getName()).get(new_col.getColumnName());
                    if(temp.equals("DOUBLE"))
                    {
                        System.out.println(new DoubleValue(itr.next()));
                    }
                    else {
                        System.out.print(itr.next());
                    }
//                    String temp = itr.next();
//                    System.err.println(temp);
//                    System.out.print(new DoubleValue(temp));
                    if (itr.hasNext()) {
                        System.out.print("|");
                    }
                }
                i++;
                System.out.println();
            }
        }else{
            for(int i = 0;i< result.size();i++){
                Iterator<String> itr = result.get(i).iterator();
                while (itr.hasNext()) {
                    System.out.print(itr.next().toString());
                    if (itr.hasNext()) {
                        System.out.print("|");
                    }
                }
                System.out.println();
            }

        }
    }
}