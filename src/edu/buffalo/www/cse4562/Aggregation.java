package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.StringTokenizer;

public class Aggregation {
    static Double SUM = 0.0,AVG =0.0;
    static LinkedHashMap<String,Tuple> result_tosend = new LinkedHashMap<>();
    public static LinkedHashMap<String, Tuple> aggregate(ArrayList<ArrayList<String>> result,LinkedHashMap<String,ArrayList<ArrayList<String>>> tuple, ArrayList<Column> schema) {
        Iterator iter_func = Data_Storage.aggregate_operations.iterator();
        while(iter_func.hasNext())
        {
            Function func = (Function) iter_func.next();
            String oper_to_perform = func.getName();
            Expression condition = func.getParameters().getExpressions().get(0);
            Iterator iter_key = tuple.keySet().iterator();
            while(iter_key.hasNext())
            {
                String key = iter_key.next().toString();
                Iterator iter_tuple = tuple.get(key).iterator();
                while(iter_tuple.hasNext())
                {
                    ArrayList<String> temp_array =(ArrayList)iter_tuple.next();
                    Tuple tup = new EvalIterator_Interface(new Iterator_Interface() {
                        @Override
                        public Tuple readOneTuple() {
                            return new Tuple(temp_array,schema);
                        }

                        @Override
                        public Iterator_Interface getChild() {
                            return null;
                        }

                        @Override
                        public void setChild(Iterator_Interface iter) {

                        }

                        @Override
                        public void reset() {

                        }
                    }, condition).readOneTuple();
                    aggregate_func(Double.parseDouble(tup.tuples.get(tup.tuples.size()-1)),oper_to_perform);
                }
                System.out.println(SUM);
                if(result_tosend.containsKey(key))
                {
                    result_tosend.get(key).tuples.add(SUM.toString());
                    result_tosend.get(key).schema.add(new Column(new Table(),func.toString()));
                }
                else
                {
                    ArrayList<String> temp = new ArrayList<>();
                    temp.add(SUM.toString());
                    ArrayList<Column> col_array = new ArrayList<>();
                    col_array.add(new Column(new Table(),func.toString()));
                    result_tosend.put(key,new Tuple(temp,col_array));
                }
                SUM = 0.0;
            }
        }
        ArrayList<Tuple> to_send = new ArrayList<>();
        Iterator selected_columns = Data_Storage.selectedColumns.iterator();
        int i = 0;
        while(i<result.size()) {
            Tuple to_add = new Tuple();
            to_add.tuples = new ArrayList<>();
            to_add.schema = new ArrayList<>();
            for(int t=0;t<Data_Storage.selectedColumns.size();i++)
            {
                int pos;
                String next_col_name = selected_columns.next().toString();
                Column to_check = new Column();
                if(next_col_name.indexOf(".")!=-1) {
                    StringTokenizer strtok = new StringTokenizer(next_col_name, ".");
                    String table_name = strtok.nextElement().toString();
                    String col_name = strtok.nextElement().toString();
                    to_check = new Column(new Table(table_name), col_name);
                }
                if (schema.contains(to_check)) {
                    to_add.tuples.add(result.get(i).get(schema.indexOf(to_check)));
                    to_add.schema.add(schema.get(schema.indexOf(to_check)));
                } else {
                    if(result_tosend.get(i).schema.contains(to_check))
                    {

                    }
                }
            }
            i++;
        }
        return result_tosend;
    }
    static void aggregate_func(Double primitiveValue, String oper)
    {
        switch (oper)
        {
            case "SUM":
                    SUM = SUM + primitiveValue;
        }
    }
}
