package edu.buffalo.www.cse4562;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import java.util.*;

public class GroupByAggregate {
        static int position1 = -1;
        static int position2 = -1;
        static int position3 = -1;

        public static LinkedHashMap<String, ArrayList<String>> groupBy(ArrayList<ArrayList<String>> tuple, ArrayList<Column> schema) {
            Tuple tup = new Tuple();
            ArrayList<Integer> positions = new ArrayList<>();
            LinkedHashMap<String, ArrayList<ArrayList<String>>> resultSet = new LinkedHashMap<>();
            LinkedHashMap<String,HashMap<Double,Integer>> aggregateset = new LinkedHashMap<>();
            LinkedHashMap<String, ArrayList<String>> finalresult = new LinkedHashMap<>();

            for (int i = 0; i < Data_Storage.groupByColumn.size(); i++) {
                Column col = Data_Storage.groupByColumn.get(i);
                positions.add(schema.indexOf(col));
            }

            for (int i = 0; i < tuple.size(); i++) {
                String key = "";
                if (Data_Storage.groupbyflag != 0) {
                        for (int j = 0; j < positions.size(); j++) {
                            if (j == positions.size() - 1) {
                                key = key + tuple.get(i).get(positions.get(j));
                            } else {
                                key = key + tuple.get(i).get(positions.get(j)) + ",";
                            }
                        }
                }
                else
                {
                    key = "1";
                }
                Tuple tup_to_send = null;
                Iterator iter = Data_Storage.aggregate_operations.iterator();
                while (iter.hasNext()) {
                    Function function = (Function) iter.next();
                    String func_name = function.getName();
                    Expression expr = function.getParameters().getExpressions().get(0);
                    if (expr instanceof Column) {
                        tup_to_send.tuples.add(tuple.get(i).get(schema.indexOf(expr)));
                        tup_to_send.schema.add(schema.get(schema.indexOf(expr)));
                    } else {
                        final int pos = i;
                        tup_to_send = new EvalIterator_Interface(new Iterator_Interface() {
                            @Override
                            public Tuple readOneTuple() {
                                return new Tuple(tuple.get(pos), schema);
                            }

                            @Override
                            public Iterator_Interface getChild() {
                                return null;
                            }

                            @Override
                            public void print() {

                            }

                            @Override
                            public void setChild(Iterator_Interface iter) {

                            }

                            @Override
                            public void reset() {

                            }
                        }, expr).readOneTuple();
                    }
                }
                if (!aggregateset.containsKey(key)) {
                    HashMap<Double,Integer> hmap = new HashMap<>();
                    hmap.put(Double.parseDouble(tup_to_send.tuples.get(tup_to_send.tuples.size() - 1)), 1);
                    aggregateset.put(key,hmap);
                }else {
                    Double dub = new ArrayList<>(aggregateset.get(key).keySet()).get(0);
                    Integer int1 = aggregateset.get(key).get(dub)+1;
                    aggregateset.get(key).remove(dub);
                    dub = dub+Double.parseDouble(tup_to_send.tuples.get(tup_to_send.tuples.size()-1));
                    aggregateset.get(key).put(dub,int1);
                }
                if (resultSet.containsKey(key)) {
                    resultSet.get(key).add(tuple.get(i));
                } else {
                    ArrayList<ArrayList<String>> temp = new ArrayList<>();
                    temp.add(tuple.get(i));
                    resultSet.put(key, temp);

                }
            }

            Iterator result_set_iter = resultSet.keySet().iterator();
            while(result_set_iter.hasNext()) {
                String key = result_set_iter.next().toString();
                Iterator final_columns_iter = Data_Storage.finalColumns.iterator();
                while (final_columns_iter.hasNext()) {
                    SelectExpressionItem selectExpressionItem = (SelectExpressionItem) final_columns_iter.next();
                    Column finalCol = null;
                    if (selectExpressionItem.getExpression() instanceof Column) {
                        Column col = (Column) selectExpressionItem.getExpression();
                        finalCol = col;
                        int pos = schema.indexOf(col);
                        if(finalresult.containsKey(key))
                        {
                            finalresult.get(key).add(resultSet.get(key).get(0).get(pos));
                            //result_tosend.get(key).schema.add(new Column(new Table(),func.toString()));
                        }
                        else {
                            ArrayList<String> temp = new ArrayList<>();
                            temp.add(resultSet.get(key).get(0).get(pos));
                            finalresult.put(key, temp);
                        }
                    }
                    else if(selectExpressionItem.getExpression() instanceof Function)
                    {
                        if(selectExpressionItem.getAlias() != null){
                            String columnName = selectExpressionItem.getAlias();
                            finalCol = new Column(new Table(null),selectExpressionItem.getAlias());
                        }

                        Function function = (Function) selectExpressionItem.getExpression();
                        Double to_add_val = 0.0;
                        try{
                        if(function.getName().equals("SUM")) {
                            to_add_val = new ArrayList<>(aggregateset.get(key).keySet()).get(0);
                        }else{
                            Double dub = new ArrayList<>(aggregateset.get(key).keySet()).get(0);
                            Integer int1 = aggregateset.get(key).get(dub);
                            to_add_val = dub/int1;
                        }
                        }
                        catch (Exception e)
                        {
                            e.printStackTrace();
                        }
                        if(finalresult.containsKey(key))
                        {
                            finalresult.get(key).add(to_add_val.toString());
                            //result_tosend.get(key).schema.add(new Column(new Table(),func.toString()));
                        }
                        else {
                            ArrayList<String> temp = new ArrayList<>();
                            temp.add(to_add_val.toString());
                            finalresult.put(key, temp);
                        }
                    }
                    if(!Data_Storage.finalSchema.contains(finalCol)){
                        Data_Storage.finalSchema.add(finalCol);
                    }
                }
            }
            return finalresult;
        }
}
