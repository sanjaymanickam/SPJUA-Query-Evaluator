package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

import java.sql.SQLException;
import java.util.*;

public class EvalIterator_Interface implements Iterator_Interface{
    Iterator_Interface iter;
    Expression condition;
    LinkedHashMap<String, Schema> retSchema = new LinkedHashMap<>();
    HashMap<Column, Integer> positonMap = new HashMap<>();
    public EvalIterator_Interface(Iterator_Interface iter,Expression condition)
    {
        this.iter = iter;
        this.condition = condition;
    }
    @Override
    public void open(){
        iter.open();
        retSchema = iter.getSchema();
    }

    @Override
    public PrimitiveValue[] readOneTuple() {
        Data_Storage.read_tuple++;
       ArrayList<String> tuple = null;
       ArrayList<Column> schema = null;
        HashSet<Column> test = new HashSet<>();
       Tuple tup;
        PrimitiveValue[] retVal = new PrimitiveValue[retSchema.size()];
        do{
            retVal= iter.readOneTuple();
            if(retVal == null)
            {
               return null;
            }
            final PrimitiveValue[] retArr= retVal;
             Data_Storage.eval = new Eval() {
                @Override
                public PrimitiveValue eval(Column column) {

                    if(positonMap.containsKey(column)){
                        return retArr[positonMap.get(column)];
                    }else{
                        String colName = column.getColumnName();
                        Integer pos = retSchema.get(colName).getPosition();
                        positonMap.put(column,pos);
                        return retArr[pos];
                    }
                }
                    /*String col_name = column.getColumnName();
                    String tableName=null;
                    String origtableName = null;
                    if(Data_Storage.alias_table.containsKey(col_name))
                        col_name = Data_Storage.alias_table.get(col_name);
                    if(column.getTable().getName()==null)
                        tableName = Data_Storage.current_schema.get(col_name);
                    else
                        tableName = column.getTable().getName();
                    int position = schema_final.indexOf(new Column(new Table(tableName),col_name));
                    if(Data_Storage.table_alias.containsKey(tableName))
                    {
                        origtableName = Data_Storage.table_alias.get(tableName);
                    }
                    else
                    {
                        origtableName = tableName;
                    }
                    String data_type = Data_Storage.tables.get(origtableName).get(col_name);
                        if (data_type.equals("INTEGER")) {
                            return new LongValue(to_copy.get(position));
                        } else if (data_type.equals("STRING") || data_type.equals("VARCHAR") | data_type.equals("CHAR")) {
                            return new StringValue(to_copy.get(position));
                        } else if (data_type.equals("DOUBLE")) {
                            return new DoubleValue(to_copy.get(position));
                        } else if (data_type.equals("DATE")) {
                            return new DateValue(to_copy.get(position));
                        } else {
                            return null;
                        }
                }*/
            };
            try{
                test.clear();
                PrimitiveValue pr = Data_Storage.eval.eval(condition);
                if(pr == BooleanValue.FALSE) {
                  retVal = null;
                }
                else
                {
                    if(pr != BooleanValue.TRUE && pr != null) {
                        tuple.add(pr.toString());
                    }
                }
            }catch(SQLException e)
            {
                e.printStackTrace();
            }
        }while(retVal ==null);
//        System.out.println("EVAL");
//        Tuple ret_tuple = new Tuple(tuple,schema);
//        ret_tuple.tuples = tuple;
//        ret_tuple.schema =schema;
        //return ret_tuple;
        return retVal;
    }

    @Override
    public Iterator_Interface getChild() {
        return iter;
    }
    public void setChild(Iterator_Interface iter){
        this.iter = iter;
    }

    @Override
    public void print()
    {
        System.out.println("EvalIterator"+condition+" ");
    }
    @Override
    public void reset() {
            iter.reset();
    }
    @Override
    public LinkedHashMap<String, Schema> getSchema(){
        return this.retSchema;
    }

}
