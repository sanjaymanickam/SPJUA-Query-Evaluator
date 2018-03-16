package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;

import java.sql.SQLException;
import java.util.ArrayList;

public class EvalIterator_Interface implements Iterator_Interface{
    Iterator_Interface iter;
    Expression condition;
    public EvalIterator_Interface(Iterator_Interface iter,Expression condition)
    {
        this.iter = iter;
        this.condition = condition;
    }
    @Override
    public Tuple readOneTuple() {
       ArrayList<String> tuple;
       ArrayList<Column> schema;
       Tuple tup;
        do{
            tup = iter.readOneTuple();
            tuple = tup.tuples;
            schema = tup.schema;
            final ArrayList<String> to_copy = tuple;
            final ArrayList<Column> schema_final = schema;
            if(tuple == null)
            {
                return null;
            }
            Eval eval = new Eval() {
                @Override
                public PrimitiveValue eval(Column column) {
                    String col_name;
                    if((col_name =Data_Storage.alias_table.get(column.getColumnName())) != null)
                    {

                    }
                    else
                    {
                        col_name = column.getColumnName();
                    }
                    int position =schema_final.indexOf(column);
                    String data_type = Data_Storage.tables.get(schema_final.get(position).getTable().getName()).get(col_name);
                        if (data_type.equals("INT")) {
                            return new LongValue(to_copy.get(position));
                        } else if (data_type.equals("STRING") || data_type.equals("VARCHAR") | data_type.equals("CHAR")) {
                            return new StringValue(to_copy.get(position));
                        } else if (data_type.equals("DECIMAL")) {
                            return new DoubleValue(to_copy.get(position));
                        } else if (data_type.equals("DATE")) {
                            return new DateValue(to_copy.get(position));
                        } else {
                            return null;
                        }

                }
            };
            try{
                PrimitiveValue pr = eval.eval(condition);
                if(pr == BooleanValue.FALSE) {
                  tuple = null;
                }
                else
                {
                    if(pr != BooleanValue.TRUE && pr != null)
                        tuple.add(pr.toString());
                }
            }catch(SQLException e)
            {
                e.printStackTrace();
            }
        }while(tuple ==null);
        System.out.println("EVAL");
        Tuple ret_tuple = new Tuple(tuple,schema);
        ret_tuple.tuples = tuple;
        ret_tuple.schema =schema;
        return ret_tuple;
    }

    @Override
    public Iterator_Interface getChild() {
        return iter;
    }
    public void setChild(Iterator_Interface iter){
        this.iter = iter;
    }

    @Override
    public void reset() {

    }
}
