package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.StringTokenizer;

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
            if(tup == null)
            {
                return null;
            }
            tuple = tup.tuples;
            schema = tup.schema;
            final ArrayList<String> to_copy = tuple;
            final ArrayList<Column> schema_final = schema;
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
                    Column col = null;
                    if(Data_Storage.table_alias.containsKey(column.getTable().getName())) {
                        col = new Column(new Table(Data_Storage.table_alias.get(column.getTable().getName())),col_name);
                    }
                    else if(Data_Storage.alias_table.containsKey(column.toString()))
                    {
                        StringTokenizer str_tok = new StringTokenizer(Data_Storage.alias_table.get(column.toString()),".");
                        String tableName = str_tok.nextElement().toString();
                        col = new Column(new Table(tableName),column.getColumnName());
                    }
                    else
                    {
                        col = column;
                    }
                    int position =schema_final.indexOf(col);
                    String tableName = schema_final.get(position).getTable().getName();
                    if(Data_Storage.table_alias.containsKey(tableName))
                    {
                        tableName = Data_Storage.table_alias.get(tableName);
                    }
                    String data_type = Data_Storage.tables.get(tableName).get(col_name);
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
//        System.out.println("EVAL");
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
