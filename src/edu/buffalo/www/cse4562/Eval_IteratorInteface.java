package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class Eval_IteratorInteface implements Iterator_Inteface {

    Iterator_Inteface iter;
    Expression condition;
    List<String> schema;
    String tableName;

    public Eval_IteratorInteface(Iterator_Inteface iter,List<String>schema,Expression condition, String tableName) {
        this.iter = iter;
        this.schema = schema;
        this.condition = condition;
        this.tableName = tableName;
    }

    @Override
    public ArrayList<String> readOneTuple() {
        ArrayList<String> tuple = new ArrayList<>();
        do {
            tuple = iter.readOneTuple();
            final ArrayList<String> to_copy = tuple;
            if (tuple == null) {
                return null;
            }
            Eval eval = new Eval() {
                @Override
                public PrimitiveValue eval(Column column) throws SQLException {
                    int count = 0;
                    String req_name = column.getColumnName();
                    String data_type = Data_Storage.tables.get(tableName).get(req_name);
                    ArrayList<String> tableColumns = Data_Storage.tableColumns.get(tableName);
                    for (String temp : tableColumns) {
                        if (temp.equals(req_name))
                            break;
                        count++;
                    }
//                    System.out.println("Required Column Datatype is " + data_type + " Column name is " + req_name);
                    if (data_type.equals("int")) {
                        return new LongValue(to_copy.get(count));
                    }
                    else if(data_type.equals("string")||data_type.equals("varchar")|data_type.equals("char")){
                        return new StringValue(to_copy.get(count));
                    }
                    else if(data_type.equals("decimal"))
                    {
                        return new DoubleValue(to_copy.get(count));
                    }
                    else if(data_type.equals("date"))
                    {
                        return new DateValue(to_copy.get(count));
                    }
                    else{
                        return null;
                    }

                }
            };
            try {
//                System.out.println(eval.eval(condition).getType());
                if(eval.eval(condition)==BooleanValue.TRUE)
                {

                }
                else if(eval.eval(condition) == BooleanValue.FALSE) {
                    tuple = null;
                }
                else if(eval.eval(condition) instanceof LongValue)
                {
                    tuple.add(eval.eval(condition).toString());
                }
                else if(eval.eval(condition) instanceof DoubleValue)
                {
                    tuple.add(eval.eval(condition).toString());
                }

            } catch (SQLException e) {
                e.printStackTrace();
            }
        } while (tuple == null);
        return tuple;
    }

    @Override
    public void reset() {

    }
}
