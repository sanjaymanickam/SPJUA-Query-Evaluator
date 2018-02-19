package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;

import java.sql.SQLException;

public class Eval_Iterator implements Iterator {

    Iterator iter;
    Expression condition;
    String tableName;

    public Eval_Iterator(Iterator iter, Expression condition, String tableName) {
        this.iter = iter;
        this.condition = condition;
        this.tableName = tableName;
    }

    @Override
    public String[] readOneTuple() {
        String[] tuple = null;
        do {
            tuple = iter.readOneTuple();
            final String[] to_copy = tuple;
            if (tuple == null) {
                return null;
            }
            Eval eval = new Eval() {
                @Override
                public PrimitiveValue eval(Column column) throws SQLException {
                    int count = 0;
                    String req_name = column.getColumnName();
                    String data_type = Data_Storage.tables.get(tableName).get(req_name);
                    String[] tableColumns = Data_Storage.tableColumns.get(tableName);
                    for (String temp : tableColumns) {
                        if (temp.equals(req_name))
                            break;
                        count++;
                    }
                    System.out.println("Required Column Datatype is " + data_type + " Column name is " + req_name);
                    if (data_type.equals("int")) {
                        return new LongValue(to_copy[count]);
                    } else
                        return null;
                }
            };
            try {
                if (eval.eval(condition) == BooleanValue.FALSE)
                    tuple = null;
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
