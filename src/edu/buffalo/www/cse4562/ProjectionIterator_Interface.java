package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;

public class ProjectionIterator_Interface implements Iterator_Interface{
    Iterator_Interface iter;
    public ProjectionIterator_Interface(Iterator_Interface iter){
        this.iter = iter;
    }
    @Override
    public Tuple readOneTuple() {
        ArrayList<String> tuple = new ArrayList<>();
        ArrayList<Column> schema = new ArrayList<>();
        Tuple tup;
            tup = iter.readOneTuple();
        if(tup!=null) {
            Iterator project_iter = Data_Storage.selectedColumns.keySet().iterator();
            while (project_iter.hasNext()) {
                String colName = project_iter.next().toString();
                String tableName = null;
                if(colName.indexOf(".")!=-1) {
                    StringTokenizer str_tok = new StringTokenizer(colName, ".");
                    tableName = str_tok.nextElement().toString();
                    colName = str_tok.nextElement().toString();
                }
                if(Data_Storage.table_alias.containsKey(tableName))
                {
                 tableName = Data_Storage.table_alias.get(tableName);
                }
                else {
                    tableName = Data_Storage.current_schema.get(colName);
                }

                /*while(str_tok.hasMoreElements())
                {
                    colName = str_tok.nextElement().toString();
                }*/
                int position = tup.schema.indexOf(new Column(new Table(tableName), colName));
                tuple.add(tup.tuples.get(position));
                schema.add(tup.schema.get(position));

            }
            return new Tuple(tuple, schema);
        }
        else {
                return null;
            }

    }

    @Override
    public Iterator_Interface getChild() {
        return null;
    }
    public void setChild(Iterator_Interface iter){
        //this.iter = iter;
    }

    @Override
    public void reset() {

    }

}
