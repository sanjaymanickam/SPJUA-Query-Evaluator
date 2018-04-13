package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

import javax.xml.crypto.Data;
import java.util.*;

public class ProjectionIterator_Interface implements Iterator_Interface{
    Iterator_Interface iter;
    ArrayList<Column> selectedColumns = new ArrayList<>();
    public ProjectionIterator_Interface(ArrayList<Column> selectedColumns, Iterator_Interface iter){
        this.iter = iter;
        this.selectedColumns = selectedColumns;
    }
    @Override
    public void print()
    {

    }
    @Override
    public Tuple readOneTuple() {
        ArrayList<String> tuple = new ArrayList<>();
        ArrayList<Column> schema = new ArrayList<>();
        Column col;
        String origColName=null,aliasColName,origTableName=null,aliasTableName;
        Tuple tup;
            tup = iter.readOneTuple();
        String tableName = null, colName;
        if(tup!=null) {
                Iterator project_iter = selectedColumns.iterator();
                while (project_iter.hasNext()) {
                    colName = project_iter.next().toString();
                    String old_val = new String();
                    old_val = old_val+(colName);
                    if (colName.indexOf(".") != -1) {
                        col = Data_Storage.stringSplitter(colName);
                        tableName = col.getTable().getName();
                        colName = col.getColumnName();
                    }
                    if (Data_Storage.alias_table.containsValue(colName)) {
                        Set set = Data_Storage.alias_table.entrySet();
                        Iterator set_iterator = set.iterator();
                        while (set_iterator.hasNext()) {
                            Map.Entry entry = (Map.Entry) set_iterator.next();
                            if (entry.getValue().equals(colName)) {
                                origColName = entry.getKey().toString();
                            }

                        }
                        aliasColName = colName;
                        if (origColName.indexOf(".") != -1) {
                            col = Data_Storage.stringSplitter(origColName);
                            origTableName = col.getTable().getName();
                            origColName = col.getColumnName();
                        }
                    }
                    else if(Data_Storage.alias_table.containsKey(old_val)){
                        aliasColName = Data_Storage.alias_table.get(old_val);
                    }
                    else {
                        origColName = colName;
                        aliasColName = origColName;
                    }
                    if (Data_Storage.table_alias.containsKey(origTableName)) {
                        aliasTableName = origTableName;
                        origTableName = Data_Storage.table_alias.get(origTableName);
                    } else {
                        aliasTableName = null;
                        origTableName = Data_Storage.current_schema.get(colName);
                    }
                    int position = tup.schema.indexOf(new Column(new Table(aliasTableName), origColName));
                    if (position != -1) {
                          tuple.add(tup.tuples.get(position));
                          schema.add(Data_Storage.stringSplitter(old_val));
                    }
                }
            return new Tuple(tuple,schema);
//                return tup;
        }
        else {
                return null;
            }

    }

    @Override
    public Iterator_Interface getChild() {
        return iter;
    }
    public void setChild(Iterator_Interface iter){
        //this.iter = iter;
    }

    @Override
    public void reset() {

    }

}
