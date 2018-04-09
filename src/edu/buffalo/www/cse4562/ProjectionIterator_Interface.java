package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

import javax.xml.crypto.Data;
import java.util.*;

public class ProjectionIterator_Interface implements Iterator_Interface{
    Iterator_Interface iter;
    HashMap<String,String> selectedColumns;
    public ProjectionIterator_Interface(HashMap<String,String> selectedColumns, Iterator_Interface iter){
        this.iter = iter;
        this.selectedColumns = selectedColumns;
    }
    @Override
    public Tuple readOneTuple() {
        ArrayList<String> tuple = new ArrayList<>();
        ArrayList<Column> schema = new ArrayList<>();
        Column col;
        String origColName=null,aliasColName,origTableName=null,aliasTableName;
        Tuple tup;
            tup = iter.readOneTuple();
        if(tup!=null) {
            if(Data_Storage.aggregateflag==0) {
                Iterator project_iter = selectedColumns.keySet().iterator();
                while (project_iter.hasNext()) {
                    String tableName = null, colName;
                    colName = project_iter.next().toString();
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
                    } else {
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
                    tuple.add(tup.tuples.get(position));
                    schema.add(tup.schema.get(position));
                }
            }
            else
            {
                tuple.addAll(tup.tuples);
                schema.addAll(tup.schema);
            }
            return new Tuple(tuple, schema);
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
