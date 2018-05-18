package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

import javax.xml.crypto.Data;
import java.lang.reflect.Array;
import java.util.*;

public class ProjectionIterator_Interface implements Iterator_Interface{
    Iterator_Interface iter;
    ArrayList<SelectExpressionItem> selectedColumns = new ArrayList<>();
    ArrayList<Column> schema = new ArrayList<>();
    LinkedHashMap<String, Schema> outSchema = new LinkedHashMap<>();
    LinkedHashMap<String, Schema> inSchema = new LinkedHashMap<>();
    LinkedHashMap<String, ArrayList<Integer>> allTableColumns = new LinkedHashMap<>();
    public ProjectionIterator_Interface(ArrayList<SelectExpressionItem> selectedColumns, Iterator_Interface iter){
        this.iter = iter;
        this.selectedColumns = selectedColumns;
    }
    @Override
    public void open(){
        iter.open();
        this.inSchema = iter.getSchema();
        generateSchema();
    }
    @Override
    public void print()
    {

    }
//    @Override
//    public Tuple readOneTuple() {
//        ArrayList<String> tuple = new ArrayList<>();
//        ArrayList<Column> schema = new ArrayList<>();
//        Column col;
//        String origColName=null,aliasColName,origTableName=null,aliasTableName;
//        PrimitiveValue[] tup = new PrimitiveValue[this.retschema.size()];
//            tup = iter.readOneTuple();
//        String tableName = null, colName;
//        if(tup!=null) {
////                Iterator project_iter = selectedColumns.iterator();
////                while (project_iter.hasNext()) {
////                    colName = project_iter.next().toString();
////                    String old_val = new String();
////                    old_val = old_val+(colName);
////                    if (colName.indexOf(".") != -1) {
////                        col = Data_Storage.stringSplitter(colName);
////                        tableName = col.getTable().getName();
////                        colName = col.getColumnName();
////                    }
////                    if (Data_Storage.alias_table.containsValue(colName)) {
////                        Set set = Data_Storage.alias_table.entrySet();
////                        Iterator set_iterator = set.iterator();
////                        while (set_iterator.hasNext()) {
////                            Map.Entry entry = (Map.Entry) set_iterator.next();
////                            if (entry.getValue().equals(colName)) {
////                                origColName = entry.getKey().toString();
////                            }
////
////                        }
////                        aliasColName = colName;
////                        if (origColName.indexOf(".") != -1) {
////                            col = Data_Storage.stringSplitter(origColName);
////                            origTableName = col.getTable().getName();
////                            origColName = col.getColumnName();
////                        }
////                    }
////                    else if(Data_Storage.alias_table.containsKey(old_val)){
////                        aliasColName = Data_Storage.alias_table.get(old_val);
////                    }
////                    else {
////                        origColName = colName;
////                        aliasColName = origColName;
////                    }
////                    if (Data_Storage.table_alias.containsKey(origTableName)) {
////                        aliasTableName = origTableName;
////                        origTableName = Data_Storage.table_alias.get(origTableName);
////                    } else {
////                        aliasTableName = null;
////                        origTableName = Data_Storage.current_schema.get(colName);
////                    }
////                    if(Data_Storage.from_alias!= null)
////                    {
////                        aliasTableName = Data_Storage.from_alias;
////                    }
////                    int position = tup.schema.indexOf(new Column(new Table(origTableName), origColName));
////                    if (position != -1) {
////                          tuple.add(tup.tuples.get(position));
////                          schema.add(Data_Storage.stringSplitter(old_val));
////                    }
////                }
////            return new Tuple(tuple,schema);
//                return tup;
//        }
//        else {
//                return null;
//            }
//
//    }

    @Override
    public PrimitiveValue[] readOneTuple(){
        PrimitiveValue[] retVal = new PrimitiveValue[this.outSchema.size()];
        PrimitiveValue[] retArr = iter.readOneTuple();
        if(retArr != null) {
            Iterator it = selectedColumns.iterator();
            int i = 0;
            while (it.hasNext()) {
                SelectItem sel = (SelectItem) it.next();
                if(sel instanceof AllColumns){
                    return retVal;
                }
                else if(sel instanceof AllTableColumns){
                    AllTableColumns all = (AllTableColumns) sel;
                    String tableName = all.getTable().getName();
                    ArrayList<Integer> cols = allTableColumns.get(tableName);
                    Iterator colIter = cols.iterator();
                    while(colIter.hasNext()){
                        int pos = (Integer) colIter.next();
                        retVal[i] = retArr[pos];
                        i++;
                    }
                }
                else{
                    SelectExpressionItem selItem = (SelectExpressionItem) sel;
                    if (selItem.getExpression() instanceof Column) {
                        Column col = (Column) selItem.getExpression();
                        String colName = col.getColumnName();
                        Integer pos = inSchema.get(colName).getPosition();
                        retVal[i] = retArr[pos];
                        i++;
                    }
                }
            }
            return retVal;
        }else{
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
    public String getFileName() {
        return null;
    }

    @Override
    public void reset() {

    }
    @Override
    public LinkedHashMap<String, Schema> getSchema(){
        return this.outSchema;
    }

    public void generateSchema(){
        Iterator it = selectedColumns.iterator();
        int pos = 0;
        while(it.hasNext()){
            //Need to handle *
            //Store it in a map , can be used in readOneTuple()
            SelectItem sel = (SelectItem)  it.next();
            if(sel instanceof AllTableColumns){
                AllTableColumns all = (AllTableColumns) sel;
                String tableName = all.getTable().getName();
                Iterator schemaIter = this.inSchema.values().iterator();
                ArrayList<Integer> tableColumns = new ArrayList<>();
                while(schemaIter.hasNext()){
                    Schema s = (Schema) schemaIter.next();
                    if(s.getTableName().equals(tableName)){
                        this.outSchema.put(s.getColumnName(),new Schema(tableName,s.getColumnName(),s.getDataType(),pos));
                        tableColumns.add(s.getPosition());
                        pos++;
                    }

                }
                allTableColumns.put(tableName,tableColumns);
            }else{
                SelectExpressionItem selItem = (SelectExpressionItem) sel;
                if(selItem.getExpression() instanceof Column){
                    Column col = (Column) selItem.getExpression();
                    String colName = col.getColumnName();
                    String alias = colName;
                    if(selItem.getAlias() != null){
                        alias = selItem.getAlias();
                    }
                    Schema s1 = this.inSchema.get(colName);
                    Schema s = new Schema(s1.getTableName(),colName,s1.getDataType(),pos);
                    if(selItem.getAlias() != null){
                        s.setColumnName(selItem.getAlias());
                    }
                    if(!(s1.getTableName().equals(col.getTable().getName()))){
                        s.setTableName(col.getTable().getName());
                    }
                    this.outSchema.put(alias,s);
                    pos++;
                }
            }
        }
    }
}
