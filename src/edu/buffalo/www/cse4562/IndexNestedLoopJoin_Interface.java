package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;

import java.io.*;
import java.util.*;

public class IndexNestedLoopJoin_Interface implements Iterator_Interface {
    Iterator_Interface iter1, iter2;
    Expression condition;
    LinkedHashMap<String, Schema> outSchema = new LinkedHashMap<>();
    LinkedHashMap<String, Schema> child1Schema = new LinkedHashMap<>();
    LinkedHashMap<String, Schema> child2Schema = new LinkedHashMap<>();
    ArrayList<PrimitiveValue[]> tuples = new ArrayList<>();
    Column leftColumn,rightColumn;
    int count = 0;
    boolean indexAvailable = false;
    String fileName = "";
    public IndexNestedLoopJoin_Interface(Iterator_Interface iter1, Iterator_Interface iter2, Expression condition) {
        this.iter1 = iter1;
        this.iter2 = iter2;
        this.condition = condition;
        this.leftColumn = (Column)((BinaryExpression)condition).getLeftExpression();
        this.rightColumn = (Column)((BinaryExpression)condition).getRightExpression();
    }

    @Override
    public void open() {
        iter1.open();
        iter2.open();
        this.child1Schema = iter1.getSchema();
        this.child2Schema = iter2.getSchema();
        generateSchema();
    }

    @Override
    public PrimitiveValue[] readOneTuple() {
        if(iter1 instanceof IndexNestedLoopJoin_Interface || iter1 instanceof ProjectionIterator_Interface || iter1 instanceof AggregateProjection){

        }else{
            read_file(iter1);
            if(Data_Storage.indexColumns.contains(rightColumn.getColumnName())){
                indexAvailable = true;
                String tableName = rightColumn.getTable().getName();
                String colName = rightColumn.getColumnName();
                if(Data_Storage.table_alias.containsKey(tableName)){
                    tableName = Data_Storage.table_alias.get(tableName);
                }
                fileName = tableName+"_"+colName+"_";
            }
        }

        Iterator tupleIter = tuples.iterator();
        if(tupleIter.hasNext()){
            PrimitiveValue[] tuple = (PrimitiveValue[]) tupleIter.next();
            int pos = this.child1Schema.get(leftColumn.getColumnName()).getPosition();
            String value = tuple[pos].toString();
            fileName = fileName+"value";
            if(iter2 instanceof FileIterator_Interface){
                try {
                    ((FileIterator_Interface)iter2.getChild()).read = new BufferedReader(new FileReader(new File("index/"+fileName+".txt")));
                }catch (IOException e){
                    e.printStackTrace();
                }
            }else{
                try {
                    ((FileIterator_Interface)iter2.getChild().getChild()).read = new BufferedReader(new FileReader(new File("index/"+fileName+".txt")));
                }catch (IOException e){
                    e.printStackTrace();
                }
            }
            PrimitiveValue[] retArr = new PrimitiveValue[this.outSchema.size()];
            PrimitiveValue[] secondTuple = iter2.readOneTuple();
            while(secondTuple !=null){

            }
        }
        return null;
    }
    @Override
    public LinkedHashMap<String, Schema> getSchema() {
        return this.outSchema;
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
    public String getFileName() {
        return null;
    }

    @Override
    public void reset() {

    }

    public void generateSchema() {
        int i = 0;
        for (Schema s : this.iter1.getSchema().values()) {
            this.outSchema.put(s.getColumnName(), new Schema(s.getTableName(), s.getColumnName(), s.getDataType(), i));
            i++;
        }
        for (Schema s : this.iter2.getSchema().values()) {
            this.outSchema.put(s.getColumnName(), new Schema(s.getTableName(), s.getColumnName(), s.getDataType(), i));
            i++;
        }
    }

    public void read_file(Iterator_Interface iter1){
        PrimitiveValue[] retArr = iter1.readOneTuple();
        while (retArr != null){
            tuples.add(retArr);
            retArr = iter1.readOneTuple();
        }
    }
}
