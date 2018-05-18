package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;

import java.util.*;

public class Sort implements Iterator_Interface{
    Iterator_Interface iter;
    List<OrderByElement> orderByElements;
    LinkedHashMap<String, Schema> inSchema = new LinkedHashMap<>();
    LinkedHashMap<String, Schema> outSchema = new LinkedHashMap<>();
    ArrayList<PrimitiveValue[]> result = new ArrayList<>();
    int count = 0;
    public Sort(Iterator_Interface iter, List<OrderByElement> orderByElements){
        this.iter = iter;
        this.orderByElements = orderByElements;
    }
    @Override
    public void open(){
        this.inSchema = this.iter.getSchema();
        generateSchema();
    }

    @Override
    public PrimitiveValue[] readOneTuple(){
        if(result.size() == 0){
            sortData();
        }else if(result.size() == count){
            return null;
        }
        return this.result.get(count++);
    }
    @Override
    public LinkedHashMap<String,Schema> getSchema(){
        return this.outSchema;
    }
    @Override
    public Iterator_Interface getChild() {
        return this.iter;
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

    public void sortData(){
        PrimitiveValue[] retArr = iter.readOneTuple();
        while (retArr != null){
            result.add(retArr);
            retArr = iter.readOneTuple();
        }
        Collections.sort(result, new CustomComparator(orderByElements,inSchema));

    }

    private void generateSchema() {
        this.outSchema = this.inSchema;
    }

}
