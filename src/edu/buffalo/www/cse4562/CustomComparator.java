package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;

import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

public class CustomComparator implements Comparator<PrimitiveValue[]> {

    List<OrderByElement> orderByElements;
    LinkedHashMap<String,Schema> schema;
    HashMap<OrderByElement,Integer> position = new HashMap<>();
    public CustomComparator(List<OrderByElement> orderByElements, LinkedHashMap<String, Schema> schema){
        this.orderByElements = orderByElements;
        this.schema = schema;
    }
    @Override
    public int compare(PrimitiveValue[] one, PrimitiveValue[] two){
        for(OrderByElement orderBy : this.orderByElements){
            Column c = (Column) orderBy.getExpression();
            int pos=-1;
            if(this.position.containsKey(orderBy)){
                pos = this.position.get(orderBy);
            }else{
                pos = schema.get(c.getColumnName()).getPosition();
                this.position.put(orderBy,pos);
            }

            PrimitiveValue val = one[pos];

            if(val instanceof StringValue){
                int compare = one[pos].toString().compareTo(two[pos].toString());
                if(compare > 0){
                    return orderBy.isAsc()? 1 : -1;
                }else if(compare < 0){
                    return orderBy.isAsc()? -1 : 1;
                }else{
                    continue;
                }
            }
            else if(val instanceof DoubleValue){
                try {
                    if(one[pos].toDouble() > two[pos].toDouble()){
                        return orderBy.isAsc()? 1 : -1;
                    }else if(one[pos].toDouble() < two[pos].toDouble()){
                        return orderBy.isAsc()? -1 : 1;
                    }else{
                        continue;
                    }
                } catch (PrimitiveValue.InvalidPrimitive throwables) {
                    throwables.printStackTrace();
                }
            }
            else if(val instanceof LongValue){
                try {
                    if(one[pos].toLong() > two[pos].toLong()){
                        return orderBy.isAsc()? 1 : -1;
                    }else if(one[pos].toLong() < two[pos].toLong()){
                        return orderBy.isAsc()? -1 : 1;
                    }else{
                        continue;
                    }
                } catch (PrimitiveValue.InvalidPrimitive e) {
                    break;
                }
            }
        }
        return 0;
    }
}
