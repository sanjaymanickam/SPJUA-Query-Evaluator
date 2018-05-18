package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

import java.sql.SQLException;
import java.util.*;

public class AggregateProjection implements Iterator_Interface{

    ArrayList<SelectExpressionItem> selectedColumns;
    Iterator_Interface iter;
    LinkedHashMap<String, Schema> inSchema = new LinkedHashMap<>();
    LinkedHashMap<String, Schema> outSchema = new LinkedHashMap<>();
    HashMap<Column, Integer> positionMap = new HashMap<>();
    int count = 0;
    ArrayList<PrimitiveValue[]> result = new ArrayList<>();
    public AggregateProjection(Iterator_Interface iter, ArrayList<SelectExpressionItem> selectedColumns){
        this.iter = iter;
        this.selectedColumns = selectedColumns;
    }
    @Override
    public Iterator_Interface getChild() {
        return this.iter;
    }
    @Override
    public PrimitiveValue[] readOneTuple() {
        if(this.result.size() == 0){
         groupByAndAggregate();
        }
        if(count == result.size()){
            return null;
        }
        return this.result.get(count++);
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

    @Override
    public void open() {
        iter.open();
        this.inSchema = iter.getSchema();
        generateSchema();
    }

    @Override
    public LinkedHashMap<String, Schema> getSchema() {
        return this.outSchema;
    }

    public void generateSchema(){
        Iterator it = selectedColumns.iterator();
        int pos = 0;
        while(it.hasNext()){
            SelectExpressionItem selItem = (SelectExpressionItem) it.next();
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
            }else{
                Function f = (Function) selItem.getExpression();
                String colName = "alias";
                if(selItem.getAlias() != null){
                 colName = selItem.getAlias();
                }
                Schema s = new Schema(null,colName,"DOUBLE",pos);
                this.outSchema.put(colName, s);
                pos++;
            }
        }
    }

    public void groupByAndAggregate(){
        PrimitiveValue[] retVal;
        LinkedHashMap<String, ArrayList<PrimitiveValue[]>> groups = new LinkedHashMap<>();
        HashSet<PrimitiveValue[]> test = new HashSet<>();
        PrimitiveValue[] to_add = null;
        retVal = iter.readOneTuple();
            while (retVal!=null){
                to_add = new PrimitiveValue[retVal.length];
                System.arraycopy(retVal,0,to_add,0,retVal.length);
                if(Data_Storage.groupbyflag == 0){
                    if(groups.containsKey("none")){
                        groups.get("none").add(to_add);
                    }else{
                        ArrayList<PrimitiveValue[]> temp = new ArrayList<>();
                        temp.add(to_add);
                        groups.put("none",temp);
                    }
                }else{
                    String key = "";
                    Iterator it = Data_Storage.groupByColumn.iterator();
                    while (it.hasNext()){
                        Column col = (Column) it.next();
                        String colName = col.getColumnName();
                        Schema s = this.inSchema.get(colName);
                        int pos = s.getPosition();
                        if(it.hasNext()){
                            key = key + to_add[pos]+",";
                        }else{
                            key = key + to_add[pos];
                        }
                    }
                    if(groups.containsKey(key)){
                        groups.get(key).add(to_add);
                    }else{
                        ArrayList<PrimitiveValue[]> temp = new ArrayList<>();
                        temp.add(to_add);
                        groups.put(key,temp);
                    }
                }
                retVal = iter.readOneTuple();
            }
        aggregate(groups);
    }

    public void aggregate(LinkedHashMap<String, ArrayList<PrimitiveValue[]>> groups){
        ArrayList<PrimitiveValue[]> result = new ArrayList<>();
        Iterator it = groups.keySet().iterator();
        while (it.hasNext()){
            LinkedHashMap<Function, Double[]> groupHash = new LinkedHashMap<>();
            String key = it.next().toString();
            Iterator tupleIter = groups.get(key).iterator();
            while(tupleIter.hasNext()){
                PrimitiveValue[] tuple = (PrimitiveValue[]) tupleIter.next();
                Iterator funcIter = Data_Storage.aggregate_operations.iterator();
                while(funcIter.hasNext()){
                    Function func = (Function) funcIter.next();
                    String func_name = func.getName();
                    Double curr_val= evaluate(tuple, func,this.inSchema);
                    if(groupHash.containsKey(func)){
                        if(func_name.equals("SUM")){
                            groupHash.get(func)[0] = groupHash.get(func)[0] + (Double) curr_val;
                        }else{
                            groupHash.get(func)[0] = groupHash.get(func)[0] + curr_val;
                            groupHash.get(func)[1] = groupHash.get(func)[1] + 1;
                        }

                    }else{
                        if(func_name.equals("SUM")){
                            Double[] temp = new Double[1];
                            temp[0] = curr_val;
                            groupHash.put(func,temp);
                        }else{
                            Double[] temp = new Double[2];
                            temp[0] = curr_val;
                            temp[1] = 1.0;
                            groupHash.put(func,temp);
                        }

                    }
                }
            }
            Iterator selectionIter = selectedColumns.iterator();
            PrimitiveValue[] tuple = new PrimitiveValue[selectedColumns.size()];
            int i=0;
            while(selectionIter.hasNext()){
                SelectExpressionItem selItem = (SelectExpressionItem) selectionIter.next();
                if(selItem.getExpression() instanceof Column){
                    Column col = (Column) selItem.getExpression();
                    int pos = this.inSchema.get(col.getColumnName()).getPosition();
                    tuple[i] = groups.get(key).get(0)[pos];
                }else{
                    Function func = (Function) selItem.getExpression();
                    if(func.getName().equals("SUM")){
                        tuple[i] = new DoubleValue(groupHash.get(func)[0]);
                    }else{
                        Double val = ((groupHash.get(func)[0]) / (groupHash.get(func)[1].intValue()));
                        tuple[i] = new DoubleValue(val);
                    }
                }
                i++;
            }
            this.result.add(tuple);
            //Project columns according to out Schema
        }
        //return null;
    }

    public Double evaluate(PrimitiveValue[] tup, Function func, LinkedHashMap<String, Schema> schema){

        Expression expr = func.getParameters().getExpressions().get(0);
        if (expr instanceof Column) {
            Column col = (Column) expr;
            int pos = schema.get(col.getColumnName()).getPosition();
            try {
                return tup[pos].toDouble();
            } catch (PrimitiveValue.InvalidPrimitive throwables) {
                throwables.printStackTrace();
            }

        } else {
            Data_Storage.eval = new Eval() {
                @Override
                public PrimitiveValue eval(Column column) {
                    if(positionMap.containsKey(column)){
                        return tup[positionMap.get(column)];
                    }else{
                        int pos = schema.get(column.getColumnName()).getPosition();
                        positionMap.put(column,pos);
                        return tup[pos];
                    }
                }
            };
            try {
                PrimitiveValue pr = Data_Storage.eval.eval(expr);
                return pr.toDouble();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return null;
    }
}
