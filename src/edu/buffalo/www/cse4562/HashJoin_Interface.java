package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;

public class HashJoin_Interface implements Iterator_Interface {
    Iterator_Interface iter1,iter2;
    Expression condition;
    LinkedHashMap<PrimitiveValue,ArrayList<PrimitiveValue[]>> builder;
    int count=0;
    int data_flag = 0;
    BinaryExpression binaryExpression;
    Column right,left;
    PrimitiveValue[] send_tuple = null;
    PrimitiveValue[] to_check = null;
    ArrayList<PrimitiveValue[]> to_send = new ArrayList<>();
    LinkedHashMap<String, Schema> outSchema = new LinkedHashMap<>();
    LinkedHashMap<String, Schema> child1Schema = new LinkedHashMap<>();
    LinkedHashMap<String, Schema> child2Schema = new LinkedHashMap<>();
    int position;
    Iterator to_send_iterator = null;
    public HashJoin_Interface(Iterator_Interface iter1, Iterator_Interface iter2, Expression condition)
    {
        this.iter1 = iter1;
        this.iter2 = iter2;
        this.condition = condition;
        this.binaryExpression = (BinaryExpression)condition;
        this.left = (Column)binaryExpression.getLeftExpression();
        this.right = (Column) binaryExpression.getRightExpression();
    }

    @Override
    public void open(){
        iter1.open();
        iter2.open();
        this.child1Schema = iter1.getSchema();
        this.child2Schema = iter2.getSchema();
        generateSchema();
    }

    @Override
    public PrimitiveValue[] readOneTuple() {
        //return null;
        //send_tuple = new PrimitiveValue[outSchema.size()];

        if(Data_Storage.hash_flag==0)
        {
            builder = new LinkedHashMap<>();
            if(iter1 instanceof HashJoin_Interface || iter1 instanceof ProjectionIterator_Interface || iter1 instanceof AggregateProjection)
            {
                read_file_tostore(iter2);
                while(iter1!=null)
                {

                    PrimitiveValue[] tup = iter1.readOneTuple();
                    //Tuple to_tup= new Tuple();
                    PrimitiveValue[] retVal = new PrimitiveValue[iter1.getSchema().size()];
                    int i=0;
                    if(tup!=null) {
                        for(int j=0;j<tup.length;j++){
                            retVal[i] = tup[j];
                            i++;
                        }
                        build_table(retVal,this.outSchema);
                        Data_Storage.hash_flag=1;
                    }else
                        break;
                }
            }
            else
            {
                read_file(iter1);
                read_file_tostore(iter2);
                Data_Storage.hash_flag = 1;
            }
        }

        do {

            if(to_send_iterator!=null) {
                if (to_send_iterator.hasNext()) {
                    PrimitiveValue[] temp = (PrimitiveValue[]) to_send_iterator.next();
                    position = 0;
                    if(Data_Storage.selfJoin == 1){
                        send_tuple = new PrimitiveValue[outSchema.size()*2];
                    }else{
                        send_tuple = new PrimitiveValue[outSchema.size()];
                    }

                    for(int j=0;j<temp.length;j++){
                        send_tuple[position] = temp[j];
                        position++;
                    }
                    for(int j=0;j<to_check.length;j++){
                        send_tuple[position] = to_check[j];
                        position++;
                    }
                    data_flag = 0;
                }
                else
                {
                    to_send_iterator = null;
                    data_flag= 1;
                }
            }
            else
            {
                    if (count != Data_Storage.stored_files.get(iter2).size()) {
                        to_check = Data_Storage.stored_files.get(iter2).get(count);
                        int pos = this.child2Schema.get(left.getColumnName()) != null ? this.child2Schema.get(left.getColumnName()).getPosition() : this.child2Schema.get(right.getColumnName()).getPosition();
                        if (builder.containsKey(to_check[pos])) {
                            to_send = builder.get(to_check[pos]);
                            to_send_iterator = to_send.iterator();
                        }
                        count++;
                        data_flag = 1;
                    } else {
                        data_flag = 0;
                        send_tuple = null;
                    }
            }
        }while(data_flag!=0);
        return send_tuple;
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
    void read_file(Iterator_Interface file){
        //ArrayList<PrimitiveValue[]> temp_array = new ArrayList<>();
        PrimitiveValue[] temp_tuple;// = new PrimitiveValue[file.getSchema().size()];
        do
        {
            temp_tuple = file.readOneTuple();
            if(temp_tuple==null)
                break;
            build_table(temp_tuple,this.child1Schema);
        }while(temp_tuple!= null);
    }
    void read_file_tostore(Iterator_Interface file){
        PrimitiveValue[] temp_tuple = new PrimitiveValue[file.getSchema().size()];
        ArrayList<PrimitiveValue[]> temp_array = new ArrayList<>(100000);
        do
        {
            temp_tuple = file.readOneTuple();
            if(temp_tuple!=null)
                temp_array.add(temp_tuple);
        }while(temp_tuple!= null);
        Data_Storage.stored_files.put(file,temp_array);
    }
    void build_table(PrimitiveValue[] tup, LinkedHashMap<String,Schema> schema)
    {
        int pos = this.child1Schema.get(left.getColumnName()) != null ? this.child1Schema.get(left.getColumnName()).getPosition() :this.child1Schema.get(right.getColumnName()).getPosition();
        if(!builder.containsKey(tup[pos]))
        {
            ArrayList<PrimitiveValue[]> temp = new ArrayList<>();
            temp.add(tup);
            builder.put(tup[pos],temp);
        }
        else
        {
            try {
                builder.get(tup[pos]).add(tup);
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }
    }
    @Override
    public LinkedHashMap<String, Schema> getSchema(){
        return this.outSchema;
    }
    public void generateSchema(){
        int i =0;
        for(Schema s : this.iter1.getSchema().values()){
            this.outSchema.put(s.getColumnName(), new Schema(s.getTableName(), s.getColumnName(), s.getDataType(),i));
            i++;
        }
        for(Schema s : this.iter2.getSchema().values()){
            this.outSchema.put(s.getColumnName(), new Schema(s.getTableName(), s.getColumnName(), s.getDataType(),i));
            i++;
        }
    }
}