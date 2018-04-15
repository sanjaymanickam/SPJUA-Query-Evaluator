package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;

public class HashJoin_Interface implements Iterator_Interface {
    Iterator_Interface iter1,iter2;
    Expression condition;
    LinkedHashMap<String,ArrayList<Tuple>> builder;
    int count=0;
    int data_flag = 0;
    BinaryExpression binaryExpression;
    Column right,left;
    Tuple send_tuple = new Tuple();
    Tuple to_check = null;
    ArrayList<Tuple> to_send = new ArrayList<>();
    int pos;
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
    public Tuple readOneTuple() {
        if(Data_Storage.hash_flag==0)
        {
            builder = new LinkedHashMap<>();
            if(iter1 instanceof HashJoin_Interface)
            {
                read_file_tostore(iter2);
                while(iter1!=null)
                {

                    Tuple tup = iter1.readOneTuple();
                    Tuple to_tup= new Tuple();
                    if(tup!=null) {
                        to_tup.tuples.addAll(tup.tuples);
                        to_tup.schema.addAll(tup.schema);
                        build_table(to_tup);
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
                    Tuple temp = (Tuple) to_send_iterator.next();
                    send_tuple.tuples.clear();
                    send_tuple.schema.clear();
                    send_tuple.tuples.addAll(temp.tuples);
                    send_tuple.schema.addAll(temp.schema);
                    send_tuple.schema.addAll(to_check.schema);
                    send_tuple.tuples.addAll(to_check.tuples);
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
                        int pos = to_check.schema.indexOf(left) != -1 ? to_check.schema.indexOf(left) : to_check.schema.indexOf(right);
                        if (builder.containsKey(to_check.tuples.get(pos))) {
                            to_send = builder.get(to_check.tuples.get(pos));
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
    public void reset() {

    }
    void read_file(Iterator_Interface file){
        Tuple temp_tuple;
        do
        {
            temp_tuple = file.readOneTuple();
            if(temp_tuple==null)
                break;
            build_table(temp_tuple);
        }while(temp_tuple!= null);
    }
    void read_file_tostore(Iterator_Interface file){
        Tuple temp_tuple;
        ArrayList<Tuple> temp_array = new ArrayList<>(100000);
        do
        {
            temp_tuple = file.readOneTuple();
            if(temp_tuple!=null)
                temp_array.add(temp_tuple);
        }while(temp_tuple!= null);
        Data_Storage.stored_files.put(file,temp_array);
    }
    void build_table(Tuple tup)
    {

        int pos = tup.schema.indexOf(left)!=-1 ? tup.schema.indexOf(left):tup.schema.indexOf(right);
        if(!builder.containsKey(tup.tuples.get(pos)))
        {
            ArrayList<Tuple> temp = new ArrayList<>();
            temp.add(tup);
            builder.put(tup.tuples.get(pos),temp);
        }
        else
        {
            try {
                builder.get(tup.tuples.get(pos)).add(tup);
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }
    }
}