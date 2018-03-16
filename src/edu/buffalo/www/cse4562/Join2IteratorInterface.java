package edu.buffalo.www.cse4562;

import javax.xml.crypto.Data;
import java.util.ArrayList;

public class Join2IteratorInterface implements Iterator_Interface{

    public Iterator_Interface iter1,iter2;
    String table1,table2;
    Tuple to_send = new Tuple();
    Tuple temp_tuple = new Tuple();
    public Join2IteratorInterface(Iterator_Interface iter1,Iterator_Interface iter2)
    {
        this.iter1 = iter1;
        this.iter2 = iter2;
    }
    @Override
    public Tuple readOneTuple() {
        to_send.schema = new ArrayList<>();
        to_send.tuples = new ArrayList<>();
        if (!Data_Storage.stored_files.containsKey(iter2))
        {
            if(!Data_Storage.stored_files.containsValue(Data_Storage.stored_files.get(iter2)))
                read_file(iter2);
            Data_Storage.file_flag.put(iter2,false);
        }
        if(Data_Storage.file_flag.get(iter2) && Data_Storage.stored_file_iterators.get(iter2).hasNext())
        {

            to_send.tuples.addAll(Data_Storage.file_temp_tuple.get(iter2).tuples);
            to_send.schema.addAll(Data_Storage.file_temp_tuple.get(iter2).schema);
            if(to_send != null) {
                if (Data_Storage.stored_file_iterators.get(iter2).hasNext()) {
                    temp_tuple = (Tuple) Data_Storage.stored_file_iterators.get(iter2).next();
                    if (temp_tuple != null) {
                        to_send.tuples.addAll(temp_tuple.tuples);
                        to_send.schema.addAll(temp_tuple.schema);
                    } else {
                        to_send = null;
                    }
                } else {
                    Data_Storage.file_flag.replace(iter2, false);
                }
            }
            else
            {
                Data_Storage.file_flag.replace(iter2,false);
            }
        }
        else
        {

            if(!Data_Storage.stored_file_iterators.containsKey(iter2))
            {
                Data_Storage.stored_file_iterators.put(iter2,Data_Storage.stored_files.get(iter2).iterator());
            }
            else
            {
                Data_Storage.stored_file_iterators.replace(iter2,Data_Storage.stored_files.get(iter2).iterator());
            }
            if(!Data_Storage.file_temp_tuple.containsKey(iter2))
                Data_Storage.file_temp_tuple.put(iter2,iter1.readOneTuple());
            else
                Data_Storage.file_temp_tuple.replace(iter2,iter1.readOneTuple());
            if(Data_Storage.file_temp_tuple.get(iter2)!=null) {
                to_send.tuples.addAll(Data_Storage.file_temp_tuple.get(iter2).tuples);
                to_send.schema.addAll(Data_Storage.file_temp_tuple.get(iter2).schema);
                Tuple tup = (Tuple) Data_Storage.stored_file_iterators.get(iter2).next();
                if(tup!=null) {
                    to_send.tuples.addAll(tup.tuples);
                    to_send.schema.addAll(tup.schema);
                }
                Data_Storage.file_flag.replace(iter2,true);
            }
            else
            {
                to_send = null;
            }
        }
        return to_send;
    }

    @Override
    public Iterator_Interface getChild() {
        return iter1;
    }

    @Override
    public void setChild(Iterator_Interface iter) {

    }

    @Override
    public void reset() {

    }

    void read_file(Iterator_Interface file){
        Tuple temp_tuple;
        ArrayList<Tuple> temp_array = new ArrayList<>();
        do
        {
            temp_tuple = file.readOneTuple();
            if(temp_tuple!=null)
                temp_array.add(temp_tuple);
        }while(temp_tuple!= null);
        Data_Storage.stored_files.put(file,temp_array);
    }
}
