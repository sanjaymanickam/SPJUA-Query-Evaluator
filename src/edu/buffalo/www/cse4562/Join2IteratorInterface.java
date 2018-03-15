package edu.buffalo.www.cse4562;

import javax.xml.crypto.Data;
import java.util.ArrayList;

public class Join2IteratorInterface implements Iterator_Interface{

    Iterator_Interface iter1,iter2;
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
        }
        if(Data_Storage.file_flag)
        {
            to_send.tuples.addAll(Data_Storage.file1_temp_tuple.tuples);
            to_send.schema.addAll(Data_Storage.file1_temp_tuple.schema);
            if(Data_Storage.it.hasNext()) {
                temp_tuple = (Tuple) Data_Storage.it.next();
                to_send.tuples.addAll(temp_tuple.tuples);
                to_send.schema.addAll(temp_tuple.schema);
            }
            else
            {
                Data_Storage.file_flag = false;
            }
        }
        else
        {
            Data_Storage.it = Data_Storage.stored_files.get(iter2).iterator();
            Data_Storage.file1_temp_tuple = iter1.readOneTuple();
            to_send.tuples.addAll(Data_Storage.file1_temp_tuple.tuples);
            to_send.schema.addAll(Data_Storage.file1_temp_tuple.schema);
            Tuple tup  = (Tuple)Data_Storage.it.next();
            to_send.tuples.addAll(tup.tuples);
            to_send.schema.addAll(tup.schema);
            Data_Storage.file_flag = true;
        }
        return to_send;
    }

    @Override
    public Iterator_Interface getChild() {
        return null;
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
        System.out.println();
    }
}
