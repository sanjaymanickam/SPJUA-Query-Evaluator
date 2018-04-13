package edu.buffalo.www.cse4562;

import javax.xml.crypto.Data;
import java.util.ArrayList;
import java.util.Iterator;

public class JoinIteratorInterface implements Iterator_Interface{

    public Iterator_Interface iter1,iter2;
    String table1,table2;
    Tuple to_send = new Tuple();
    Tuple temp_tuple = new Tuple();
    Tuple temp_tuple_1 =null;
    Iterator iterator_file1,iterator_file2;
    int flag= 0;
    public JoinIteratorInterface(Iterator_Interface iter1, Iterator_Interface iter2)
    {
        this.iter1 = iter1;
        this.iter2 = iter2;
    }
    @Override
    public Tuple readOneTuple() {
        to_send.schema = new ArrayList<>();
        to_send.tuples = new ArrayList<>();
            if (iter1 instanceof JoinIteratorInterface) {
                if (!Data_Storage.file_temp_tuple.containsKey(iter1)) {
                    Tuple temp = iter1.readOneTuple();
                    if (temp != null) {
                        Data_Storage.temp_tuple = temp;
                        Data_Storage.file_temp_tuple.put(iter1, temp);
                        to_send.tuples.addAll(temp.tuples);
                        to_send.schema.addAll(temp.schema);
                    } else {
                        to_send = null;
                    }
                } else {
                    to_send.tuples.addAll(Data_Storage.file_temp_tuple.get(iter1).tuples);
                    to_send.schema.addAll(Data_Storage.file_temp_tuple.get(iter1).schema);
                }
            } else {
                if (!Data_Storage.file_temp_tuple.containsKey(iter1)) {
                    if (!Data_Storage.stored_files.containsKey(iter1)) {
                        read_file(iter1);
                        iterator_file1 = Data_Storage.stored_files.get(iter1).iterator();
                        Data_Storage.stored_file_iterators.put(iter1, iterator_file1);
                    }
                    iterator_file1 = Data_Storage.stored_file_iterators.get(iter1);
                    if (iterator_file1.hasNext()) {
                        Tuple temp = (Tuple) iterator_file1.next();
                        Data_Storage.file_temp_tuple.put(iter1, new Tuple(new ArrayList<>(temp.tuples),new ArrayList<>(temp.schema)));
                        to_send.tuples.addAll(temp.tuples);
                        to_send.schema.addAll(temp.schema);
                        Data_Storage.stored_file_iterators.replace(iter1, iterator_file1);
                    } else {
                        to_send = null;
                    }
                } else {
                    to_send.tuples.addAll(Data_Storage.file_temp_tuple.get(iter1).tuples);
                    to_send.schema.addAll(Data_Storage.file_temp_tuple.get(iter1).schema);
                }
            }
            do {
                flag=0;
            if (!Data_Storage.stored_files.containsKey(iter2)) {
                read_file(iter2);
                iterator_file2 = Data_Storage.stored_files.get(iter2).iterator();
                Data_Storage.stored_file_iterators.put(iter2, iterator_file2);
            }
            iterator_file2 = Data_Storage.stored_file_iterators.get(iter2);
            if (iterator_file2.hasNext()) {
                Tuple temp = (Tuple) iterator_file2.next();
                if (temp != null && to_send != null) {
                    to_send.tuples.addAll(temp.tuples);
                    to_send.schema.addAll(temp.schema);
                    Data_Storage.stored_file_iterators.replace(iter2, iterator_file2);
                }
            } else {
                Data_Storage.file_temp_tuple.remove(iter1);
                Data_Storage.stored_file_iterators.put(iter2, Data_Storage.stored_files.get(iter2).iterator());
                flag=1;
            }
        }while(flag==1);
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
