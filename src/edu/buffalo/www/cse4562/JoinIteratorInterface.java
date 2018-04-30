package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;

import javax.xml.crypto.Data;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;

public class JoinIteratorInterface implements Iterator_Interface{

    public Iterator_Interface iter1,iter2;
    String table1,table2;
    Tuple to_send = new Tuple();
    //Tuple temp_tuple = new Tuple();
    Tuple temp_tuple_1 =null;
    Iterator iterator_file1,iterator_file2;
    ArrayList<Column> schema;
    int schema_flag;
    int flag= 0;
    LinkedHashMap<String, Schema> retSchema = new LinkedHashMap<>();
    public JoinIteratorInterface(Iterator_Interface iter1, Iterator_Interface iter2)
    {
        this.iter1 = iter1;
        this.iter2 = iter2;
        this.schema = new ArrayList<>();
        this.schema_flag=0;
    }
    @Override
    public void open(){
        iter1.open();
        iter2.open();
        generateSchema();
    }
    @Override
    public PrimitiveValue[] readOneTuple() {
       // return null;
        to_send.schema = new ArrayList<>();
        to_send.tuples = new ArrayList<>();
        PrimitiveValue[] ret = new PrimitiveValue[retSchema.size()];
        do {
            to_send.tuples.clear();
            to_send.schema.clear();
            int i =0;
            if (iter1 instanceof JoinIteratorInterface || iter1 instanceof ProjectionIterator_Interface || iter1 instanceof AggregateProjection) {
                if (!Data_Storage.file_temp_tuple.containsKey(iter1)) {
                    PrimitiveValue[] temp = iter1.readOneTuple();
                    if (temp != null) {
                        //Data_Storage.temp_tuple = temp;
                        Data_Storage.file_temp_tuple.put(iter1, temp);
                        for(int j=0;j<temp.length;j++){
                            ret[i] = temp[j];
                            i++;
                        }
//
//                        if(schema_flag==0) {
//                            to_send.tuples.addAll(temp.tuples);
//                            to_send.schema.addAll(temp.schema);
//                            schema = temp.schema;
//                        }
//                        else
//                        {
//                            to_send.tuples.addAll(temp.tuples);
//                        }
                    } else {
                        ret = null;
                    }
                } else {
                    PrimitiveValue[] temp = Data_Storage.file_temp_tuple.get(iter1);
                    for(int j=0;j<temp.length;j++){
                        ret[i] = temp[j];
                        i++;
                    }
//                        to_send.schema.addAll(Data_Storage.file_temp_tuple.get(iter1).schema);
//                    if(schema_flag==0) {
//                    }
//                    to_send.tuples.addAll(Data_Storage.file_temp_tuple.get(iter1).tuples);
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
                        PrimitiveValue[] temp = (PrimitiveValue [])iterator_file1.next();
                        Data_Storage.file_temp_tuple.put(iter1,temp);
//                        if(temp.schema==null) {
//                            Data_Storage.file_temp_tuple.put(iter1, new Tuple(new ArrayList<>(temp.tuples), null));
//                        }
//                        else {
//                            Data_Storage.file_temp_tuple.put(iter1, new Tuple(new ArrayList<>(temp.tuples), new ArrayList<>(temp.schema)));
//                        }
//                        if(temp.tuples.size() > 0){
//                            to_send.tuples.addAll(temp.tuples);
//                            if(schema_flag==0) {
//                                to_send.schema.addAll(temp.schema);
//                            }
//                        }
                            if(temp.length > 0){
                                for(int j=0;j<temp.length;j++){
                                    ret[i] = temp[j];
                                    i++;
                                }
                            }

                        Data_Storage.stored_file_iterators.replace(iter1, iterator_file1);
                    } else {
                        ret = null;
                    }
                } else {
//                    if(schema_flag==0)
//                    {
//                        to_send.schema.addAll(Data_Storage.file_temp_tuple.get(iter1).schema);
//                    }
//                    to_send.tuples.addAll(Data_Storage.file_temp_tuple.get(iter1).tuples);

                    PrimitiveValue[] temp = Data_Storage.file_temp_tuple.get(iter1);
                    for(int j=0;j<temp.length;j++){
                        ret[i] = temp[j];
                        i++;
                    }
                }
            }
            flag=0;
            if (!Data_Storage.stored_files.containsKey(iter2)) {
                read_file(iter2);
                iterator_file2 = Data_Storage.stored_files.get(iter2).iterator();
                Data_Storage.stored_file_iterators.put(iter2, iterator_file2);
            }
            iterator_file2 = Data_Storage.stored_file_iterators.get(iter2);
            if (iterator_file2.hasNext()) {
                PrimitiveValue[] temp = (PrimitiveValue[]) iterator_file2.next();
                if (temp != null && ret != null) {
//                    if(temp.tuples.size() > 0){
//                        to_send.tuples.addAll(temp.tuples);
//                        if(schema_flag==0) {
//                            to_send.schema.addAll(temp.schema);
//                            schema_flag=1;
//                        }
//                    }
                    for(int j=0;j<temp.length;j++){
                        ret[i] = temp[j];
                        i++;
                    }
                    Data_Storage.stored_file_iterators.replace(iter2, iterator_file2);
                }

            } else {
                Data_Storage.file_temp_tuple.remove(iter1);
                Data_Storage.stored_file_iterators.put(iter2, Data_Storage.stored_files.get(iter2).iterator());
                flag=1;
            }
        }while(flag==1);
        return ret;
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
    @Override
    public void print()
    {
        System.out.println(iter1.toString()+" "+iter2.toString());

    }
    void read_file(Iterator_Interface file){
        ArrayList<PrimitiveValue[]> temp_array = new ArrayList<>();
        PrimitiveValue[] temp_tuple = new PrimitiveValue[file.getSchema().size()];
        do
        {
            temp_tuple = file.readOneTuple();
            if(temp_tuple!=null)
                temp_array.add(temp_tuple);
        }while(temp_tuple!= null);
        Data_Storage.stored_files.put(file,temp_array);
    }

    @Override
    public LinkedHashMap<String, Schema> getSchema(){
        return this.retSchema;
    }
    public void generateSchema(){
        int i = 0;
        for(Schema s : this.iter1.getSchema().values()){
            retSchema.put(s.getColumnName(), new Schema(s.getTableName(), s.getColumnName(), s.getDataType(),i));
            i++;
        }
        for(Schema s : this.iter2.getSchema().values()){
            retSchema.put(s.getColumnName(), new Schema(s.getTableName(), s.getColumnName(), s.getDataType(),i));
            i++;
        }
        System.out.println("Done check");
    }
}
