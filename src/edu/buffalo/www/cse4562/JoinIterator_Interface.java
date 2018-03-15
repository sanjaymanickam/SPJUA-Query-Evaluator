package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.statement.select.FromItem;
import java.util.ArrayList;
import java.util.Iterator;

public class JoinIterator_Interface implements Iterator_Interface{
    @Override
    public Tuple readOneTuple() {
        return null;
    }

    @Override
    public Iterator_Interface getChild() {
        return null;
    }

    @Override
    public void reset() {

    }
//    Iterator_Interface iter;
//    FromItem fromItem;
//    public JoinIterator_Interface(FromItem fromItem, Iterator_Interface iter)
//    {
//        this.iter = iter;
//        this.fromItem = fromItem;
//    }
//    @Override
//    public Tuple readOneTuple() {
//        Iterator iter = Data_Storage.operator_map.values().iterator();
//        Tuple to_send = new Tuple();
//        Tuple tup1;
//        to_send.schema = new ArrayList<>();
//        to_send.tuples = new ArrayList<>();
////        while(it.hasNext())
////        {
////            Iterator_Interface iterator =(Iterator_Interface) it.next();
////            Iterator_Interface temp_iter = iterator;
////            Tuple temp_tuple = iterator.readOneTuple();
////            if(temp_tuple.tuples==null)
////            {
////                while(temp_iter!=null)
////                {
////                    if(temp_iter.getChild() instanceof FileIterator_Interface)
////                    {
////                        temp_iter.getChild().reset();
////                    }
////                    temp_iter = temp_iter.getChild();
////                }
////            }
////            to_send.tuples.addAll(temp_tuple.tuples);
////            to_send.schema.addAll(temp_tuple.schema);
////        }
////        System.out.println(" JOIN ");
////        return to_send;
//        Iterator_Interface file1 = (Iterator_Interface) iter.next();
//        Iterator_Interface file2 = (Iterator_Interface) iter.next();
//        if(!Data_Storage.file_flag) {
//            Data_Storage.temp_file.clear();
//            read_file(file2);
//        }
//        if(Data_Storage.file_flag)
//        {
//                to_send.tuples.addAll(Data_Storage.file1_temp_tuple.tuples);
//                to_send.schema.addAll(Data_Storage.file1_temp_tuple.schema);
//                tup1 = (Tuple)Data_Storage.it.next();
//                if(tup1!=null) {
//                    to_send.tuples.addAll(tup1.tuples);
//                    to_send.schema.addAll(tup1.schema);
//                }
//                else
//                    Data_Storage.file_flag = false;
//        }
//        else
//        {
//            Data_Storage.it = Data_Storage.temp_file.iterator();
//            Data_Storage.file1_temp_tuple = file1.readOneTuple();
//            Tuple tup = (Tuple) Data_Storage.it.next();
//            Data_Storage.file1_temp_tuple.tuples.addAll(tup.tuples);
//            Data_Storage.file1_temp_tuple.schema.addAll(tup.schema);
//            to_send.tuples.addAll(Data_Storage.file1_temp_tuple.tuples);
//            to_send.schema.addAll(Data_Storage.file1_temp_tuple.schema);
//            Data_Storage.file_flag = true;
//        }
//        return to_send;
//    }
//
//    @Override
//    public Iterator_Interface getChild() {
//        return iter;
//    }
//
//    @Override
//    public void reset() {
//
//    }
//
//    public void read_file(Iterator_Interface file)
//    {
//        String file_name = "";
//        while(file!=null)
//        {
//            if(file.getChild() instanceof FileIterator_Interface)
//            {
//                file_name = ((FileIterator_Interface) file.getChild()).new_file;
//            }
//            file = file.getChild();
//        }
//        try {
//            FileIterator_Interface fread = new FileIterator_Interface(file_name);
//            Tuple temp_line = fread.readOneTuple();
//            while(temp_line!= null)
//            {
//                Data_Storage.temp_file.add(temp_line);
//                temp_line = fread.readOneTuple();
//            }
//            System.out.println("");
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
}
