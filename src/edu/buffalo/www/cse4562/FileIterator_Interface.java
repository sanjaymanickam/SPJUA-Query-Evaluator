package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

import java.io.*;
import java.util.*;

public class FileIterator_Interface implements Iterator_Interface{
    BufferedReader read;
    String new_file;
    String aliastableName;
    HashMap<String, ArrayList<Column>> schemaMap = new HashMap<>();
    public FileIterator_Interface(String new_file,String aliastableName) {
        this.new_file = new_file;
        this.aliastableName = aliastableName;
        String file = "data/"+new_file+".dat";
        try {
            read = new BufferedReader(new FileReader(new File(file)));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Tuple readOneTuple() {
        ArrayList<Column> schema = new ArrayList<>();
        if (read == null) {
            return null;
        }
        String new_line = null;
        try {
            new_line = read.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (new_line == null) {
            return null;
        }

        ArrayList<String> str_split =new ArrayList<>();
        StringTokenizer str_tok = new StringTokenizer(new_line,"|");
        while(str_tok.hasMoreElements())
        {
            str_split.add(str_tok.nextElement().toString());
        }
        Iterator it = Data_Storage.tables.get(new_file).keySet().iterator();
        Iterator iter_string = str_split.iterator();
        String col_name;
        ArrayList<String> to_send = new ArrayList<>();
//        if(Data_Storage.table_alias.containsValue(new_file))
//        {
//
//          Set set = Data_Storage.table_alias.entrySet();
//          Iterator set_iterator  = set.iterator();
//          while(set_iterator.hasNext())
//          {
//              Map.Entry entry =(Map.Entry) set_iterator.next();
//              if(entry.getValue().equals(new_file))
//                  new_file = entry.getKey().toString();
//          }
//        }
        if(aliastableName==null)
            aliastableName = new_file;
        while(it.hasNext())
        {
            col_name = it.next().toString();
            if(Data_Storage.join == 1) {
                if (Data_Storage.project_array.contains(col_name)) {
                    Column col = new Column(new Table(aliastableName), col_name);
                    schema.add(col);
                    to_send.add(iter_string.next().toString());
                } else {
                    iter_string.next();
                }
            }
            else
            {
                Column col = new Column(new Table(aliastableName), col_name);
                schema.add(col);
                to_send.add(iter_string.next().toString());
            }
        }
        return new Tuple(to_send,schema);
    }

    @Override
    public void reset() {
        try {
            read.reset();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    @Override
    public Iterator_Interface getChild(){
        return null;
    }

    @Override
    public void setChild(Iterator_Interface iter) {

    }
}
