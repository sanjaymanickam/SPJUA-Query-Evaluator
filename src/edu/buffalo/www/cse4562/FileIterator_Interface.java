package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;

public class FileIterator_Interface implements Iterator_Interface{
    BufferedReader read;
    String new_file;
    ArrayList<Column> schema = new ArrayList<>();
    public FileIterator_Interface(String new_file) {
        this.new_file = new_file;
        String file = "data/"+new_file+".dat";
        try {
            read = new BufferedReader(new FileReader(new File(file)));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Tuple readOneTuple() {
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
        System.out.println("FILE");
        Iterator it = Data_Storage.tables.get(new_file).keySet().iterator();
        while(it.hasNext())
        {
            Column col = new Column(new Table(new_file),it.next().toString());
            schema.add(col);
        }
        return new Tuple(str_split,schema);
//        return null;
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
}
