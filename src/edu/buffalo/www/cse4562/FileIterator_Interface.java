package edu.buffalo.www.cse4562;

import java.io.*;
import java.util.ArrayList;
import java.util.StringTokenizer;

public class FileIterator_Interface implements Iterator_Interface{
    BufferedReader read;
    File new_file;

    public FileIterator_Interface(File new_file) {
        this.new_file = new_file;
        try {
            read = new BufferedReader(new FileReader(new_file));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public ArrayList<String> readOneTuple() {
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
        return str_split;

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
