package edu.buffalo.www.cse4562;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;

public class File_IteratorInteface implements Iterator_Inteface {

    BufferedReader read;
    File new_file;

    public File_IteratorInteface(File new_file) {
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
        ArrayList<String> str_split =new ArrayList<>(Arrays.asList(new_line.split("\\|")));
        return str_split;
    }

    @Override
    public void reset() {
        try {
            read.reset();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
