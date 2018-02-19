package edu.buffalo.www.cse4562;

import java.io.*;

public class File_Iterator implements Iterator {

    BufferedReader read;
    File new_file;

    public File_Iterator(File new_file) {
        this.new_file = new_file;
        try {
            read = new BufferedReader(new FileReader(new_file));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String[] readOneTuple() {
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
        String str_split[] = new_line.split("\\|");
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
