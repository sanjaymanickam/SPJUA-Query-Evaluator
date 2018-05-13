package edu.buffalo.www.cse4562;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Iterator;

public class Statistics_Collection {
    Iterator file_name_iter = Data_Storage.tables.keySet().iterator();
    public void collect() {
        try {
            while (file_name_iter.hasNext()) {
                String filename = file_name_iter.next().toString();
                BufferedReader bufferedReader = new BufferedReader(new FileReader(new File("data/"+filename+".dat")));
                int count=0;
                while(bufferedReader.readLine()!=null)
                    count++;
                Data_Storage.table_sizes.put(filename,count);
            }
        } catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
