package edu.buffalo.www.cse4562;

import java.io.File;
import java.io.FileInputStream;

public class Main {
    public static void main(String[] args) {
        try {
            //FileInputStream is = new FileInputStream(new File("1.txt"));
            //System.setIn(is);
            Command_Executor.exec(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
