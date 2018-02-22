package edu.buffalo.www.cse4562;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Data_Storage {
    static HashMap<String, HashMap<String, String>> tables = new HashMap<>();
    static HashMap<String, ArrayList<String>> tableColumns = new HashMap<>();
    static List<String> selectedColumns = new ArrayList<>();
    static String dataDir = null;
    static String tablename = null;
    static int star_flag = 0;
    static Iterator_Inteface oper;
}
