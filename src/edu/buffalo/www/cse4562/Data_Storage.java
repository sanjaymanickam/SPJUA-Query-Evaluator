package edu.buffalo.www.cse4562;

import java.util.*;

public class Data_Storage {
    static HashMap<String,LinkedHashMap<String,String>> tables = new HashMap<>();
    static HashMap<String,String> alias_table = new HashMap<>();
    static LinkedHashMap<String,String> selectedColumns = new LinkedHashMap<>();
    static HashMap<String,String> current_schema = new HashMap<>();
    static Iterator_Interface oper = null;
    static boolean file_flag = false;
    static int all_flag = 0;
    static Iterator it;
    static Tuple file1_temp_tuple;
    static HashMap<Iterator_Interface,ArrayList<Tuple>> stored_files = new HashMap<>();
    static HashMap<String , Iterator_Interface> operator_map = new HashMap<>();
    static HashMap<String, ArrayList<String>> project_columns = new HashMap<>();
}
