package edu.buffalo.www.cse4562;

import java.util.*;

public class Data_Storage {
    static HashMap<String,LinkedHashMap<String,String>> tables = new HashMap<>();
    static HashMap<String,String> alias_table = new HashMap<>();
    static LinkedHashMap<String,String> selectedColumns = new LinkedHashMap<>();
    static HashMap<String,String> current_schema = new HashMap<>();
    static Iterator_Interface oper = null;
    static List<String> second_file = new ArrayList<>();
    static int join_flag = 0;
    static int all_flag = 0;
    static Iterator it;
    static ArrayList<String> curr_tuple;
    static List<Iterator_Interface> from_list;
    static HashMap<String , Iterator_Interface> operator_map = new HashMap<>();
    static HashMap<String, ArrayList<String>> project_columns = new HashMap<>();
}
