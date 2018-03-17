package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.schema.Column;

import java.util.*;

public class Data_Storage {
    static HashMap<String,LinkedHashMap<String,String>> tables = new HashMap<>();
    static HashMap<String,String> alias_table = new HashMap<>();
    static HashMap<String,String> table_alias = new HashMap<>();
    static LinkedHashMap<String,String> selectedColumns = new LinkedHashMap<>();
    static HashMap<String,String> current_schema = new HashMap<>();
    static Iterator_Interface oper = null;
    static HashMap<Iterator_Interface,Boolean> file_flag = new HashMap<>();
    static int all_flag = 0;
    static HashMap<Iterator_Interface,Iterator> stored_file_iterators = new HashMap<>();
    static HashMap<Iterator_Interface,Tuple> file_temp_tuple = new HashMap<>();
    static HashMap<Iterator_Interface,ArrayList<Tuple>> stored_files = new HashMap<>();
    static HashMap<String , Iterator_Interface> operator_map = new HashMap<>();
    static HashMap<String, ArrayList<String>> project_columns = new HashMap<>();
    static Long limit;
    static ArrayList<Column> orderBy;
    static ArrayList<String> orderBy_sort;
    static int join = 0;
}
