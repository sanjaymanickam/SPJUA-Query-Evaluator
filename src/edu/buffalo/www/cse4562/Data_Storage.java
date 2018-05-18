package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

import java.util.*;

public class Data_Storage {
    static HashMap<String,LinkedHashMap<String,String>> tables = new HashMap<>();
    static HashMap<String,String> alias_table = new HashMap<>();
    static HashMap<String,String> table_alias = new HashMap<>();
    static ArrayList<Column> selectedColumns = new ArrayList<>();
    static HashMap<String,String> columns_needed_for_aggregate = new HashMap<>();
    static HashMap<String,String> current_schema = new HashMap<>();
    static Iterator_Interface oper = null;
    static HashMap<Iterator_Interface,Boolean> file_flag = new HashMap<>();
    static int all_flag = 0;
    static HashMap<Iterator_Interface,Iterator> stored_file_iterators = new HashMap<>();
    static HashMap<Iterator_Interface,PrimitiveValue[]> file_temp_tuple = new HashMap<>();
    static HashMap<Iterator_Interface,ArrayList<PrimitiveValue[]>> stored_files = new HashMap<>();
    static HashMap<String , Iterator_Interface> operator_map = new HashMap<>();
    static HashMap<String,Integer> table_sizes = new HashMap<>();
    static Long limit;
    static Eval eval;
    static int hash_flag = 0;
    static ArrayList<Column> orderBy;
    static ArrayList<String> orderBy_sort;
    static String from_alias = null;
    static int aggregateflag = 0;
    static int groupbyflag = 0;
    static LinkedHashMap<String, ArrayList<ArrayList<String>>> groupby_resultset;
    static int join = 0;
    static Tuple temp_tuple = new Tuple();
    static List<Column> groupByColumn;
    static List<Function> aggregate_operations = new ArrayList<>();
    static ArrayList<String> project_array = new ArrayList<>();
    static ArrayList<Column> aggregate = new ArrayList<>();
    static ArrayList<Function> aggregateFunctions = new ArrayList<>();
    static ArrayList<Column> projectionColumns = new ArrayList<>();
    static ArrayList<SelectExpressionItem> finalColumns = new ArrayList<>();
    static ArrayList<Column> finalSchema = new ArrayList<>();
    static int read_tuple = 0;
    static HashMap<Column,String> dataTypeHash = new HashMap<>();
    static HashMap<Column,Integer> positionHash = new HashMap<>();
    static LinkedHashMap<String,ArrayList<Double []>> aggregateHash = new LinkedHashMap<>();
    static HashMap<Column,String> valHash = new HashMap<>();

    static HashMap<String, ColumnType> colType = new HashMap<>();
    static HashMap<String, Integer> tableSize = new HashMap<>();
    static HashSet<String> projectionCols = new HashSet<>();
    public static int selfJoin = 0;

    static HashMap<String, ArrayList<String>> fKeyNames = new HashMap<>();
    static HashMap<String, ArrayList<Integer>> fKeyPositions = new HashMap<>();
    static HashSet<String> indexColumns = new HashSet<>();
    static Column stringSplitter(String colName)
    {
        String tableName;
        StringTokenizer str_tok = new StringTokenizer(colName, ".");
        tableName = str_tok.nextElement().toString();
        colName = str_tok.nextElement().toString();
        return new Column(new Table(tableName),colName);
    }
    static String get_filename(Column rightColumn){
        String fileName;
        if(Data_Storage.indexColumns.contains(rightColumn.getColumnName())){
            String tableName = rightColumn.getTable().getName();
            String colName = rightColumn.getColumnName();
            if(Data_Storage.table_alias.containsKey(tableName)){
                tableName = Data_Storage.table_alias.get(tableName);
            }
            if(tableName == null) {
                ArrayList<String> names = new ArrayList(Data_Storage.tables.keySet());
                String table_name1 = null;
                Iterator iter = Data_Storage.tables.keySet().iterator();
                for (int i = 0; i < Data_Storage.tables.size(); i++) {
                    if (iter.hasNext()) {
                        if (Data_Storage.tables.get(iter.next()).containsKey(colName))
                            table_name1 = names.get(i);
                    }
                }
                tableName = table_name1;
            }
            fileName = tableName+"_"+colName+"_";
        }
        else
        {
            fileName = null;
        }
        return fileName;
    }
}
