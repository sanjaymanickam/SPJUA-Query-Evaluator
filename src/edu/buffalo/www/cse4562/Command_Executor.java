package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Select;

import javax.xml.crypto.Data;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.*;
import java.util.concurrent.atomic.DoubleAccumulator;

public class Command_Executor {
    static String prompt = "$> ";
    static LinkedHashMap<String,ArrayList<String>> aggregate_result = new LinkedHashMap<>();
    public static void exec(String[] args)
    {
        System.out.println(prompt);
        System.out.flush();
        Reader in = new InputStreamReader(System.in);
        CCJSqlParser parser = new CCJSqlParser(in);
        Statement stmt;
        try {
            int createTableCount = 0;
            while ((stmt = parser.Statement()) != null) {
                ArrayList<ArrayList<String>> result = new ArrayList<>();
                ArrayList<Column> schema = new ArrayList<>();
                Data_Storage.selectedColumns.clear();
                Data_Storage.operator_map.clear();
                Data_Storage.current_schema.clear();
                Data_Storage.oper = null;
                Data_Storage.limit = Long.parseLong("0");
                Data_Storage.orderBy = null;
                Data_Storage.groupbyflag = 0;
                Data_Storage.aggregateflag = 0;
                Data_Storage.finalColumns.clear();
                Data_Storage.hash_flag = 0;
                Data_Storage.projectionColumns.clear();
                Data_Storage.finalSchema.clear();
                Data_Storage.aggregate_operations.clear();
                Data_Storage.positionHash.clear();
                Data_Storage.dataTypeHash.clear();
                Data_Storage.projectionCols.clear();
                Data_Storage.selfJoin = 0;
                Data_Storage.colType.clear();
                int schema_flag=0;
                Visitor_Parse.ret_type(stmt);
                boolean flag_temp = false;
                if(stmt instanceof CreateTable){
                    createTableCount++;
                }
                else
                {
                    flag_temp = true;
                    System.err.println(createTableCount);
                }
                if(flag_temp){
                    Preprocess.preprocessData();
                }
                if(Data_Storage.oper!=null) {
                    if(Data_Storage.join ==1) {
                        Iterator_Interface iter = new Optimize().optimize(Data_Storage.oper);
                        Data_Storage.oper = iter;
                    }
                    System.out.println(stmt.toString());
                    Data_Storage.oper.open();
                    Set<String> temp_set = new HashSet<>();
                    temp_set.addAll(Data_Storage.project_array);
                    Data_Storage.project_array.clear();
                    Data_Storage.project_array.addAll(temp_set);
                    ArrayList<PrimitiveValue[]> resultTuples = new ArrayList<>();
                    PrimitiveValue[] tuple = Data_Storage.oper.readOneTuple();
                    int count = 1;
                    while(tuple != null){
                        if(tuple != null){
                            resultTuples.add(tuple);
                            print(tuple);
                            if(count >= Data_Storage.limit){
                                break;
                            }
                        }
                        count++;
                        tuple = Data_Storage.oper.readOneTuple();
                    }
                }

                System.out.println(prompt);
                System.out.flush();
            }
        }
        catch(Exception e)
        {
            e.printStackTrace();    
        }
    }
    static void print(PrimitiveValue[] arr){
        for(int i=0;i<arr.length;i++){
            System.out.print(arr[i]);
            if(i != arr.length -1){
                System.out.print("|");
            }
        }
        System.out.println();
    }
}