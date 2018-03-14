package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;

import java.io.InputStreamReader;
import java.io.Reader;

public class Command_Executor {
    static String prompt = "$> ";
        public static void exec(String[] args)
        {
            System.out.println(prompt);
            System.out.flush();
            Reader in = new InputStreamReader(System.in);
            CCJSqlParser parser = new CCJSqlParser(in);
            Statement stmt;
            try
            {
                while((stmt = parser.Statement())!=null) {
                    Data_Storage.selectedColumns.clear();
                    Data_Storage.project_columns.clear();
                    Visitor_Parse.ret_type(stmt);
                    new Optimize().optimize();
                    if(Data_Storage.oper!=null)
                    {
                        Data_Storage.oper.readOneTuple();
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
}
