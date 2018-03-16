package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;

import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Iterator;

public class Command_Executor {
    static String prompt = "$> ";
        public static void exec(String[] args)
        {
            System.out.println(prompt);
            System.out.flush();
            Reader in = new InputStreamReader(System.in);
            CCJSqlParser parser = new CCJSqlParser(in);
            Statement stmt;
            try {
                while ((stmt = parser.Statement()) != null) {
                    Data_Storage.selectedColumns.clear();
                    Data_Storage.project_columns.clear();
                    Data_Storage.operator_map.clear();
                    Data_Storage.current_schema.clear();
                    Data_Storage.oper = null;
                    Visitor_Parse.ret_type(stmt);
                    if(Data_Storage.oper!=null) {
//                        new Optimize_3().optimize();
                        Tuple tuple = Data_Storage.oper.readOneTuple();
                        do {
                            Iterator it = tuple.tuples.iterator();
                            while (it.hasNext()) {
                                System.out.print(it.next().toString());
                                if (it.hasNext())
                                    System.out.print("|");
                            }
                            tuple = Data_Storage.oper.readOneTuple();
                        } while (tuple != null);
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
