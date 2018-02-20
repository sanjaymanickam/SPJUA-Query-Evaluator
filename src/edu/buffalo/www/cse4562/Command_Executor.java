package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;

import java.io.*;

public class Command_Executor {
     static String prompt = "$> "; // expected prompt
        public static void exec(String[] argsArray){
                // ready to read stdin, print out prompt
                Data_Storage.dataDir = "data";
                Statement stmt;
                System.out.println(prompt);
                System.out.flush();
                Reader in = new InputStreamReader(System.in);
                CCJSqlParser parser = new CCJSqlParser(in);
                try
                {

                    // project here
                    while((stmt = parser.Statement()) != null){
                        System.out.println(stmt+" ");
                    Visitor_Parse stmt_visitor = new Visitor_Parse();
                    stmt.accept(stmt_visitor);
                    System.out.println(prompt);
                    System.out.flush();
                    }
                }
                catch (ParseException e) {
                e.printStackTrace();
                }
        }
}
