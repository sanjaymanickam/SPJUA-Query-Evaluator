package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitor;

import java.io.*;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class Command_Executor {
     static String prompt = "$> "; // expected prompt
        public static void exec(String[] argsArray) throws Exception {
                // ready to read stdin, print out prompt
                System.out.println(prompt);
                System.out.flush();
                try
                {
                    Reader in = new InputStreamReader(System.in);
                    CCJSqlParser parser = new CCJSqlParser(in);
                    Statement stmt;
                    // project here
                    while((stmt = parser.Statement()) != null){
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
