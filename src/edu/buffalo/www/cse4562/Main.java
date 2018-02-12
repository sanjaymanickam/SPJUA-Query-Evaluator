package edu.buffalo.www.cse4562;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;

public class Main {
    public static void main(String[] args) {
        // write your code here
        int i;
        File dataDir = null;
        ArrayList<File> sqlFile = new ArrayList<File>();
        for (i = 0; i < args.length; i++) {
            if (args[i].equals("--data")) {
                dataDir = new File(args[i + 1]);
                i++;
            } else {
                sqlFile.add(new File(args[i]));
            }
        }
        for (File sql : sqlFile) {
            try {
                FileReader fil = new FileReader(sql);
                CCJSqlParser parser = new CCJSqlParser(fil);
                Statement stmt;
                while ((stmt = parser.Statement()) != null) {
                    /* -----------------------------                  Using InstanceOf              ---------------------------------------------*/
//                    if (stmt instanceof CreateTable) {
//                        CreateTable ct = (CreateTable) stmt;
//                        tables.put(ct.getTable().getName(), ct);
//                        System.out.println("CreateTable : " + ct.getTable().getName());
//                    }
                    StatementVisitor stmt_visitor = new Visitor_Parse();
                    stmt.accept(stmt_visitor);
                }

            } catch (IOException e) {
                e.printStackTrace();
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }

    }
}
