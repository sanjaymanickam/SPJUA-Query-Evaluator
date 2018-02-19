package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.StatementVisitor;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class Command_Executor {
    public static void exec(String[] args) {
        int i;
        ArrayList<File> sqlFile = new ArrayList<>();
        for (i = 0; i < args.length; i++) {
            if (args[i].equals("--data")) {
                Data_Storage.dataDir = new File(args[i + 1]);
                i++;
            } else
                sqlFile.add(new File(args[i]));
        }
        for (File sql : sqlFile) {
            try {
                FileReader fil = new FileReader(sql);
                CCJSqlParser parser = new CCJSqlParser(fil);
                Statement stmt;
                while ((stmt = parser.Statement()) != null) {
                    System.out.println("$ > :" + stmt);
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
