package edu.buffalo.www.cse4562;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.PrimitiveType;

public class Main {
    public static void main(String[] args) {
        // write your code here
        Command_Executor.exec(args);
    }
}
