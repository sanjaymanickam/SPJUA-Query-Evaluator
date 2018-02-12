package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.replace.Replace;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.delete.*;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.*;
import net.sf.jsqlparser.statement.insert.*;
import net.sf.jsqlparser.statement.drop.*;

import java.util.HashMap;

public class Visitor_Parse implements StatementVisitor {
    HashMap<String, CreateTable> tables = new HashMap<>();
    @Override
    public void visit(Select select) {
        SelectVisitor s_visit = new Select_Parse();
        select.getSelectBody().accept(s_visit);
    }

    @Override
    public void visit(Delete delete) {
    }

    @Override
    public void visit(Update update) {
    }

    @Override
    public void visit(Insert insert) {
    }

    @Override
    public void visit(Replace replace) {
    }

    @Override
    public void visit(Drop drop) {
    }

    @Override
    public void visit(Truncate trunc) {
    }

    @Override
    public void visit(CreateTable createTable) {
        System.out.println("Table name from visitor is :" + createTable.getTable().getName());
        tables.put(createTable.getTable().getName(), createTable);
    }

    public HashMap retTable() {
        return tables;
    }
}

