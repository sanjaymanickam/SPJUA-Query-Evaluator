package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.schema.Column;

import java.util.ArrayList;

public class Tuple {
    public ArrayList<String> tuples;
    public ArrayList<Column> schema;
    public Tuple(){ this.tuples = new ArrayList<>();
                    this.schema = new ArrayList<>();};
    public Tuple(ArrayList<String> tuples,ArrayList<Column> schema)
    {
        this.schema = schema;
        this.tuples = tuples;
    }
    public void copy()
    {

    }
}
