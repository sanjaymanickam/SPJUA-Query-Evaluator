package edu.buffalo.www.cse4562;


import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;

import java.util.ArrayList;
import java.util.LinkedHashMap;

public interface Iterator_Interface  {
    PrimitiveValue[] readOneTuple();
    Iterator_Interface getChild();
    void print();
    void setChild(Iterator_Interface iter);
    void reset();
    void open();
    LinkedHashMap<String, Schema> getSchema();
}
