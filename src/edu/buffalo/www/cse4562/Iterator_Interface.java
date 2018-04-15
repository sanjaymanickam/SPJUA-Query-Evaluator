package edu.buffalo.www.cse4562;


import net.sf.jsqlparser.schema.Column;

import java.util.ArrayList;

public interface Iterator_Interface {
    Tuple readOneTuple();
    Iterator_Interface getChild();
    void print();
    void setChild(Iterator_Interface iter);
    void reset();
}
