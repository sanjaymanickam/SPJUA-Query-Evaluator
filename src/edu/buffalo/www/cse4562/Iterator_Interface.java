package edu.buffalo.www.cse4562;

import java.util.ArrayList;

public interface Iterator_Interface {
    ArrayList<String> readOneTuple();
    Iterator_Interface getChild();
    void reset();
}
