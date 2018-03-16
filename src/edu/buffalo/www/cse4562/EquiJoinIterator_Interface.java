package edu.buffalo.www.cse4562;


import net.sf.jsqlparser.expression.Expression;

public class EquiJoinIterator_Interface implements Iterator_Interface{

    String fileName1,fileName2;
    Expression condition;
    public EquiJoinIterator_Interface(String fileName1,String fileName2,Expression condition)
    {
        this.fileName1 = fileName1;
        this.fileName2 = fileName2;
        this.condition = condition;
        System.out.println("EQUIJOIN");
    }
    @Override
    public Tuple readOneTuple() {
        System.out.println("IN READ ONE TUPLE OF EQUIJOIN");
        return null;
    }

    @Override
    public Iterator_Interface getChild() {
        return null;
    }

    @Override
    public void setChild(Iterator_Interface iter) {

    }

    @Override
    public void reset() {

    }
}
