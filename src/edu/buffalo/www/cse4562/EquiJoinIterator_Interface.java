package edu.buffalo.www.cse4562;


import net.sf.jsqlparser.expression.Expression;

public class
EquiJoinIterator_Interface implements Iterator_Interface{

    Iterator_Interface fileName1,fileName2;
    Expression condition;
    public EquiJoinIterator_Interface(Iterator_Interface fileName1,Iterator_Interface fileName2)
    {
        this.fileName1 = fileName1;
        this.fileName2 = fileName2;
    }
    @Override
    public Tuple readOneTuple() {
        Tuple tup1 = fileName1.readOneTuple();
        Tuple tup2 = fileName2.readOneTuple();
        Tuple to_send = tup1;
        to_send.schema.addAll(tup2.schema);
        to_send.tuples.addAll(tup2.tuples);
        return to_send;
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
