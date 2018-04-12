package edu.buffalo.www.cse4562;


import net.sf.jsqlparser.expression.Expression;

public class
EquiJoinIterator_Interface implements Iterator_Interface{

    Iterator_Interface fileName1,fileName2;
    Expression condition;
    public EquiJoinIterator_Interface(Iterator_Interface fileName1,Iterator_Interface fileName2,Expression condition)
    {
        this.fileName1 = fileName1;
        this.fileName2 = fileName2;
        this.condition = condition;
    }
    @Override
    public Tuple readOneTuple() {
        Tuple tup1 = fileName1.readOneTuple();
        Tuple tup2 = fileName2.readOneTuple();
        Tuple to_send = tup1;
        to_send.schema.addAll(tup2.schema);
        to_send.tuples.addAll(tup2.tuples);
        final Tuple to_send_final = to_send;
        EvalIterator_Interface evalIterator_interface = new EvalIterator_Interface(new Iterator_Interface() {
            @Override
            public Tuple readOneTuple() {
                return to_send_final;
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
        },condition);
        to_send = evalIterator_interface.readOneTuple();
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
