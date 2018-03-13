package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.statement.select.FromItem;
import java.util.Iterator;

public class JoinIterator_Interface implements Iterator_Interface{
    Iterator_Interface iter;
    FromItem fromItem;
    public JoinIterator_Interface(FromItem fromItem, Iterator_Interface iter)
    {
        this.iter = iter;
        this.fromItem = fromItem;
    }
    @Override
    public Tuple readOneTuple() {
        Iterator it = Data_Storage.operator_map.values().iterator();
        Tuple tuple;
        while(it.hasNext())
        {
            Iterator_Interface iterator =(Iterator_Interface) it.next();
            tuple = iterator.readOneTuple();
            Iterator iter = tuple.tuples.iterator();
            while(iter.hasNext()) {
                System.out.println(iter.next());
            }
        }
        System.out.println(" JOIN ");
        return null;
    }

    @Override
    public Iterator_Interface getChild() {
        return iter;
    }

    @Override
    public void reset() {

    }
}
