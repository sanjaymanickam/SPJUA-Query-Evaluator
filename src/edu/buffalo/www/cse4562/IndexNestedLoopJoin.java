package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;

import java.util.*;

public class IndexNestedLoopJoin implements Iterator_Interface {
    Iterator_Interface iter1,iter2;
    Expression condition;
    LinkedHashMap<String, Schema> outSchema = new LinkedHashMap<>();
    LinkedHashMap<String, Schema> child1Schema = new LinkedHashMap<>();
    LinkedHashMap<String, Schema> child2Schema = new LinkedHashMap<>();
    PrimitiveValue[] to_save;
    PrimitiveValue[] to_send;
    ArrayList<PrimitiveValue[]> tuples = new ArrayList<>();
    Boolean indexAvailable;
    String fileName;
    Column leftColumn,rightColumn;
    int count;
    ArrayList<Expression> expression_list;
    Iterator_Interface second_file;
    public IndexNestedLoopJoin(Iterator_Interface iter1 , Iterator_Interface iter2, Expression condition){
        this.iter1 = iter1;
        this.iter2 = iter2;
        this.condition = condition;
        this.count = 0;
        this.indexAvailable = false;
        this.fileName = "";
        this.expression_list = new ArrayList<>();
        this.leftColumn = (Column)((BinaryExpression)condition).getLeftExpression();
        this.rightColumn = (Column)((BinaryExpression)condition).getRightExpression();
    }

    @Override
    public PrimitiveValue[] readOneTuple() {
        boolean to_flag = false;
        if(iter1 instanceof FileIterator_Interface || iter1 instanceof EvalIterator_Interface) {
            do {
                if (tuples.size() != -1)
                    read_file(iter1);
                if (new ArrayList(Data_Storage.tables.get(iter2.getFileName()).keySet()).contains(leftColumn.getColumnName()))
                    fileName = Data_Storage.get_filename(leftColumn);
                else
                    fileName = Data_Storage.get_filename(rightColumn);
                indexAvailable = fileName == null ? false : true;
                PrimitiveValue[] child1 = null;
                PrimitiveValue[] child2 = null;
                if (count < tuples.size())
                    child1 = tuples.get(count);
                else
                    return null;
                int child1setSize = child1Schema.keySet().size();
                for (int i = 0; i < child1setSize; i++)
                    to_send[i] = child1[i];
                if (indexAvailable) {
                    if (second_file == null)
                        second_file = set_iterator(iter2, fileName + to_send[child1Schema.get(rightColumn.getColumnName()).getPosition()].toRawString());
                } else {
                    if (second_file == null)
                        second_file = iter2;
                }
                child2 = second_file.readOneTuple();
                if (child2 == null) {
                    to_flag = true;
                    this.count++;
                    if(count<tuples.size())
                        second_file = set_iterator(iter2, fileName + tuples.get(count)[child1Schema.get(rightColumn.getColumnName()).getPosition()].toString());
                    else
                        return null;
                }
                if (child2 != null) {
                    to_flag = false;
                    for (int j = 0; j < child2Schema.keySet().size(); j++)
                        to_send[j + child1setSize] = child2[j];
                }
            }while(to_flag);
        }
        return to_send;
    }

    public Iterator_Interface set_iterator(Iterator_Interface iter2,String fileName)
    {
        Iterator_Interface to_ret = null;
        String aliastTableName;
        if(Data_Storage.table_alias.containsKey(fileName))
            aliastTableName = Data_Storage.table_alias.get(fileName);
        else
            aliastTableName = null;
        if(expression_list.size()<=0)
        {
            split_eval(iter2);
        }
        Iterator expr_iter = expression_list.iterator();
        while(expr_iter.hasNext())
        {
            if(to_ret==null)
            {
                to_ret = new EvalIterator_Interface(new FileIterator_Interface(fileName,aliastTableName,false),(Expression)expr_iter.next());
            }
            else
            {
                to_ret = new EvalIterator_Interface(to_ret,(Expression) expr_iter.next());
            }
        }
        if(to_ret == null)
            to_ret = new FileIterator_Interface(fileName,aliastTableName,false);
        to_ret.open();
        return to_ret;
    }
    public void split_eval(Iterator_Interface iter2){
        while(iter2 != null)
        {
            if(iter2 instanceof EvalIterator_Interface) {
                EvalIterator_Interface evalIterator_interface = (EvalIterator_Interface) iter2;
                expression_list.add(evalIterator_interface.condition);
            }
            iter2 = iter2.getChild();
        }
    }
    @Override
    public Iterator_Interface getChild() {
        return null;
    }

    @Override
    public void print() {

    }

    @Override
    public void setChild(Iterator_Interface iter) {

    }

    @Override
    public String getFileName() {
        return null;
    }

    @Override
    public void reset() {

    }

    @Override
    public void open() {
        iter1.open();
        iter2.open();
        this.child1Schema = iter1.getSchema();
        this.child2Schema = iter2.getSchema();
        generateSchema();
    }

    @Override
    public LinkedHashMap<String, Schema> getSchema() {
        return outSchema;
    }
    public void generateSchema(){
        int i =0;
        for(Schema s : this.iter1.getSchema().values()){
            this.outSchema.put(s.getColumnName(), new Schema(s.getTableName(), s.getColumnName(), s.getDataType(),i));
            i++;
        }
        for(Schema s : this.iter2.getSchema().values()){
            this.outSchema.put(s.getColumnName(), new Schema(s.getTableName(), s.getColumnName(), s.getDataType(),i));
            i++;
        }
        this.to_send = new PrimitiveValue[this.outSchema.keySet().size()];
    }
    public void read_file(Iterator_Interface iter1){
        PrimitiveValue[] retArr = iter1.readOneTuple();
        while (retArr != null){
            tuples.add(retArr);
            retArr = iter1.readOneTuple();
        }
    }
}
