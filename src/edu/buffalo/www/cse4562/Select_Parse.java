package edu.buffalo.www.cse4562;


//import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.*;

import java.io.File;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

import javax.lang.model.type.PrimitiveType;

public class Select_Parse implements SelectVisitor {
    @Override
    public void visit(PlainSelect plainSelect) {
        List l = plainSelect.getSelectItems();
        Iterator t = l.iterator();
        while (t.hasNext()) {
            System.out.println("ColumnName : "+t.next() + " ");
        }
//        Expression e = plainSelect.getWhere();
//        System.out.println(e.toString());
         
        FromItemVisitor from_Item = new From_Parse();
        plainSelect.getFromItem().accept(from_Item);
        
        
        
        SelectItemVisitor select_Item = new SelectItems();
        List<SelectItem> columns = plainSelect.getSelectItems();
        System.out.println("Select count");
        System.out.println(Utility.selectedColumns.size());
        Utility.selectedColumns.clear();
        for(SelectItem col : columns) {
        		col.accept(select_Item);
        }
        
        
        
        if(plainSelect.getWhere() != null) {
        		Evaluator evalu = new Evaluator();
        		try {
        			PrimitiveValue result = evalu.eval(plainSelect.getWhere());
        			System.out.println("Result");
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
        }
        //System.out.println("Done parsing select");
        RelationalOperator oper = new ScanOperator(new File("data/R.csv"));
        MinorThan cmp = new MinorThan();
        cmp.setLeftExpression(new Column(null,"A"));
        cmp.setRightExpression(new Column(null,"B"));
        
        oper = new SelectionOperator(oper, new Column[] {new Column(null,"A"), new Column(null,"B")}, cmp);
        //System.out.println("Calling read tuple from Select_Parse");
        String cols[] = oper.readTuple();
        while(cols != null) {
        	for(int i=0;i<cols.length;i++) {
        		if(Utility.selectedColumns.contains(Utility.tableColumns.get(Utility.tableName)[i])) {
        			System.out.print(cols[i] + " | ");
        		}
        }
        System.out.println();
        cols = oper.readTuple();
        }
    }

    @Override
    public void visit(Union union) {

    }
}
