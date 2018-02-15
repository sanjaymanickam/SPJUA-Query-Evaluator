package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.schema.Column;

public class SelectionOperator implements RelationalOperator{
	
	RelationalOperator input;
	Column[] schema;
	Expression condition;
	
	public SelectionOperator(RelationalOperator input, Column[] schema, Expression condition) {
		// TODO Auto-generated constructor stub
		this.input = input;
		this.schema = schema;
		this.condition = condition;
		
	}

	@Override
	public String[] readTuple() {
		// TODO Auto-generated method stub
		String[] tuple = null;
		do {
			//System.out.println("Calling read tuple from Selection Operator");
			tuple = input.readTuple();
			if(tuple == null) {return null;}
			//Evaluator eval = new Evaluator();
			//System.out.println(tuple[0]);
			if(Integer.parseInt(tuple[0]) < 4) {
				tuple = null;
			}
		}while(tuple == null);
		return tuple;
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
		
	}

}
