package edu.buffalo.www.cse4562;
import java.util.ArrayList;
import java.util.Arrays;

import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;

public class SelectItems implements SelectItemVisitor{

	@Override
	public void visit(AllColumns allCoumns) {
		// TODO Auto-generated method stub
		Utility.selectedColumns = new ArrayList(Arrays.asList(Utility.tableColumns.get(Utility.tableName)));
		System.out.println("Select all columns");
		//Code if have to select all columns
		
	}

	@Override
	public void visit(AllTableColumns allTableColumns) {
		// TODO Auto-generated method stub
		System.out.println("This");
		System.out.println("2");
		
	}

	@Override
	public void visit(SelectExpressionItem selectExpression) {
		// TODO Auto-generated method stub
		
		String columnName = selectExpression.getExpression().toString();
		if(columnName.indexOf(".") !=-1 ) {
			columnName = columnName.split("\\.")[1];
		}
		Utility.selectedColumns.add(columnName);
		
		if(selectExpression.getAlias() != null) {
			System.out.println("Alias is "+ selectExpression.getAlias());
		}
		
	}

}
