package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;

import java.util.ArrayList;
import java.util.Arrays;

public class SelectItem_Visitor implements SelectItemVisitor {
    @Override
    public void visit(AllColumns allColumns) {
//        Data_Storage.selectedColumns = new ArrayList<>(Arrays.asList(Data_Storage.tableColumns.get(Data_Storage.tablename)));
    }

    @Override
    public void visit(AllTableColumns allTableColumns) {

    }

    @Override
    public void visit(SelectExpressionItem selectExpressionItem) {
        String columnName = selectExpressionItem.getExpression().toString();
        if (columnName.indexOf(".") != -1) {
            columnName = columnName.split("\\.")[1];
        }
//        if(selectExpressionItem.getExpression())
        Data_Storage.selectedColumns.add(columnName);

        if (selectExpressionItem.getAlias() != null) {
            System.out.println("Alias is " + selectExpressionItem.getAlias());
        }

    }
}
