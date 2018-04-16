package edu.buffalo.www.cse4562;
import java.sql.Array;
import java.sql.SQLException;
import java.util.ArrayList;

import net.sf.jsqlparser.eval.Eval;
//import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.SubSelect;

public class Evaluator extends Eval{
	String col_name;
	String tableName = null;
	String origtableName = null;
	ArrayList<Column> schema;
	ArrayList<String> temp_array;
	Evaluator(ArrayList<Column> schema,ArrayList<String> temp_array){
	    this.schema = schema;
	    this.temp_array = temp_array;
    }
	@Override
	public PrimitiveValue eval(Column column) throws SQLException {
		// TODO Auto-generated method stub
		col_name = column.getColumnName();
		tableName = null;
		origtableName = null;
		if (Data_Storage.alias_table.containsKey(col_name))
			col_name = Data_Storage.alias_table.get(col_name);
		if (column.getTable().getName() == null)
			tableName = Data_Storage.current_schema.get(col_name);
		else
			tableName = column.getTable().getName();
		if (Data_Storage.table_alias.containsKey(tableName)) {
			origtableName = Data_Storage.table_alias.get(tableName);
		} else {
			origtableName = tableName;
		}

		int position = schema.indexOf(new Column(new Table(origtableName), col_name));
		if(position == -1){
			position = schema.indexOf(new Column(new Table(origtableName), col_name.split("_")[1]));
		}
		String data_type_table = origtableName;
		if(Data_Storage.table_alias.get(origtableName) != null){
			data_type_table = Data_Storage.table_alias.get(origtableName);
		}

		String data_type = Data_Storage.tables.get(data_type_table).get(col_name);
		if(data_type == null){
			data_type = Data_Storage.tables.get(data_type_table).get(col_name.split("_")[1]);
		}
		if (data_type.equals("INTEGER")) {
			return new LongValue(temp_array.get(position));
		} else if (data_type.equals("STRING") || data_type.equals("VARCHAR") | data_type.equals("CHAR")) {
			return new StringValue(temp_array.get(position));
		} else if (data_type.equals("DOUBLE")) {
			return new DoubleValue(temp_array.get(position));
		} else if (data_type.equals("DATE")) {
			return new DateValue(temp_array.get(position));
		} else {
			return null;
		}
	}

	
}
