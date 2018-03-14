package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.schema.Column;

public class ProjectColumn {

    static public void getWhereColumns(Expression expr){
       while(expr != null){
           if(expr instanceof BinaryExpression) {
               BinaryExpression bin = (BinaryExpression) expr;
               System.out.println(bin.getRightExpression().toString());
               System.out.println(bin.getLeftExpression().toString());
           }
       }
    }
}
