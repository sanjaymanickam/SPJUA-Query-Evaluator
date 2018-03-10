package edu.buffalo.www.cse4562;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;

public class Expr_Visitor implements ExpressionVisitor {

    Expression expr = null;
    @Override
    public void visit(NullValue nullValue) {
        expr = nullValue;
    }

    @Override
    public void visit(Function function) {
        expr = function;
    }

    @Override
    public void visit(InverseExpression inverseExpression) {
        expr = inverseExpression;
    }

    @Override
    public void visit(JdbcParameter jdbcParameter) {

    }

    @Override
    public void visit(DoubleValue doubleValue) {
        expr = doubleValue;
    }

    @Override
    public void visit(LongValue longValue) {
        expr = longValue;
    }

    @Override
    public void visit(DateValue dateValue) {
        expr = dateValue;
    }

    @Override
    public void visit(TimeValue timeValue) {
        expr = timeValue;
    }

    @Override
    public void visit(TimestampValue timestampValue) {
        expr = timestampValue;
    }

    @Override
    public void visit(BooleanValue booleanValue) {
        expr = booleanValue;
    }

    @Override
    public void visit(StringValue stringValue) {
        expr = stringValue;
    }

    @Override
    public void visit(Addition addition) {
        expr = addition;
    }

    @Override
    public void visit(Division division) {
        expr = division;
    }

    @Override
    public void visit(Multiplication multiplication) {
        expr = multiplication;
    }

    @Override
    public void visit(Subtraction subtraction) {
        expr = subtraction;
    }

    @Override
    public void visit(AndExpression andExpression) {
        expr = andExpression;
    }

    @Override
    public void visit(OrExpression orExpression) {
        expr = orExpression;
    }

    @Override
    public void visit(Between between) {
        expr = between;
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        expr = equalsTo;
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        expr = greaterThan;
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        expr = greaterThanEquals;
    }

    @Override
    public void visit(InExpression inExpression) {
        expr = inExpression;
    }

    @Override
    public void visit(IsNullExpression isNullExpression) {
        expr = isNullExpression;
    }

    @Override
    public void visit(LikeExpression likeExpression) {
        expr = likeExpression;
    }

    @Override
    public void visit(MinorThan minorThan) {
        expr = minorThan;
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        expr = minorThanEquals;
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        expr = notEqualsTo;
    }

    @Override
    public void visit(Column column) {
        expr = column;
    }

    @Override
    public void visit(SubSelect subSelect) {
        expr = subSelect;
    }

    @Override
    public void visit(CaseExpression caseExpression) {
        expr = caseExpression;
    }

    @Override
    public void visit(WhenClause whenClause) {
        expr = whenClause;
    }

    @Override
    public void visit(ExistsExpression existsExpression) {
        expr = existsExpression;
    }

    @Override
    public void visit(AllComparisonExpression allComparisonExpression) {
        expr = allComparisonExpression;
    }

    @Override
    public void visit(AnyComparisonExpression anyComparisonExpression) {
        expr = anyComparisonExpression;
    }

    @Override
    public void visit(Concat concat) {
        expr = concat;
    }

    @Override
    public void visit(Matches matches) {
        expr = matches;
    }

    @Override
    public void visit(BitwiseAnd bitwiseAnd) {
        expr = bitwiseAnd;
    }

    @Override
    public void visit(BitwiseOr bitwiseOr) {
        expr = bitwiseOr;
    }

    @Override
    public void visit(BitwiseXor bitwiseXor) {
        expr = bitwiseXor;
    }

    public Expression getExpr() {
        return expr;
    }
}

