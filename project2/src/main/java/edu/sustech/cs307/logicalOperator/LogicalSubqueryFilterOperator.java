package edu.sustech.cs307.logicalOperator;

import net.sf.jsqlparser.expression.Expression;

import java.util.Collections;

public class LogicalSubqueryFilterOperator extends LogicalOperator {
    private final LogicalOperator child;
    private final Expression whereExpr;

    public LogicalSubqueryFilterOperator(LogicalOperator child, Expression whereExpr) {
        super(Collections.singletonList(child));
        this.child = child;
        this.whereExpr = whereExpr;
    }

    public LogicalOperator getChild() {
        return child;
    }

    public Expression getWhereExpr() {
        return whereExpr;
    }

    @Override
    public String toString() {
        return "SubqueryFilterOperator(condition=" + whereExpr + ")\n    " + child;
    }
}
