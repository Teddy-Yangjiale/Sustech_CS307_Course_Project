package edu.sustech.cs307.physicalOperator;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.logicalOperator.LogicalOperator;
import edu.sustech.cs307.meta.ColumnMeta;
import edu.sustech.cs307.meta.TabCol;
import edu.sustech.cs307.optimizer.LogicalPlanner;
import edu.sustech.cs307.optimizer.PhysicalPlanner;
import edu.sustech.cs307.system.DBManager;
import edu.sustech.cs307.tuple.JoinTuple;
import edu.sustech.cs307.tuple.Tuple;
import edu.sustech.cs307.value.Value;
import edu.sustech.cs307.value.ValueComparer;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.statement.select.ParenthesedSelect;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.ArrayList;
import java.util.List;

public class SubqueryFilterOperator implements PhysicalOperator {
    private final DBManager dbManager;
    private final PhysicalOperator child;
    private final Expression whereExpr;
    private Tuple currentTuple;
    private boolean readyForNext;
    private boolean open;

    public SubqueryFilterOperator(DBManager dbManager, PhysicalOperator child, Expression whereExpr) {
        this.dbManager = dbManager;
        this.child = child;
        this.whereExpr = whereExpr;
    }

    @Override
    public boolean hasNext() throws DBException {
        if (!open) {
            return false;
        }
        if (!readyForNext) {
            return findNext();
        }
        return currentTuple != null;
    }

    @Override
    public void Begin() throws DBException {
        child.Begin();
        currentTuple = null;
        readyForNext = false;
        open = true;
    }

    @Override
    public void Next() throws DBException {
        if (!readyForNext) {
            hasNext();
        }
        readyForNext = false;
    }

    private boolean findNext() throws DBException {
        currentTuple = null;
        while (child.hasNext()) {
            child.Next();
            Tuple tuple = child.Current();
            if (tuple != null && evaluate(tuple, whereExpr)) {
                currentTuple = tuple;
                readyForNext = true;
                return true;
            }
        }
        return false;
    }

    @SuppressWarnings("deprecation")
    private boolean evaluate(Tuple tuple, Expression expression) throws DBException {
        if (expression instanceof Parenthesis parenthesis) {
            return evaluate(tuple, parenthesis.getExpression());
        }
        if (expression instanceof AndExpression andExpression) {
            return evaluate(tuple, andExpression.getLeftExpression())
                    && evaluate(tuple, andExpression.getRightExpression());
        }
        if (expression instanceof OrExpression orExpression) {
            return evaluate(tuple, orExpression.getLeftExpression())
                    || evaluate(tuple, orExpression.getRightExpression());
        }
        if (expression instanceof ExistsExpression existsExpression) {
            boolean exists = !executeSubquery(selectFromExpression(existsExpression.getRightExpression()), tuple).isEmpty();
            return existsExpression.isNot() ? !exists : exists;
        }
        if (expression instanceof InExpression inExpression) {
            Value leftValue = tuple.evaluateExpression(inExpression.getLeftExpression());
            Expression rightExpression = inExpression.getRightExpression();
            boolean found = false;
            if (rightExpression instanceof ExpressionList<?> expressionList) {
                for (Expression rightValueExpression : expressionList.getExpressions()) {
                    if (ValueComparer.compare(leftValue, tuple.evaluateExpression(rightValueExpression)) == 0) {
                        found = true;
                        break;
                    }
                }
            } else {
                for (Value value : executeSubquery(selectFromExpression(rightExpression), tuple)) {
                    if (ValueComparer.compare(leftValue, value) == 0) {
                        found = true;
                        break;
                    }
                }
            }
            return inExpression.isNot() ? !found : found;
        }
        if (expression instanceof BinaryExpression) {
            return tuple.eval_expr(expression);
        }
        return tuple.eval_expr(expression);
    }

    private Select selectFromExpression(Expression expression) {
        if (expression instanceof ParenthesedSelect parenthesedSelect) {
            return parenthesedSelect;
        }
        if (expression instanceof Select select) {
            return select;
        }
        throw new RuntimeException("Expected subquery expression: " + expression);
    }

    private ArrayList<Value> executeSubquery(Select select, Tuple outerTuple) throws DBException {
        if (outerTuple != null && select.getPlainSelect() != null
                && select.getPlainSelect().getWhere() != null
                && !hasAggregate(select.getPlainSelect())) {
            return executeCorrelatedPlainSelect(select.getPlainSelect(), outerTuple);
        }
        LogicalOperator logicalOperator = LogicalPlanner.handleSelect(dbManager, select);
        PhysicalOperator physicalOperator = PhysicalPlanner.generateOperator(dbManager, logicalOperator);
        ArrayList<Value> values = new ArrayList<>();
        physicalOperator.Begin();
        while (physicalOperator.hasNext()) {
            physicalOperator.Next();
            Tuple tuple = physicalOperator.Current();
            if (tuple != null && tuple.getValues().length > 0) {
                values.add(tuple.getValues()[0]);
            }
        }
        physicalOperator.Close();
        return values;
    }

    private boolean hasAggregate(PlainSelect plainSelect) {
        if (plainSelect.getGroupBy() != null) {
            return true;
        }
        for (SelectItem<?> selectItem : plainSelect.getSelectItems()) {
            if (selectItem.getExpression() instanceof net.sf.jsqlparser.expression.Function) {
                return true;
            }
        }
        return false;
    }

    private ArrayList<Value> executeCorrelatedPlainSelect(PlainSelect plainSelect, Tuple outerTuple) throws DBException {
        PhysicalOperator input = new SeqScanOperator(plainSelect.getFromItem().toString(), dbManager);
        if (plainSelect.getJoins() != null) {
            for (net.sf.jsqlparser.statement.select.Join join : plainSelect.getJoins()) {
                input = new NestedLoopJoinOperator(
                        input,
                        new SeqScanOperator(join.getRightItem().toString(), dbManager),
                        join.getOnExpressions());
            }
        }
        ArrayList<Value> values = new ArrayList<>();
        input.Begin();
        while (input.hasNext()) {
            input.Next();
            Tuple innerTuple = input.Current();
            if (innerTuple == null) {
                continue;
            }
            Tuple combinedTuple = new JoinTuple(outerTuple, innerTuple, combineSchema(outerTuple, innerTuple));
            if (evaluate(combinedTuple, plainSelect.getWhere())) {
                values.add(combinedTuple.evaluateExpression(plainSelect.getSelectItems().get(0).getExpression()));
            }
        }
        input.Close();
        return values;
    }

    private TabCol[] combineSchema(Tuple left, Tuple right) {
        ArrayList<TabCol> schema = new ArrayList<>();
        if (left.getTupleSchema() != null) {
            schema.addAll(List.of(left.getTupleSchema()));
        }
        if (right.getTupleSchema() != null) {
            schema.addAll(List.of(right.getTupleSchema()));
        }
        return schema.toArray(new TabCol[0]);
    }

    @Override
    public Tuple Current() {
        return currentTuple;
    }

    @Override
    public void Close() {
        child.Close();
        currentTuple = null;
        readyForNext = false;
        open = false;
    }

    @Override
    public ArrayList<ColumnMeta> outputSchema() {
        return child.outputSchema();
    }
}
