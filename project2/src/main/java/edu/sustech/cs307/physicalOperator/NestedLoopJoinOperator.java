package edu.sustech.cs307.physicalOperator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.meta.ColumnMeta;
import edu.sustech.cs307.meta.TabCol;
import edu.sustech.cs307.tuple.JoinTuple;
import edu.sustech.cs307.tuple.Tuple;
import net.sf.jsqlparser.expression.Expression;

public class NestedLoopJoinOperator implements PhysicalOperator {

    private PhysicalOperator leftOperator;
    private PhysicalOperator rightOperator;
    private Collection<Expression> expr;
    private final List<Tuple> leftRows = new ArrayList<>();
    private final List<Tuple> rightRows = new ArrayList<>();
    private int leftIndex;
    private int rightIndex;
    private Tuple current;

    public NestedLoopJoinOperator(PhysicalOperator leftOperator, PhysicalOperator rightOperator,
            Collection<Expression> expr) {
        this.leftOperator = leftOperator;
        this.rightOperator = rightOperator;
        this.expr = expr;
    }

    @Override
    public boolean hasNext() {
        return findNext(false);
    }

    @Override
    public void Begin() throws DBException {
        leftOperator.Begin();
        rightOperator.Begin();
        leftRows.clear();
        rightRows.clear();
        while (leftOperator.hasNext()) {
            leftOperator.Next();
            Tuple tuple = leftOperator.Current();
            if (tuple != null) {
                leftRows.add(tuple);
            }
        }
        while (rightOperator.hasNext()) {
            rightOperator.Next();
            Tuple tuple = rightOperator.Current();
            if (tuple != null) {
                rightRows.add(tuple);
            }
        }
        leftIndex = 0;
        rightIndex = 0;
        current = null;
    }

    @Override
    public void Next() {
        findNext(true);
    }

    @Override
    public Tuple Current() {
        return current;
    }

    @Override
    public void Close() {
        leftOperator.Close();
        rightOperator.Close();
        leftRows.clear();
        rightRows.clear();
        current = null;
    }

    @Override
    public ArrayList<ColumnMeta> outputSchema() {
        ArrayList<ColumnMeta> schema = new ArrayList<>();
        schema.addAll(leftOperator.outputSchema());
        schema.addAll(rightOperator.outputSchema());
        return schema;
    }

    private boolean findNext(boolean consume) {
        int savedLeft = leftIndex;
        int savedRight = rightIndex;
        Tuple savedCurrent = current;
        try {
            while (leftIndex < leftRows.size()) {
                while (rightIndex < rightRows.size()) {
                    Tuple candidate = new JoinTuple(leftRows.get(leftIndex), rightRows.get(rightIndex), outputTabCols());
                    rightIndex++;
                    if (matches(candidate)) {
                        if (consume) {
                            current = candidate;
                        } else {
                            leftIndex = savedLeft;
                            rightIndex = savedRight;
                            current = savedCurrent;
                        }
                        return true;
                    }
                }
                leftIndex++;
                rightIndex = 0;
            }
            if (consume) {
                current = null;
            } else {
                leftIndex = savedLeft;
                rightIndex = savedRight;
                current = savedCurrent;
            }
            return false;
        } catch (DBException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean matches(Tuple tuple) throws DBException {
        if (expr == null || expr.isEmpty()) {
            return true;
        }
        for (Expression expression : expr) {
            if (!tuple.eval_expr(expression)) {
                return false;
            }
        }
        return true;
    }

    private TabCol[] outputTabCols() {
        ArrayList<TabCol> tabCols = new ArrayList<>();
        for (ColumnMeta columnMeta : outputSchema()) {
            tabCols.add(new TabCol(columnMeta.tableName, columnMeta.name));
        }
        return tabCols.toArray(new TabCol[0]);
    }
}
