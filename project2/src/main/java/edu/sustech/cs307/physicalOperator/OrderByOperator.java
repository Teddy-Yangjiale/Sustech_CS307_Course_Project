package edu.sustech.cs307.physicalOperator;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.meta.ColumnMeta;
import edu.sustech.cs307.tuple.Tuple;
import edu.sustech.cs307.value.Value;
import edu.sustech.cs307.value.ValueComparer;
import net.sf.jsqlparser.statement.select.OrderByElement;

import java.util.ArrayList;
import java.util.List;

public class OrderByOperator implements PhysicalOperator {
    private final PhysicalOperator child;
    private final List<OrderByElement> orderByElements;
    private final List<Tuple> rows = new ArrayList<>();
    private int index;
    private Tuple current;

    public OrderByOperator(PhysicalOperator child, List<OrderByElement> orderByElements) {
        this.child = child;
        if (orderByElements == null || orderByElements.isEmpty()) {
            throw new RuntimeException("ORDER BY requires at least one expression");
        }
        this.orderByElements = orderByElements;
    }

    @Override
    public boolean hasNext() {
        return index < rows.size();
    }

    @Override
    public void Begin() throws DBException {
        child.Begin();
        rows.clear();
        while (child.hasNext()) {
            child.Next();
            Tuple tuple = child.Current();
            if (tuple != null) {
                rows.add(tuple);
            }
        }
        rows.sort((left, right) -> {
            try {
                for (OrderByElement orderByElement : orderByElements) {
                    Value leftValue = left.evaluateExpression(orderByElement.getExpression());
                    Value rightValue = right.evaluateExpression(orderByElement.getExpression());
                    int result = ValueComparer.compare(leftValue, rightValue);
                    if (result != 0) {
                        return orderByElement.isAsc() ? result : -result;
                    }
                }
                return 0;
            } catch (DBException e) {
                throw new RuntimeException(e);
            }
        });
        index = 0;
        current = null;
    }

    @Override
    public void Next() {
        current = rows.get(index++);
    }

    @Override
    public Tuple Current() {
        return current;
    }

    @Override
    public void Close() {
        child.Close();
        rows.clear();
        current = null;
    }

    @Override
    public ArrayList<ColumnMeta> outputSchema() {
        return child.outputSchema();
    }
}
