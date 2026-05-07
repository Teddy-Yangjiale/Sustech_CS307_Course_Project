package edu.sustech.cs307.physicalOperator;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.meta.ColumnMeta;
import edu.sustech.cs307.tuple.TempTuple;
import edu.sustech.cs307.tuple.Tuple;
import edu.sustech.cs307.value.Value;
import edu.sustech.cs307.value.ValueType;

import java.util.ArrayList;

public class CountOperator implements PhysicalOperator {
    private final PhysicalOperator child;
    private int count;
    private boolean done;

    public CountOperator(PhysicalOperator child) {
        this.child = child;
    }

    @Override
    public boolean hasNext() {
        return !done;
    }

    @Override
    public void Begin() throws DBException {
        child.Begin();
        count = 0;
        done = false;
        while (child.hasNext()) {
            child.Next();
            if (child.Current() != null) {
                count++;
            }
        }
    }

    @Override
    public void Next() {
        done = true;
    }

    @Override
    public Tuple Current() {
        ArrayList<Value> values = new ArrayList<>();
        values.add(new Value((long) count));
        return new TempTuple(values);
    }

    @Override
    public void Close() {
        child.Close();
    }

    @Override
    public ArrayList<ColumnMeta> outputSchema() {
        ArrayList<ColumnMeta> schema = new ArrayList<>();
        schema.add(new ColumnMeta("count", "count", ValueType.INTEGER, 0, 0));
        return schema;
    }
}
