package edu.sustech.cs307.index;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.record.RID;
import edu.sustech.cs307.value.Value;

import java.util.List;

public interface Index {
    List<RID> equalTo(Value value) throws DBException;

    List<RID> lessThan(Value value, boolean isEqual) throws DBException;

    List<RID> moreThan(Value value, boolean isEqual) throws DBException;

    List<RID> range(Value low, Value high, boolean leftEqual, boolean rightEqual) throws DBException;

    void insert(Value value, RID rid) throws DBException;

    void delete(Value value, RID rid) throws DBException;

    String printTree();
}
