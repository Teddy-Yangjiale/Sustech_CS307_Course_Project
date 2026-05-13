package edu.sustech.cs307.physicalOperator;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.index.InMemoryOrderedIndex;
import edu.sustech.cs307.meta.ColumnMeta;
import edu.sustech.cs307.meta.IndexMeta;
import edu.sustech.cs307.meta.TableMeta;
import edu.sustech.cs307.record.RID;
import edu.sustech.cs307.record.Record;
import edu.sustech.cs307.record.RecordFileHandle;
import edu.sustech.cs307.system.DBManager;
import edu.sustech.cs307.tuple.TableTuple;
import edu.sustech.cs307.tuple.Tuple;
import edu.sustech.cs307.value.Value;

import java.util.ArrayList;
import java.util.List;

public class IndexScanOperator implements PhysicalOperator {
    public enum PredicateType {
        EQUAL,
        LESS_THAN,
        LESS_THAN_OR_EQUAL,
        GREATER_THAN,
        GREATER_THAN_OR_EQUAL,
        RANGE
    }

    private final DBManager dbManager;
    private final String tableName;
    private final IndexMeta indexMeta;
    private final PredicateType predicateType;
    private final Value value;
    private final Value highValue;
    private final boolean leftEqual;
    private final boolean rightEqual;
    private TableMeta tableMeta;
    private RecordFileHandle fileHandle;
    private List<RID> rids;
    private int cursor;
    private Tuple current;
    private boolean open;

    public IndexScanOperator(DBManager dbManager, String tableName, IndexMeta indexMeta,
                             PredicateType predicateType, Value value) {
        this(dbManager, tableName, indexMeta, predicateType, value, null, false, false);
    }

    public IndexScanOperator(DBManager dbManager, String tableName, IndexMeta indexMeta,
                             Value value, Value highValue, boolean leftEqual, boolean rightEqual) {
        this(dbManager, tableName, indexMeta, PredicateType.RANGE, value, highValue, leftEqual, rightEqual);
    }

    private IndexScanOperator(DBManager dbManager, String tableName, IndexMeta indexMeta,
                              PredicateType predicateType, Value value,
                              Value highValue, boolean leftEqual, boolean rightEqual) {
        this.dbManager = dbManager;
        this.tableName = tableName;
        this.indexMeta = indexMeta;
        this.predicateType = predicateType;
        this.value = value;
        this.highValue = highValue;
        this.leftEqual = leftEqual;
        this.rightEqual = rightEqual;
        try {
            this.tableMeta = dbManager.getMetaManager().getTable(tableName);
        } catch (DBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean hasNext() {
        return open && cursor < rids.size();
    }

    @Override
    public void Begin() throws DBException {
        this.tableMeta = dbManager.getMetaManager().getTable(tableName);
        InMemoryOrderedIndex index = dbManager.getIndex(indexMeta.indexName);
        this.rids = switch (predicateType) {
            case EQUAL -> index.equalTo(value);
            case LESS_THAN -> index.lessThan(value, false);
            case LESS_THAN_OR_EQUAL -> index.lessThan(value, true);
            case GREATER_THAN -> index.moreThan(value, false);
            case GREATER_THAN_OR_EQUAL -> index.moreThan(value, true);
            case RANGE -> index.range(value, highValue, leftEqual, rightEqual);
        };
        this.fileHandle = dbManager.getRecordManager().OpenFile(tableName);
        this.cursor = 0;
        this.current = null;
        this.open = true;
    }

    @Override
    public void Next() throws DBException {
        current = null;
        while (hasNext()) {
            RID rid = rids.get(cursor++);
            if (!fileHandle.IsRecord(rid)) {
                continue;
            }
            Record record = fileHandle.GetRecord(rid);
            current = new TableTuple(tableName, tableMeta, record, rid);
            return;
        }
    }

    @Override
    public Tuple Current() {
        return current;
    }

    @Override
    public void Close() {
        if (!open) {
            return;
        }
        try {
            dbManager.getRecordManager().CloseFile(fileHandle);
        } catch (DBException e) {
            e.printStackTrace();
        }
        fileHandle = null;
        current = null;
        open = false;
    }

    @Override
    public ArrayList<ColumnMeta> outputSchema() {
        return tableMeta.columns_list;
    }
}
