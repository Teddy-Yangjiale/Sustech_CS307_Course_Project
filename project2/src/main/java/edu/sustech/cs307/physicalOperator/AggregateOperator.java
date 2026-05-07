package edu.sustech.cs307.physicalOperator;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.exception.ExceptionTypes;
import edu.sustech.cs307.meta.ColumnMeta;
import edu.sustech.cs307.meta.TabCol;
import edu.sustech.cs307.tuple.TempTuple;
import edu.sustech.cs307.tuple.Tuple;
import edu.sustech.cs307.value.Value;
import edu.sustech.cs307.value.ValueComparer;
import edu.sustech.cs307.value.ValueType;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class AggregateOperator implements PhysicalOperator {
    private final PhysicalOperator child;
    private final List<SelectItem<?>> selectItems;
    private final List<Expression> groupByExpressions;
    private final List<Tuple> rows = new ArrayList<>();
    private ArrayList<ColumnMeta> schema;
    private int index;
    private Tuple current;

    public AggregateOperator(PhysicalOperator child, List<SelectItem<?>> selectItems, List<Expression> groupByExpressions) {
        this.child = child;
        this.selectItems = selectItems;
        this.groupByExpressions = groupByExpressions == null ? List.of() : groupByExpressions;
    }

    @Override
    public boolean hasNext() {
        return index < rows.size();
    }

    @Override
    public void Begin() throws DBException {
        child.Begin();
        rows.clear();
        schema = buildSchema();
        LinkedHashMap<String, GroupState> groups = new LinkedHashMap<>();
        if (groupByExpressions.isEmpty()) {
            groups.put("", new GroupState(List.of()));
        }

        while (child.hasNext()) {
            child.Next();
            Tuple tuple = child.Current();
            if (tuple == null) {
                continue;
            }
            List<Value> groupValues = evaluateGroupValues(tuple);
            String key = groupKey(groupValues);
            GroupState state = groups.computeIfAbsent(key, ignored -> new GroupState(groupValues));
            state.accept(tuple);
        }

        TabCol[] tupleSchema = schema.stream()
                .map(column -> new TabCol(column.tableName, column.name))
                .toArray(TabCol[]::new);
        for (GroupState state : groups.values()) {
            rows.add(new TempTuple(state.outputValues(), tupleSchema));
        }
        index = 0;
        current = null;
    }

    private List<Value> evaluateGroupValues(Tuple tuple) throws DBException {
        ArrayList<Value> values = new ArrayList<>();
        for (Expression expression : groupByExpressions) {
            values.add(tuple.evaluateExpression(expression));
        }
        return values;
    }

    private String groupKey(List<Value> values) {
        StringBuilder builder = new StringBuilder();
        for (Value value : values) {
            builder.append(value == null ? "null" : value.type + ":" + value.value).append('\u0001');
        }
        return builder.toString();
    }

    private ArrayList<ColumnMeta> buildSchema() throws DBException {
        ArrayList<ColumnMeta> result = new ArrayList<>();
        for (SelectItem<?> selectItem : selectItems) {
            Expression expression = selectItem.getExpression();
            String name = outputName(selectItem);
            if (expression instanceof Column column) {
                ColumnMeta source = findColumn(column);
                String outputTable = selectItem.getAliasName() == null ? source.tableName : "aggregate";
                String outputName = selectItem.getAliasName() == null ? source.name : name;
                result.add(new ColumnMeta(outputTable, outputName, source.type, source.len, result.size()));
            } else if (expression instanceof Function function) {
                ValueType type = function.getName().equalsIgnoreCase("count")
                        ? ValueType.INTEGER
                        : functionArgumentType(function);
                result.add(new ColumnMeta("aggregate", name, type, type == ValueType.CHAR ? Value.CHAR_SIZE : 8,
                        result.size()));
            } else {
                throw new DBException(ExceptionTypes.UnsupportedExpression(expression));
            }
        }
        return result;
    }

    private ColumnMeta findColumn(Column column) throws DBException {
        ColumnMeta match = null;
        String tableName = column.getTableName();
        for (ColumnMeta columnMeta : child.outputSchema()) {
            boolean tableMatches = tableName == null || tableName.isBlank()
                    || tableName.equalsIgnoreCase(columnMeta.tableName);
            if (tableMatches && column.getColumnName().equalsIgnoreCase(columnMeta.name)) {
                if (match != null) {
                    throw new RuntimeException("Ambiguous column in aggregate output: " + column.getColumnName());
                }
                match = columnMeta;
            }
        }
        if (match == null) {
            throw new DBException(ExceptionTypes.ColumnDoesNotExist(column.getColumnName()));
        }
        return match;
    }

    private ValueType functionArgumentType(Function function) throws DBException {
        Expression parameter = getSingleParameter(function);
        if (parameter instanceof Column column) {
            return findColumn(column).type;
        }
        return ValueType.UNKNOWN;
    }

    private String outputName(SelectItem<?> selectItem) {
        if (selectItem.getAliasName() != null) {
            return selectItem.getAliasName();
        }
        return selectItem.getExpression().toString();
    }

    @SuppressWarnings("deprecation")
    private Expression getSingleParameter(Function function) throws DBException {
        if (function.getName().equalsIgnoreCase("count") && function.isAllColumns()) {
            return null;
        }
        if (function.getParameters() == null || function.getParameters().size() != 1) {
            throw new DBException(ExceptionTypes.UnsupportedExpression(function));
        }
        return function.getParameters().getExpressions().get(0);
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
        return schema == null ? new ArrayList<>() : schema;
    }

    private final class GroupState {
        private final List<Value> groupValues;
        private final Map<Integer, Value> aggregateValues = new LinkedHashMap<>();
        private long count;

        private GroupState(List<Value> groupValues) {
            this.groupValues = new ArrayList<>(groupValues);
        }

        private void accept(Tuple tuple) throws DBException {
            count++;
            for (int i = 0; i < selectItems.size(); i++) {
                Expression expression = selectItems.get(i).getExpression();
                if (!(expression instanceof Function function)) {
                    continue;
                }
                String name = function.getName();
                if (name.equalsIgnoreCase("count")) {
                    aggregateValues.put(i, new Value(count));
                    continue;
                }
                if (!name.equalsIgnoreCase("max") && !name.equalsIgnoreCase("min")) {
                    throw new DBException(ExceptionTypes.UnsupportedExpression(function));
                }
                Value value = tuple.evaluateExpression(getSingleParameter(function));
                Value current = aggregateValues.get(i);
                if (current == null) {
                    aggregateValues.put(i, value);
                } else {
                    int comparison = ValueComparer.compare(value, current);
                    if (name.equalsIgnoreCase("max") && comparison > 0) {
                        aggregateValues.put(i, value);
                    } else if (name.equalsIgnoreCase("min") && comparison < 0) {
                        aggregateValues.put(i, value);
                    }
                }
            }
        }

        private List<Value> outputValues() throws DBException {
            ArrayList<Value> values = new ArrayList<>();
            int groupIndex = 0;
            for (int i = 0; i < selectItems.size(); i++) {
                Expression expression = selectItems.get(i).getExpression();
                if (expression instanceof Column) {
                    values.add(groupValues.get(groupIndex++));
                } else if (expression instanceof Function function) {
                    if (function.getName().equalsIgnoreCase("count")) {
                        values.add(aggregateValues.getOrDefault(i, new Value(0L)));
                    } else {
                        values.add(aggregateValues.get(i));
                    }
                } else {
                    throw new DBException(ExceptionTypes.UnsupportedExpression(expression));
                }
            }
            return values;
        }
    }
}
