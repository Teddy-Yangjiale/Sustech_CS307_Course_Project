package edu.sustech.cs307.meta;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.exception.ExceptionTypes;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class TableMeta {
    public String tableName;
    public ArrayList<ColumnMeta> columns_list;

    @JsonIgnore
    public Map<String, ColumnMeta> columns; // 列名 -> 列的元数据

    private Map<String, IndexMeta> indexes; // 索引名 -> 索引元数据

    private Map<String, Integer> column_rank;

    public enum IndexType {
        BTREE
    }

    public TableMeta(String tableName) {
        this.tableName = tableName;
        this.columns_list = new ArrayList<>();
        this.columns = new HashMap<>();
        this.indexes = new HashMap<>();
    }

    public TableMeta(String tableName, ArrayList<ColumnMeta> columns) {
        this.tableName = tableName;
        this.columns_list = columns;
        this.columns = new HashMap<>();
        this.indexes = new HashMap<>();
        for (ColumnMeta column : columns) {
            this.columns.put(column.name, column);
        }
    }

    @JsonCreator
    public TableMeta(@JsonProperty("tableName") String tableName, @JsonProperty("columns_list") ArrayList<ColumnMeta> columns_list, @JsonProperty("indexes")  Map<String, IndexMeta> indexes) {
        this.tableName = tableName;
        this.columns_list = columns_list;
        this.columns = new HashMap<>();
        this.indexes = indexes == null ? new HashMap<>() : indexes;
        for (var column : columns_list) {
            this.columns.put(column.name, column);
        }
    }

    public void addColumn(ColumnMeta column) throws DBException {
        String columnName = column.name;
        if (this.columns.containsKey(columnName)) {
            throw new DBException(ExceptionTypes.ColumnAlreadyExist(columnName));
        }
        this.columns.put(columnName, column);
        this.columns_list.add(column);
    }

    public void dropColumn(String columnName) throws DBException {
        if (!this.columns.containsKey(columnName)) {
            throw new DBException(ExceptionTypes.ColumnDoesNotExist(columnName));
        }
        this.columns.remove(columnName);
        this.columns_list.removeIf(column -> column.name.equalsIgnoreCase(columnName));
    }

    public ColumnMeta getColumnMeta(String columnName) {
        if (this.columns.containsKey(columnName)) {
            return this.columns.get(columnName);
        }
        return null;
    }

    public Map<String, ColumnMeta> getColumns() {
        return this.columns;
    }

    public void setColumns(Map<String, ColumnMeta> columns) {
        this.columns = columns;
    }

    public int columnCount() {
        return this.columns.size();
    }

    public boolean hasColumn(String columnName) {
        return this.columns.containsKey(columnName);
    }

    public Map<String, IndexMeta> getIndexes() {
        return indexes;
    }

    public void setIndexes(Map<String, IndexMeta> indexes) {
        this.indexes = indexes == null ? new HashMap<>() : indexes;
    }

    public void addIndex(IndexMeta indexMeta) throws DBException {
        if (indexes.containsKey(indexMeta.indexName)) {
            throw new DBException(ExceptionTypes.InvalidSQL(indexMeta.indexName, "Index already exists"));
        }
        indexes.put(indexMeta.indexName, indexMeta);
    }

    public IndexMeta dropIndex(String indexName) throws DBException {
        IndexMeta removed = indexes.remove(indexName);
        if (removed == null) {
            throw new DBException(ExceptionTypes.InvalidSQL(indexName, "Index does not exist"));
        }
        return removed;
    }

    public IndexMeta findIndexOnColumn(String columnName) {
        for (IndexMeta indexMeta : indexes.values()) {
            if (indexMeta.columnName.equalsIgnoreCase(columnName)) {
                return indexMeta;
            }
        }
        return null;
    }
}
