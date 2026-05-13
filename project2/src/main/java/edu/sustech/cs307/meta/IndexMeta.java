package edu.sustech.cs307.meta;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class IndexMeta {
    public String indexName;
    public String tableName;
    public String columnName;
    public TableMeta.IndexType type;

    @JsonCreator
    public IndexMeta(@JsonProperty("indexName") String indexName,
                     @JsonProperty("tableName") String tableName,
                     @JsonProperty("columnName") String columnName,
                     @JsonProperty("type") TableMeta.IndexType type) {
        this.indexName = indexName;
        this.tableName = tableName;
        this.columnName = columnName;
        this.type = type == null ? TableMeta.IndexType.BTREE : type;
    }
}
