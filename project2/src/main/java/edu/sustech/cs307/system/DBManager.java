package edu.sustech.cs307.system;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.exception.ExceptionTypes;
import edu.sustech.cs307.index.InMemoryOrderedIndex;
import edu.sustech.cs307.meta.ColumnMeta;
import edu.sustech.cs307.meta.IndexMeta;
import edu.sustech.cs307.meta.MetaManager;
import edu.sustech.cs307.meta.TabCol;
import edu.sustech.cs307.meta.TableMeta;
import edu.sustech.cs307.record.RID;
import edu.sustech.cs307.storage.BufferPool;
import edu.sustech.cs307.storage.DiskManager;
import edu.sustech.cs307.storage.replacer.ClockReplacer;
import edu.sustech.cs307.storage.replacer.PageReplacer;
import edu.sustech.cs307.value.Value;
import edu.sustech.cs307.value.ValueType;
import edu.sustech.cs307.record.RecordFileHandle;
import org.apache.commons.lang3.StringUtils;
import org.pmw.tinylog.Logger;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.alter.AlterExpression;
import net.sf.jsqlparser.statement.alter.AlterOperation;
import net.sf.jsqlparser.statement.create.table.ColDataType;

import java.io.File;
import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

public class DBManager {
    private final MetaManager metaManager;
    /* --- --- --- */
    private final DiskManager diskManager;
    private final BufferPool bufferPool;
    private final RecordManager recordManager;
    private TransactionManager transactionManager;
    private final IntFunction<PageReplacer> replacerFactory;
    private final Map<String, InMemoryOrderedIndex> runtimeIndexes;

    public DBManager(DiskManager diskManager, BufferPool bufferPool, RecordManager recordManager,
                     MetaManager metaManager) {
        this(diskManager, bufferPool, recordManager, metaManager, null, ClockReplacer::new);
    }

    public DBManager(DiskManager diskManager, BufferPool bufferPool, RecordManager recordManager,
                     MetaManager metaManager, TransactionManager transactionManager,
                     IntFunction<PageReplacer> replacerFactory) {
        this.diskManager = diskManager;
        this.bufferPool = bufferPool;
        this.recordManager = recordManager;
        this.metaManager = metaManager;
        this.replacerFactory = replacerFactory;
        this.transactionManager = transactionManager == null ? new TransactionManager(this) : transactionManager;
        this.runtimeIndexes = new HashMap<>();
    }

    public TransactionManager getTransactionManager() {
        return transactionManager;
    }

    public void setTransactionManager(TransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    public BufferPool getBufferPool() {
        return bufferPool;
    }

    public RecordManager getRecordManager() {
        return recordManager;
    }

    public DiskManager getDiskManager() {
        return diskManager;
    }

    public MetaManager getMetaManager() {
        return metaManager;
    }

    public boolean isDirExists(String dir) {
        File file = new File(dir);
        return file.exists() && file.isDirectory();
    }

    /**
     * Displays a formatted table listing all available tables in the database.
     * The output is presented in a bordered ASCII table format with centered table
     * names.
     * Each table name is displayed in a separate row within the ASCII borders.
     */
    public void showTables() {
        Logger.info("|-----------|");
        Logger.info("|  Tables   |");
        Logger.info("|-----------|");
        for (String tableName : metaManager.getTableNames()) {
            Logger.info("|{}|", StringUtils.center(tableName, 11));
        }
        Logger.info("|-----------|");
    }

    public void descTable(String table_name) throws DBException {
        TableMeta tableMeta = metaManager.getTable(table_name);
        Logger.info("Table: {}", tableMeta.tableName);
        Logger.info("|---------------|---------------|");
        Logger.info("|     Field     |     Type      |");
        Logger.info("|---------------|---------------|");
        for (ColumnMeta column : tableMeta.columns_list) {
            Logger.info("|{}|{}|", StringUtils.center(column.name, 15), StringUtils.center(column.type.toString(), 15));
        }
        Logger.info("|---------------|---------------|");
    }

    public void alterTable(Alter alterStmt) throws DBException {
        String tableName = alterStmt.getTable().getName();
        if (!isTableExists(tableName)) {
            throw new DBException(ExceptionTypes.TableDoesNotExist(tableName));
        }
        if (alterStmt.getAlterExpressions() == null || alterStmt.getAlterExpressions().size() != 1) {
            throw new DBException(ExceptionTypes.UnsupportedCommand(alterStmt.toString()));
        }
        AlterExpression alterExpression = alterStmt.getAlterExpressions().get(0);
        if (alterExpression.getOperation() == AlterOperation.ADD) {
            for (AlterExpression.ColumnDataType columnDataType : alterExpression.getColDataTypeList()) {
                addColumnAndRewrite(tableName, columnDataType.getColumnName(), columnDataType.getColDataType());
            }
            return;
        }
        if (alterExpression.getOperation() == AlterOperation.DROP) {
            dropColumnAndRewrite(tableName, alterExpression.getColumnName());
            return;
        }
        throw new DBException(ExceptionTypes.UnsupportedCommand(alterStmt.toString()));
    }

    private void addColumnAndRewrite(String tableName, String columnName, ColDataType colDataType) throws DBException {
        TableMeta oldMeta = metaManager.getTable(tableName);
        if (oldMeta.hasColumn(columnName)) {
            throw new DBException(ExceptionTypes.ColumnAlreadyExist(columnName));
        }
        ArrayList<Value[]> oldRows = readAllRows(tableName);
        ArrayList<ColumnMeta> newColumns = copyColumns(oldMeta.columns_list);
        int offset = recordSize(newColumns);
        newColumns.add(new ColumnMeta(tableName, columnName, valueType(colDataType), valueLength(colDataType), offset));
        rewriteTable(tableName, newColumns, oldRows, null, defaultValue(valueType(colDataType)));
        Logger.info("Successfully altered table {} add column {}", tableName, columnName);
    }

    private void dropColumnAndRewrite(String tableName, String columnName) throws DBException {
        TableMeta oldMeta = metaManager.getTable(tableName);
        int dropIndex = -1;
        for (int i = 0; i < oldMeta.columns_list.size(); i++) {
            if (oldMeta.columns_list.get(i).name.equalsIgnoreCase(columnName)) {
                dropIndex = i;
                break;
            }
        }
        if (dropIndex < 0) {
            throw new DBException(ExceptionTypes.ColumnDoesNotExist(columnName));
        }
        ArrayList<Value[]> oldRows = readAllRows(tableName);
        ArrayList<ColumnMeta> newColumns = new ArrayList<>();
        int offset = 0;
        for (ColumnMeta column : oldMeta.columns_list) {
            if (column.name.equalsIgnoreCase(columnName)) {
                continue;
            }
            newColumns.add(new ColumnMeta(tableName, column.name, column.type, column.len, offset));
            offset += column.len;
        }
        rewriteTable(tableName, newColumns, oldRows, dropIndex, null);
        Logger.info("Successfully altered table {} drop column {}", tableName, columnName);
    }

    private ArrayList<Value[]> readAllRows(String tableName) throws DBException {
        ArrayList<Value[]> rows = new ArrayList<>();
        edu.sustech.cs307.physicalOperator.SeqScanOperator scanner =
                new edu.sustech.cs307.physicalOperator.SeqScanOperator(tableName, this);
        scanner.Begin();
        while (scanner.hasNext()) {
            scanner.Next();
            if (scanner.Current() != null) {
                rows.add(scanner.Current().getValues());
            }
        }
        scanner.Close();
        return rows;
    }

    private void rewriteTable(String tableName, ArrayList<ColumnMeta> newColumns, ArrayList<Value[]> oldRows,
                              Integer dropIndex, Value appendedValue) throws DBException {
        TableMeta oldMeta = metaManager.getTable(tableName);
        Map<String, IndexMeta> preservedIndexes = new HashMap<>();
        for (IndexMeta indexMeta : oldMeta.getIndexes().values()) {
            boolean droppedIndexedColumn = dropIndex != null
                    && oldMeta.columns_list.get(dropIndex).name.equalsIgnoreCase(indexMeta.columnName);
            if (!droppedIndexedColumn) {
                preservedIndexes.put(indexMeta.indexName, indexMeta);
            }
        }
        String dataFile = String.format("%s/%s", tableName, "data");
        bufferPool.DeleteAllPages(dataFile);
        bufferPool.Reset();
        diskManager.DeleteFile(dataFile);
        recordManager.CreateFile(dataFile, recordSize(newColumns));
        metaManager.dropTable(tableName);
        TableMeta newMeta = new TableMeta(tableName, newColumns);
        newMeta.setIndexes(preservedIndexes);
        metaManager.createTable(newMeta);
        RecordFileHandle fileHandle = recordManager.OpenFile(tableName);
        for (Value[] oldRow : oldRows) {
            ByteBuf buffer = Unpooled.buffer();
            int oldIndex = 0;
            for (ColumnMeta column : newColumns) {
                Value value;
                if (appendedValue != null && oldIndex >= oldRow.length) {
                    value = appendedValue;
                } else {
                    while (dropIndex != null && oldIndex == dropIndex) {
                        oldIndex++;
                    }
                    value = oldIndex < oldRow.length ? oldRow[oldIndex++] : defaultValue(column.type);
                }
                writeFixedValue(buffer, value, column.len);
            }
            fileHandle.InsertRecord(buffer);
        }
        recordManager.CloseFile(fileHandle);
        persistRuntimeState();
        rebuildIndexesForTable(tableName);
    }

    private ArrayList<ColumnMeta> copyColumns(List<ColumnMeta> columns) {
        ArrayList<ColumnMeta> result = new ArrayList<>();
        int offset = 0;
        for (ColumnMeta column : columns) {
            result.add(new ColumnMeta(column.tableName, column.name, column.type, column.len, offset));
            offset += column.len;
        }
        return result;
    }

    private int recordSize(List<ColumnMeta> columns) {
        int size = 0;
        for (ColumnMeta column : columns) {
            size += column.len;
        }
        return size;
    }

    private ValueType valueType(ColDataType colDataType) throws DBException {
        if (colDataType.getDataType().equalsIgnoreCase("char")
                || colDataType.getDataType().equalsIgnoreCase("varchar")) {
            return ValueType.CHAR;
        }
        if (colDataType.getDataType().equalsIgnoreCase("int")) {
            return ValueType.INTEGER;
        }
        if (colDataType.getDataType().equalsIgnoreCase("float")
                || colDataType.getDataType().equalsIgnoreCase("double")) {
            return ValueType.FLOAT;
        }
        throw new DBException(ExceptionTypes.UnsupportedCommand(colDataType.toString()));
    }

    private int valueLength(ColDataType colDataType) throws DBException {
        ValueType type = valueType(colDataType);
        if (type == ValueType.CHAR) {
            return Value.CHAR_SIZE;
        }
        if (type == ValueType.INTEGER) {
            return Value.INT_SIZE;
        }
        if (type == ValueType.FLOAT) {
            return Value.FLOAT_SIZE;
        }
        throw new DBException(ExceptionTypes.UnsupportedCommand(colDataType.toString()));
    }

    private Value defaultValue(ValueType type) {
        return switch (type) {
            case CHAR -> new Value("");
            case INTEGER -> new Value(0L);
            case FLOAT -> new Value(0.0);
            default -> new Value("");
        };
    }

    private void writeFixedValue(ByteBuf buffer, Value value, int len) {
        byte[] bytes = value.ToByte();
        buffer.writeBytes(bytes, 0, Math.min(bytes.length, len));
        for (int i = bytes.length; i < len; i++) {
            buffer.writeByte(0);
        }
    }

    /**
     * Creates a new table in the database with specified name and column metadata.
     * This method sets up both the table metadata and the physical storage
     * structure.
     *
     * @param table_name The name of the table to be created
     * @param columns    List of column metadata defining the table structure
     * @throws DBException If there is an error during table creation
     */
    public void createTable(String table_name, ArrayList<ColumnMeta> columns) throws DBException {
        TableMeta tableMeta = new TableMeta(
                table_name, columns);
        metaManager.createTable(tableMeta);
        String table_folder = String.format("%s/%s", diskManager.getCurrentDir(), table_name);
        File file_folder = new File(table_folder);
        if (!file_folder.exists()) {
            file_folder.mkdirs();
        }
        int record_size = 0;
        for (var col : columns) {
            record_size += col.len;
        }
        String data_file = String.format("%s/%s", table_name, "data");
        recordManager.CreateFile(data_file, record_size);
    }

    public void createIndex(String indexName, String tableName, String columnName) throws DBException {
        TableMeta tableMeta = metaManager.getTable(tableName);
        if (!tableMeta.hasColumn(columnName)) {
            throw new DBException(ExceptionTypes.ColumnDoesNotExist(columnName));
        }
        IndexMeta indexMeta = new IndexMeta(indexName, tableName, columnName, TableMeta.IndexType.BTREE);
        metaManager.createIndex(indexMeta);
        rebuildIndex(indexMeta);
        persistRuntimeState();
        Logger.info("Successfully created index {} on {}({})", indexName, tableName, columnName);
        Logger.info("\n{}", runtimeIndexes.get(indexName).printTree());
    }

    public void dropIndex(String indexName) throws DBException {
        metaManager.dropIndex(indexName);
        runtimeIndexes.remove(indexName);
        persistRuntimeState();
        Logger.info("Successfully dropped index: {}", indexName);
    }

    public InMemoryOrderedIndex getIndex(String indexName) throws DBException {
        ensureRuntimeIndexes();
        return runtimeIndexes.get(indexName);
    }

    public IndexMeta findIndexOnColumn(String tableName, String columnName) throws DBException {
        return metaManager.getTable(tableName).findIndexOnColumn(columnName);
    }

    public void insertIndexEntries(String tableName, RID rid, Value[] values) throws DBException {
        ensureRuntimeIndexes();
        TableMeta tableMeta = metaManager.getTable(tableName);
        for (IndexMeta indexMeta : tableMeta.getIndexes().values()) {
            runtimeIndexes.get(indexMeta.indexName).insert(values[columnIndex(tableMeta, indexMeta.columnName)], rid);
        }
    }

    public void deleteIndexEntries(String tableName, RID rid, Value[] values) throws DBException {
        ensureRuntimeIndexes();
        TableMeta tableMeta = metaManager.getTable(tableName);
        for (IndexMeta indexMeta : tableMeta.getIndexes().values()) {
            InMemoryOrderedIndex index = runtimeIndexes.get(indexMeta.indexName);
            if (index != null) {
                index.delete(values[columnIndex(tableMeta, indexMeta.columnName)], rid);
            }
        }
    }

    public void updateIndexEntries(String tableName, RID rid, Value[] oldValues, Value[] newValues) throws DBException {
        ensureRuntimeIndexes();
        TableMeta tableMeta = metaManager.getTable(tableName);
        for (IndexMeta indexMeta : tableMeta.getIndexes().values()) {
            int columnIndex = columnIndex(tableMeta, indexMeta.columnName);
            InMemoryOrderedIndex index = runtimeIndexes.get(indexMeta.indexName);
            if (index != null) {
                index.delete(oldValues[columnIndex], rid);
                index.insert(newValues[columnIndex], rid);
            }
        }
    }

    /**
     * Drops a table from the database by removing its metadata and associated
     * files.
     *
     * @param table_name The name of the table to be dropped
     * @throws DBException If the table directory does not exist or encounters IO
     *                     errors during deletion
     */
    public void dropTable(String table_name) throws DBException {
        if (!isTableExists(table_name)) {
            throw new DBException(ExceptionTypes.TableDoesNotExist(table_name));
        }
        bufferPool.DeleteAllPages(String.format("%s/%s", table_name, "data"));
        bufferPool.Reset();
        TableMeta tableMeta = metaManager.getTable(table_name);
        for (String indexName : new ArrayList<>(tableMeta.getIndexes().keySet())) {
            runtimeIndexes.remove(indexName);
        }
        metaManager.dropTable(table_name);
        diskManager.DeleteFile(String.format("%s/%s", table_name, "data"));
        File tableDir = new File(String.format("%s/%s", diskManager.getCurrentDir(), table_name));
        if (tableDir.exists()) {
            deleteDirectory(tableDir);
        }
        persistRuntimeState();
        Logger.info("Successfully dropped table: {}", table_name);
    }

    /**
     * Recursively deletes a directory and all its contents.
     * If the given file is a directory, it first deletes all its entries
     * recursively.
     * Finally deletes the file/directory itself.
     *
     * @param file The file or directory to be deleted
     * @throws IOException If deletion of any file or directory fails
     */
    private void deleteDirectory(File file) throws DBException {
        if (file.isDirectory()) {
            File[] entries = file.listFiles();
            if (entries != null) {
                for (File entry : entries) {
                    deleteDirectory(entry);
                }
            }
        }
        if (!file.delete()) {
            throw new DBException(ExceptionTypes.BadIOError("File deletion failed: " + file.getAbsolutePath()));
        }
    }

    /**
     * Checks if a table exists in the database.
     *
     * @param table the name of the table to check
     * @return true if the table exists, false otherwise
     */
    public boolean isTableExists(String table) {
        return metaManager.getTableNames().contains(table);
    }

    /**
     * Closes the database manager and performs cleanup operations.
     * This method flushes all pages in the buffer pool, dumps disk manager
     * metadata,
     * and saves meta manager state to JSON format.
     *
     * @throws DBException if an error occurs during the closing process
     */
    public void closeDBManager() throws DBException {
        this.bufferPool.FlushAllPages(null);
        DiskManager.dump_disk_manager_meta(this.diskManager);
        this.metaManager.saveToJson();
    }

    public void beginTransaction() throws DBException {
        transactionManager.begin();
    }

    public void commitTransaction() throws DBException{
        transactionManager.commit();
    }

    public void rollbackTransaction() throws DBException {
        transactionManager.rollback();
    }

    public void savepoint(String savepointName) throws DBException {
        transactionManager.savepoint(savepointName);
    }

    public void rollbackToSavepoint(String savepointName) throws DBException {
        transactionManager.rollbackToSavepoint(savepointName);
    }

    public void releaseSavepoint(String savepointName) throws DBException {
        transactionManager.releaseSavepoint(savepointName);
    }

    public void persistRuntimeState() throws DBException {
        this.bufferPool.FlushAllPages("");
        DiskManager.dump_disk_manager_meta(this.diskManager);
        this.metaManager.saveToJson();
    }

    public void reloadRuntimeState() throws DBException {
        this.bufferPool.Reset();
        this.diskManager.reloadMeta();
        this.metaManager.reloadFromJson();
        rebuildAllIndexes();
    }

    private void ensureRuntimeIndexes() throws DBException {
        for (String tableName : metaManager.getTableNames()) {
            TableMeta tableMeta = metaManager.getTable(tableName);
            for (IndexMeta indexMeta : tableMeta.getIndexes().values()) {
                if (!runtimeIndexes.containsKey(indexMeta.indexName)) {
                    rebuildIndex(indexMeta);
                }
            }
        }
    }

    private void rebuildAllIndexes() throws DBException {
        runtimeIndexes.clear();
        for (String tableName : metaManager.getTableNames()) {
            rebuildIndexesForTable(tableName);
        }
    }

    private void rebuildIndexesForTable(String tableName) throws DBException {
        TableMeta tableMeta = metaManager.getTable(tableName);
        for (IndexMeta indexMeta : tableMeta.getIndexes().values()) {
            rebuildIndex(indexMeta);
        }
    }

    private void rebuildIndex(IndexMeta indexMeta) throws DBException {
        InMemoryOrderedIndex index = new InMemoryOrderedIndex();
        runtimeIndexes.put(indexMeta.indexName, index);
        edu.sustech.cs307.physicalOperator.SeqScanOperator scanner =
                new edu.sustech.cs307.physicalOperator.SeqScanOperator(indexMeta.tableName, this);
        scanner.Begin();
        try {
            while (scanner.hasNext()) {
                scanner.Next();
                if (scanner.Current() instanceof edu.sustech.cs307.tuple.TableTuple tuple) {
                    Value value = tuple.getValue(new TabCol(indexMeta.tableName, indexMeta.columnName));
                    if (value != null) {
                        index.insert(value, tuple.getRID());
                    }
                }
            }
        } finally {
            scanner.Close();
        }
    }

    private int columnIndex(TableMeta tableMeta, String columnName) throws DBException {
        for (int i = 0; i < tableMeta.columns_list.size(); i++) {
            if (tableMeta.columns_list.get(i).name.equalsIgnoreCase(columnName)) {
                return i;
            }
        }
        throw new DBException(ExceptionTypes.ColumnDoesNotExist(columnName));
    }
}
