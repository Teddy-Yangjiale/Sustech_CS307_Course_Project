package system;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.logicalOperator.LogicalOperator;
import edu.sustech.cs307.meta.MetaManager;
import edu.sustech.cs307.optimizer.LogicalPlanner;
import edu.sustech.cs307.optimizer.PhysicalPlanner;
import edu.sustech.cs307.physicalOperator.IndexScanOperator;
import edu.sustech.cs307.physicalOperator.PhysicalOperator;
import edu.sustech.cs307.storage.BufferPool;
import edu.sustech.cs307.storage.DiskManager;
import edu.sustech.cs307.storage.replacer.ClockReplacer;
import edu.sustech.cs307.storage.replacer.PageReplacer;
import edu.sustech.cs307.system.DBManager;
import edu.sustech.cs307.system.RecordManager;
import edu.sustech.cs307.system.TransactionManager;
import edu.sustech.cs307.tuple.Tuple;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.function.IntFunction;

import static org.assertj.core.api.Assertions.assertThat;

class Task3IndexTest {

    @TempDir
    Path tempDir;

    @Test
    void createDropIndexUpdatesMetadataAndPlannerUsesIndexScan() throws DBException {
        DBManager dbManager = buildDbManager();
        executeStatement(dbManager, "create table t(id int, age int)");
        executeStatement(dbManager, "insert into t (id, age) values (1, 18)");
        executeStatement(dbManager, "insert into t (id, age) values (2, 19)");
        executeStatement(dbManager, "create index idx_id on t(id)");

        assertThat(dbManager.getMetaManager().getTable("t").getIndexes()).containsKey("idx_id");
        assertThat(hasIndexScan(plan(dbManager, "select * from t where t.id = 2"))).isTrue();
        assertThat(selectFirstColumn(dbManager, "select * from t where t.id = 2")).containsExactly(2L);

        executeStatement(dbManager, "drop index idx_id");
        assertThat(dbManager.getMetaManager().getTable("t").getIndexes()).doesNotContainKey("idx_id");
        assertThat(hasIndexScan(plan(dbManager, "select * from t where t.id = 2"))).isFalse();
        assertThat(selectFirstColumn(dbManager, "select * from t where t.id = 2")).containsExactly(2L);
    }

    @Test
    void indexTracksInsertDeleteUpdateDuplicatesAndRangePredicates() throws DBException {
        DBManager dbManager = buildDbManager();
        executeStatement(dbManager, "create table t(id int, age int)");
        for (int i = 1; i <= 120; i++) {
            executeStatement(dbManager, String.format("insert into t (id, age) values (%d, %d)", i, i % 10));
        }
        executeStatement(dbManager, "create index idx_id on t(id)");
        executeStatement(dbManager, "create index idx_age on t(age)");

        assertThat(selectFirstColumn(dbManager, "select * from t where t.id = 42")).containsExactly(42L);
        assertThat(selectFirstColumn(dbManager, "select * from t where t.id >= 118")).containsExactly(118L, 119L, 120L);
        assertThat(selectFirstColumn(dbManager, "select * from t where t.id < 4")).containsExactly(1L, 2L, 3L);

        executeStatement(dbManager, "insert into t (id, age) values (121, 1)");
        assertThat(selectFirstColumn(dbManager, "select * from t where t.id = 121")).containsExactly(121L);
        assertThat(selectFirstColumn(dbManager, "select t.id from t where t.age = 1"))
                .contains(1L, 11L, 21L, 121L);

        executeStatement(dbManager, "delete from t where t.id = 42");
        assertThat(selectFirstColumn(dbManager, "select * from t where t.id = 42")).isEmpty();

        executeStatement(dbManager, "update t set t.id = 142 where t.id = 43");
        assertThat(selectFirstColumn(dbManager, "select * from t where t.id = 43")).isEmpty();
        assertThat(selectFirstColumn(dbManager, "select * from t where t.id = 142")).containsExactly(142L);
    }

    @Test
    void rollbackRebuildsRuntimeIndexesFromPersistedState() throws DBException {
        DBManager dbManager = buildDbManager();
        executeStatement(dbManager, "create table t(id int, age int)");
        executeStatement(dbManager, "insert into t (id, age) values (1, 18)");
        executeStatement(dbManager, "create index idx_id on t(id)");

        executeStatement(dbManager, "begin");
        executeStatement(dbManager, "insert into t (id, age) values (2, 19)");
        executeStatement(dbManager, "rollback");

        assertThat(selectFirstColumn(dbManager, "select * from t where t.id = 1")).containsExactly(1L);
        assertThat(selectFirstColumn(dbManager, "select * from t where t.id = 2")).isEmpty();
    }

    private DBManager buildDbManager() throws DBException {
        HashMap<String, Integer> fileOffsets = new HashMap<>();
        DiskManager diskManager = new DiskManager(tempDir.toString(), fileOffsets);
        IntFunction<PageReplacer> replacerFactory = ClockReplacer::new;
        BufferPool bufferPool = new BufferPool(32, diskManager, replacerFactory.apply(32));
        RecordManager recordManager = new RecordManager(diskManager, bufferPool);
        MetaManager metaManager = new MetaManager(tempDir.resolve("meta").toString());
        DBManager dbManager = new DBManager(diskManager, bufferPool, recordManager, metaManager, null,
                replacerFactory);
        dbManager.setTransactionManager(new TransactionManager(dbManager));
        return dbManager;
    }

    private void executeStatement(DBManager dbManager, String sql) throws DBException {
        LogicalOperator logicalOperator = LogicalPlanner.resolveAndPlan(dbManager, sql);
        if (logicalOperator == null) {
            return;
        }
        PhysicalOperator physicalOperator = PhysicalPlanner.generateOperator(dbManager, logicalOperator);
        physicalOperator.Begin();
        while (physicalOperator.hasNext()) {
            physicalOperator.Next();
            physicalOperator.Current();
        }
        physicalOperator.Close();
        dbManager.getBufferPool().FlushAllPages("");
    }

    private PhysicalOperator plan(DBManager dbManager, String sql) throws DBException {
        LogicalOperator logicalOperator = LogicalPlanner.resolveAndPlan(dbManager, sql);
        return PhysicalPlanner.generateOperator(dbManager, logicalOperator);
    }

    private boolean hasIndexScan(PhysicalOperator operator) {
        if (operator instanceof IndexScanOperator) {
            return true;
        }
        try {
            Field child = operator.getClass().getDeclaredField("child");
            child.setAccessible(true);
            Object value = child.get(operator);
            return value instanceof PhysicalOperator physicalOperator && hasIndexScan(physicalOperator);
        } catch (NoSuchFieldException ignored) {
            return false;
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private List<Object> selectFirstColumn(DBManager dbManager, String sql) throws DBException {
        PhysicalOperator physicalOperator = plan(dbManager, sql);
        List<Object> result = new ArrayList<>();
        physicalOperator.Begin();
        while (physicalOperator.hasNext()) {
            physicalOperator.Next();
            Tuple tuple = physicalOperator.Current();
            result.add(tuple.getValues()[0].value);
        }
        physicalOperator.Close();
        return result;
    }
}
