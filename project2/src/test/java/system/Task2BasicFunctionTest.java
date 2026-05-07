package system;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.logicalOperator.LogicalOperator;
import edu.sustech.cs307.meta.MetaManager;
import edu.sustech.cs307.optimizer.LogicalPlanner;
import edu.sustech.cs307.optimizer.PhysicalPlanner;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.function.IntFunction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class Task2BasicFunctionTest {

    @TempDir
    Path tempDir;

    @Test
    void basicDdlProjectionWhereCountDeleteAndDrop() throws DBException {
        DBManager dbManager = buildDbManager();

        executeStatement(dbManager, "create table t(id int, age int)");
        executeStatement(dbManager, "insert into t (id, age) values (1, 18)");
        executeStatement(dbManager, "insert into t (id, age) values (2, 19)");
        executeStatement(dbManager, "insert into t (id, age) values (3, 20)");

        assertThatCode(() -> executeStatement(dbManager, "show tables")).doesNotThrowAnyException();
        assertThatCode(() -> executeStatement(dbManager, "describe t")).doesNotThrowAnyException();
        assertThatCode(() -> executeStatement(dbManager, "explain select t.id from t where t.age >= 19")).doesNotThrowAnyException();

        assertThat(selectFirstColumn(dbManager, "select t.id from t where t.age >= 19 or t.id = 1"))
                .containsExactly(1L, 2L, 3L);
        assertThat(selectFirstColumn(dbManager, "select count(*) from t where t.age >= 19"))
                .containsExactly(2L);

        assertThat(selectFirstColumn(dbManager, "delete from t where t.age >= 20"))
                .containsExactly(1L);
        assertThat(selectFirstColumn(dbManager, "select count(*) from t"))
                .containsExactly(2L);

        executeStatement(dbManager, "drop table t");
        assertThatThrownBy(() -> dbManager.getMetaManager().getTable("t"))
                .isInstanceOf(DBException.class);
    }

    private DBManager buildDbManager() throws DBException {
        HashMap<String, Integer> fileOffsets = new HashMap<>();
        DiskManager diskManager = new DiskManager(tempDir.toString(), fileOffsets);
        IntFunction<PageReplacer> replacerFactory = ClockReplacer::new;
        BufferPool bufferPool = new BufferPool(16, diskManager, replacerFactory.apply(16));
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

    private List<Object> selectFirstColumn(DBManager dbManager, String sql) throws DBException {
        LogicalOperator logicalOperator = LogicalPlanner.resolveAndPlan(dbManager, sql);
        PhysicalOperator physicalOperator = PhysicalPlanner.generateOperator(dbManager, logicalOperator);
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
