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

class Task2AdvancedFunctionTest {

    @TempDir
    Path tempDir;

    @Test
    void joinOrderByAndMinMaxAggregates() throws DBException {
        DBManager dbManager = buildDbManager();

        executeStatement(dbManager, "create table students(id int, age int)");
        executeStatement(dbManager, "create table scores(id int, score int)");
        executeStatement(dbManager, "insert into students (id, age) values (1, 18)");
        executeStatement(dbManager, "insert into students (id, age) values (2, 19)");
        executeStatement(dbManager, "insert into students (id, age) values (3, 20)");
        executeStatement(dbManager, "insert into scores (id, score) values (1, 80)");
        executeStatement(dbManager, "insert into scores (id, score) values (3, 90)");

        assertThat(selectFirstColumn(dbManager,
                "select students.id, scores.score from students join scores on students.id = scores.id"))
                .containsExactly(1L, 3L);
        assertThat(selectFirstColumn(dbManager,
                "select students.id from students order by students.age desc"))
                .containsExactly(3L, 2L, 1L);
        assertThat(selectFirstColumn(dbManager,
                "select max(students.age) from students where students.id >= 2"))
                .containsExactly(20L);
        assertThat(selectFirstColumn(dbManager,
                "select min(students.age) from students"))
                .containsExactly(18L);
    }

    @Test
    void groupByInExistsMultiOrderAndAlterTable() throws DBException {
        DBManager dbManager = buildDbManager();

        executeStatement(dbManager, "create table students(id int, age int)");
        executeStatement(dbManager, "create table scores(id int, score int)");
        executeStatement(dbManager, "insert into students (id, age) values (1, 18)");
        executeStatement(dbManager, "insert into students (id, age) values (2, 18)");
        executeStatement(dbManager, "insert into students (id, age) values (3, 20)");
        executeStatement(dbManager, "insert into scores (id, score) values (1, 80)");
        executeStatement(dbManager, "insert into scores (id, score) values (3, 90)");

        assertThat(selectFirstColumn(dbManager,
                "select students.age, count(*) from students group by students.age order by students.age"))
                .containsExactly(18L, 20L);
        assertThat(selectFirstColumn(dbManager,
                "select students.id from students order by students.age asc, students.id desc"))
                .containsExactly(2L, 1L, 3L);
        assertThat(selectFirstColumn(dbManager,
                "select students.id from students where students.id in (1, 3) order by students.id"))
                .containsExactly(1L, 3L);
        assertThat(selectFirstColumn(dbManager,
                "select students.id from students where students.id in (select scores.id from scores) order by students.id"))
                .containsExactly(1L, 3L);
        assertThat(selectFirstColumn(dbManager,
                "select students.id from students where exists (select scores.id from scores where scores.score >= 90) order by students.id"))
                .containsExactly(1L, 2L, 3L);
        assertThat(selectFirstColumn(dbManager,
                "select students.id from students where exists (select scores.id from scores where scores.id = students.id) order by students.id"))
                .containsExactly(1L, 3L);

        executeStatement(dbManager, "alter table students add column grade int");
        executeStatement(dbManager, "insert into students (id, age, grade) values (4, 21, 95)");
        assertThat(selectFirstColumn(dbManager,
                "select students.grade from students where students.id = 4"))
                .containsExactly(95L);

        executeStatement(dbManager, "alter table students drop column grade");
        assertThat(selectFirstColumn(dbManager,
                "select students.id from students where students.age >= 20 order by students.id"))
                .containsExactly(3L, 4L);

        executeStatement(dbManager, "drop table students");
        executeStatement(dbManager, "drop table scores");
        executeStatement(dbManager, "create table students(id int, age int)");
        executeStatement(dbManager, "create table scores(id int, score int)");
        executeStatement(dbManager, "insert into students (id, age) values (1, 18)");
        executeStatement(dbManager, "insert into students (id, age) values (2, 19)");
        executeStatement(dbManager, "insert into students (id, age) values (3, 20)");
        executeStatement(dbManager, "insert into scores (id, score) values (1, 80)");

        assertThat(selectFirstColumn(dbManager,
                "select students.id from students where students.id not in (select scores.id from scores) order by students.id"))
                .containsExactly(2L, 3L);
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
