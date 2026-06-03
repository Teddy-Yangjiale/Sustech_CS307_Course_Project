# B+树索引性能测试报告

## 1. 测试目标

本报告用于验证项目中的 in-memory B+ Tree index 是否满足以下要求：

- 使用足够大的数据集测试索引功能。
- 验证索引查询、范围查询、重复键、多索引、动态 insert/delete/update 维护的正确性。
- 对索引构建和查询执行进行计时，观察当前实现的性能表现。

测试对象是 `project2` 中的索引实现，包括：

- `InMemoryOrderedIndex`
- `DBManager.createIndex/dropIndex/getIndex`
- `IndexScanOperator`
- `InsertOperator/DeleteOperator/UpdateOperator` 对索引的动态维护
- `PhysicalPlanner` 对可用索引谓词的选择

## 2. 测试数据规模

本次实际运行的数据规模如下：

| 项目 | 数量 |
| --- | ---: |
| 表数量 | 1 |
| 初始记录数 | 5000 |
| 字段数 | 3 |
| 索引数量 | 2 |
| 动态插入 | 1 条 |
| 动态删除 | 1 条 |
| 动态更新 | 1 条 |

测试表结构：

```sql
create table t(id int, age int, score int);
```

插入数据规律：

- `id`: 1 到 5000，唯一。
- `age`: `id % 10`，制造大量重复键。
- `score`: `id * 2`。

创建的索引：

```sql
create index idx_id on t(id);
create index idx_age on t(age);
```

这可以同时覆盖唯一键索引和重复键索引：

- `idx_id`：5000 个不同 key。
- `idx_age`：10 个不同 key，每个 key 对应大量 RID。

## 3. 测试方法

测试通过一个临时 runner 直接调用项目中的数据库组件完成，测试结束后 runner 文件已删除，不保留在项目中。

测试流程：

1. 创建临时数据库目录。
2. 初始化 `DiskManager`、`BufferPool`、`RecordManager`、`MetaManager`、`DBManager`、`TransactionManager`。
3. 创建表 `t(id int, age int, score int)`。
4. 批量插入 5000 条记录。
5. 在没有索引时重复执行等值查询，记录端到端耗时。
6. 创建 `idx_id` 和 `idx_age` 两个索引，记录构建耗时。
7. 使用索引重复执行等值查询、范围查询、重复键查询。
8. 测试 insert/delete/update 后索引是否同步维护。
9. 删除 `idx_id`，确认 `idx_age` 仍然存在。

注意：这里的耗时是数据库引擎端到端耗时，不是单独调用 B+树 API 的微基准测试。计时包含 SQL 解析、逻辑计划、物理计划、operator 打开/关闭、RecordFile 读取、对象构造等开销。

## 4. 正确性结果

| 验证项 | 查询或操作 | 期望 | 实测 |
| --- | --- | ---: | ---: |
| 初始数据插入 | 插入 5000 条记录 | 5000 | 5000 |
| 等值查询 | `id = 4321` | 1 | 1 |
| 范围查询 | `id >= 4900` | 103 | 103 |
| 重复键查询 | `age = 1` | 501 | 501 |
| insert 后索引维护 | 插入 `id = 5001, age = 1` 后查 `id = 5001` | 1 | 1 |
| delete 后索引维护 | 删除 `id = 2500` 后查 `id = 2500` | 0 | 0 |
| update 后旧 key 删除 | `id = 2501` 更新为 `id = 6001` 后查旧 key | 0 | 0 |
| update 后新 key 插入 | `id = 2501` 更新为 `id = 6001` 后查新 key | 1 | 1 |
| 删除一个索引 | `drop index idx_id` 后剩余索引 | `[idx_age]` | `[idx_age]` |
| 最终记录数 | insert/delete/update 后总记录数 | 5000 | 5000 |

说明：

- 范围查询结果为 103，是因为测试中先插入了 `id = 5001`，又把 `id = 2501` 更新成 `id = 6001`，所以 `id >= 4900` 的结果包括 `4900..5001` 以及 `6001`。
- `age = 1` 的结果为 501，是因为原始 5000 条数据中有 500 条 `age = 1`，动态插入的 `id = 5001` 也是 `age = 1`。

结论：本次测试中，索引的构建、等值查询、范围查询、重复键 RID 列表、insert/delete/update 动态维护、drop index 后多索引状态都通过了正确性验证。

## 5. 性能结果

测试机器运行环境为当前本地开发环境。计时单位为毫秒。

| 测试项 | 操作规模 | 耗时 |
| --- | ---: | ---: |
| 建表 | 1 张表 | 190 ms |
| 插入数据 | 5000 条 | 4431 ms |
| 创建 `idx_id` | 5000 条记录建树 | 216 ms |
| 创建 `idx_age` | 5000 条记录建树 | 222 ms |
| 无索引等值查询 | `id = 4321`，重复 200 次 | 19824 ms |
| 有索引等值查询 | `id = 4321`，重复 200 次 | 14642 ms |
| 删除 `idx_id` 后等值查询 | `id = 4321`，重复 200 次 | 18957 ms |
| 有索引范围查询 | `id >= 4900`，重复 50 次 | 4769 ms |
| 有索引重复键查询 | `age = 1`，重复 10 次 | 932 ms |

## 6. 性能分析

### 6.1 索引构建性能

`idx_id` 和 `idx_age` 都是在 5000 条记录上构建，耗时分别为 216 ms 和 222 ms。

这说明当前 B+树可以在项目规模的数据集上快速完成构建。构建过程会扫描表中已有记录，并把每条记录的索引列值和 RID 插入对应的 `InMemoryOrderedIndex`。

### 6.2 多索引支持

本次测试同时创建了两个索引：

- `idx_id on t(id)`
- `idx_age on t(age)`

两个索引可以同时存在。删除 `idx_id` 后，`idx_age` 仍然保留，实测剩余索引为 `[idx_age]`。

这说明当前代码支持 multiple indexes，并且每个 index name 对应一个独立的 in-memory B+ tree。

### 6.3 动态维护

测试覆盖了三类动态维护：

- insert：插入新记录后，索引中可以查到新 key。
- delete：删除记录后，索引中查不到被删除 key。
- update：旧 key 会从索引中删除，新 key 会插入索引。

这说明当前索引不是只在 `create index` 时静态构建，而是在单次运行期间会随着 DML 操作动态变化。

### 6.4 查询性能表现

从端到端计时看，有索引等值查询更快：

- 无索引等值查询 200 次：19824 ms。
- 有索引等值查询 200 次：14642 ms。
- 删除索引后等值查询 200 次：18957 ms。

主要原因是本次计时包含了完整 SQL 执行链路，而不是只测 B+树查找：

- 每次查询都要经过 SQL parse、logical plan、physical plan。
- `IndexScanOperator` 找到 RID 后仍然要打开 record file 并按 RID 读取记录。
- `DBManager.getIndex()` 会检查运行时索引状态。
- 当前实现不是成本优化器，没有根据数据规模、选择率、缓存状态计算最优计划。
- B+树 order 较小，5000 个不同 key 会产生较多节点层级。
- `create index` 时会打印 B+树结构，构建阶段计时会受到日志输出影响。

因此，当前测试更适合证明索引功能正确、计划能够走 `IndexScanOperator`，以及索引可以在中等数据规模下运行；它不能证明当前端到端 SQL 引擎已经达到真实数据库那种稳定的索引加速效果。

## 7. 当前实现满足情况

| 要求 | 当前结果 |
| --- | --- |
| Recognize `create index xx on t(col)` | 支持 |
| Recognize `drop index xx` | 支持 |
| JSON metadata 被修改 | 支持，索引元数据写入 table metadata |
| 构建 in-memory B+ tree | 支持 |
| 能打印每个节点 | 支持，`create index` 后会打印 tree |
| 支持多个索引 | 支持，本次测试同时创建 `idx_id` 和 `idx_age` |
| 支持多个 B+树 | 支持，每个 index name 对应一个 runtime B+ tree |
| 单次运行中动态创建 B+树 | 支持 |
| insert 动态维护索引 | 支持 |
| delete 动态维护索引 | 支持 |
| update 动态维护索引 | 支持 |
| 大数据量测试 | 本次使用 5000 条记录 |
| 正确性验证 | 通过 |
| 性能验证 | 已计时，索引构建较快，查询端到端加速有限 |

## 8. 结论

本项目的 B+树索引实现能够在 5000 条记录的数据集上正确构建和维护索引。它支持多个 index、多个 in-memory B+ tree、重复 key、范围查询、动态 insert/delete/update 维护，并且可以在 `drop index` 后正确更新索引状态。

性能方面，索引构建耗时约 216 到 222 ms，说明 B+树构建本身在当前数据规模下可接受。但端到端 SQL 查询中，索引等值查询没有明显快于顺序扫描，主要瓶颈来自完整执行链路和当前简单优化器实现，而不是单纯的 B+树查找逻辑。

