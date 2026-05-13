# Changelog

## 2026-05-13 — Task 2 Advanced 补全 & Query Optimizer 增强

### 新增功能

#### 1. 数据类型关键字扩展

- `CREATE TABLE` 现已支持 `VARCHAR` 和 `DOUBLE` 关键字，分别映射到 `CHAR`（64字节）和 `FLOAT`（8字节双精度）。
- `ALTER TABLE ADD COLUMN` 同步支持这两种类型。

涉及文件：
- `src/main/java/edu/sustech/cs307/logicalOperator/ddl/CreateTableExecutor.java`
- `src/main/java/edu/sustech/cs307/system/DBManager.java` (`valueType()` 方法)

#### 2. Query Optimizer — AND 条件分解

- `PhysicalPlanner.extractIndexedPredicate()` 现在能递归遍历 `AND` 表达式树，找到任意一个可走索引的子谓词。
- 对于 `WHERE col = val AND other_condition` 形式的查询，如果 `col` 建有索引，优化器将生成 `FilterOperator(IndexScanOperator, full_where)`，先通过索引缩小范围，再用 `FilterOperator` 过滤剩余条件。
- 简单谓词（非 `AND`）行为不变，直接返回 `IndexScanOperator`。

#### 3. Query Optimizer — 同列范围合并

- 新增 `tryMergeRange()` 方法，检测 `WHERE col > 5 AND col < 10` 形式的同列双边界条件，合并为 `IndexScanOperator + RANGE` 谓词类型，调用 B+ tree 的 `range()` 方法一次完成区间扫描。

涉及文件：
- `src/main/java/edu/sustech/cs307/optimizer/PhysicalPlanner.java`
- `src/main/java/edu/sustech/cs307/physicalOperator/IndexScanOperator.java`（新增 `RANGE` 谓词类型）

### 新增测试

#### Task2AdvancedFunctionTest — NOT IN 子查询

- 新增测试断言：`SELECT students.id FROM students WHERE students.id NOT IN (SELECT scores.id FROM scores)`，验证 `NOT IN` 子查询语义正确，返回不在子查询结果集中的行（期望 `2, 3`）。

涉及文件：
- `src/test/java/system/Task2AdvancedFunctionTest.java`

### 测试结果

全部 **117 个测试通过**，0 失败，BUILD SUCCESS。
