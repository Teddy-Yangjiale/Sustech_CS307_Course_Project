# CS307 Project2 中文演示脚本

本文件按 Project2 PDF 的评分点组织。演示时建议运行 `edu.sustech.cs307.DBEntry`，然后把下面的 SQL **一行一行复制执行**。

说明：

- SQL 都放在 `sql` 代码块中，便于复制。
- Task 1 的 LRU / Clock 页面替换不是 SQL 功能，用 JUnit 命令验证。
- 每个评分点都在最后的“覆盖清单”中对应到具体演示章节，避免遗漏。

## 0. 构建和 JUnit 验证

这一项用于验证 Task 1 的 LRU / Clock，以及 Task 2-4 的自动化测试。

```powershell
& 'D:\IntelliJ IDEA Community Edition 2025.1.1\plugins\maven\lib\maven3\bin\mvn.cmd' -q test
```

预期结果：Maven 正常结束，退出码为 0。

覆盖内容：

- `LRUReplacerTest`
- `ClockReplacerTest`
- `Task2BasicFunctionTest`
- `Task2AdvancedFunctionTest`
- `Task3IndexTest`
- `TransactionManagerTest`

## 1. 清理演示环境

正式演示前可以先运行。不存在的表或索引会报错，但程序应该记录错误并继续运行。

```sql
drop index idx_students_id;
drop index idx_students_age;
drop index idx_scores_sid;
drop table students;
drop table scores;
drop table tx_demo;
```

## 2. Task 2 基础 DDL：CREATE TABLE 和数据类型

验证 `create table`，以及 PDF 要求的 `int`、`varchar`、`double` 类型。

```sql
create table students(id int, name varchar, age int, gpa double);
create table scores(sid int, course varchar, score int);
```

## 3. 数据准备：插入超过 30 行数据

验证 `insert`，并满足 PDF 中“至少一张测试表超过 30 行”的要求。这里 `students` 表插入 36 行。

```sql
insert into students (id, name, age, gpa) values (1, 'alice', 18, 3.60);
insert into students (id, name, age, gpa) values (2, 'bob', 19, 3.65);
insert into students (id, name, age, gpa) values (3, 'carol', 20, 3.72);
insert into students (id, name, age, gpa) values (4, 'david', 21, 3.10);
insert into students (id, name, age, gpa) values (5, 'eva', 22, 3.88);
insert into students (id, name, age, gpa) values (6, 'frank', 18, 2.95);
insert into students (id, name, age, gpa) values (7, 'grace', 19, 3.42);
insert into students (id, name, age, gpa) values (8, 'henry', 20, 3.05);
insert into students (id, name, age, gpa) values (9, 'irene', 21, 3.91);
insert into students (id, name, age, gpa) values (10, 'jack', 22, 2.80);
insert into students (id, name, age, gpa) values (11, 'kate', 18, 3.33);
insert into students (id, name, age, gpa) values (12, 'leo', 19, 3.76);
insert into students (id, name, age, gpa) values (13, 'mona', 20, 3.18);
insert into students (id, name, age, gpa) values (14, 'nick', 21, 3.55);
insert into students (id, name, age, gpa) values (15, 'olivia', 22, 3.99);
insert into students (id, name, age, gpa) values (16, 'paul', 18, 2.71);
insert into students (id, name, age, gpa) values (17, 'queen', 19, 3.47);
insert into students (id, name, age, gpa) values (18, 'rose', 20, 3.84);
insert into students (id, name, age, gpa) values (19, 'sam', 21, 3.29);
insert into students (id, name, age, gpa) values (20, 'tina', 22, 3.67);
insert into students (id, name, age, gpa) values (21, 'uma', 18, 3.01);
insert into students (id, name, age, gpa) values (22, 'victor', 19, 3.22);
insert into students (id, name, age, gpa) values (23, 'wendy', 20, 3.50);
insert into students (id, name, age, gpa) values (24, 'xavier', 21, 3.95);
insert into students (id, name, age, gpa) values (25, 'yara', 22, 2.99);
insert into students (id, name, age, gpa) values (26, 'zack', 18, 3.44);
insert into students (id, name, age, gpa) values (27, 'amy', 19, 3.73);
insert into students (id, name, age, gpa) values (28, 'brian', 20, 3.26);
insert into students (id, name, age, gpa) values (29, 'cindy', 21, 3.81);
insert into students (id, name, age, gpa) values (30, 'derek', 22, 3.12);
insert into students (id, name, age, gpa) values (31, 'ellen', 18, 3.58);
insert into students (id, name, age, gpa) values (32, 'felix', 19, 2.88);
insert into students (id, name, age, gpa) values (33, 'gina', 20, 3.62);
insert into students (id, name, age, gpa) values (34, 'harry', 21, 3.36);
insert into students (id, name, age, gpa) values (35, 'ivy', 22, 3.93);
insert into students (id, name, age, gpa) values (36, 'jason', 18, 3.07);
```

插入 Join 侧数据。

```sql
insert into scores (sid, course, score) values (1, 'db', 86);
insert into scores (sid, course, score) values (2, 'db', 91);
insert into scores (sid, course, score) values (3, 'db', 78);
insert into scores (sid, course, score) values (5, 'db', 95);
insert into scores (sid, course, score) values (8, 'db', 66);
insert into scores (sid, course, score) values (13, 'db', 88);
insert into scores (sid, course, score) values (21, 'db', 73);
insert into scores (sid, course, score) values (24, 'db', 99);
insert into scores (sid, course, score) values (35, 'db', 92);
```

## 4. Task 2 基础 DDL：SHOW TABLES 和 DESCRIBE

验证 `show tables` 和 `describe table`。

```sql
show tables;
describe students;
describe scores;
```

## 5. Task 2 基础查询：SELECT * 和投影

验证全表扫描和任意列投影。

```sql
select * from students;
select students.id, students.name from students;
select id, name from students;
```

## 6. Task 2 WHERE：等值、范围、AND、OR、任意列

验证 `where` 条件解析和过滤执行。

```sql
select students.id, students.name from students where students.age = 19;
select students.id, students.name from students where students.gpa > 3.80;
select students.id, students.name from students where students.age >= 20 and students.gpa <= 3.50;
select students.id, students.name from students where students.age < 19 or students.gpa >= 3.90;
select students.id, students.name from students where students.name = 'alice';
```

## 7. Task 2 EXPLAIN：查询计划展示

验证逻辑计划输出。

```sql
explain select students.id, students.name from students where students.age > 18;
explain select students.id from students where students.age >= 20 and students.gpa <= 3.50;
```

## 8. Task 2 UPDATE

验证带条件更新，并通过后续查询确认更新生效。

```sql
update students set students.name = 'apple' where students.id = 1;
select students.id, students.name from students where students.id = 1;
update students set students.gpa = 4.00 where students.id = 24;
select students.id, students.gpa from students where students.id = 24;
```

## 9. Task 2 DELETE

验证带条件删除。这里删除不影响后续核心演示的数据行。

```sql
select count(*) from students;
delete from students where students.id = 36;
select count(*) from students;
select * from students where students.id = 36;
```

## 10. Task 2 COUNT：带条件计数

验证 `count(*)` 和条件过滤组合。

```sql
select count(*) from students;
select count(*) from students where students.age >= 20;
select count(*) from students where students.age >= 20 and students.gpa > 3.50;
select count(*) from students where students.age = 18 or students.age = 22;
```

## 11. Task 2 Advanced：MAX、MIN、GROUP BY

验证聚合函数和分组。

```sql
select max(students.gpa) from students;
select min(students.gpa) from students;
select students.age, count(*) from students group by students.age order by students.age;
select students.age, max(students.gpa), min(students.gpa), count(*) from students group by students.age order by students.age;
```

## 12. Task 2 Advanced：ORDER BY

验证单列排序和多列排序。

```sql
select students.id, students.name, students.gpa from students order by students.gpa desc;
select students.id, students.age from students order by students.age asc, students.id desc;
```

## 13. Task 2 Advanced：Nested Loop Join

验证等值 Join。

```sql
select students.id, students.name, scores.score from students join scores on students.id = scores.sid;
select students.id, scores.score from students join scores on students.id = scores.sid where scores.score >= 90 order by scores.score desc;
```

## 14. Task 2 Advanced：IN、NOT IN、EXISTS、NOT EXISTS

验证子查询和集合谓词。

```sql
select students.id, students.name from students where students.id in (1, 3, 5) order by students.id;
select students.id, students.name from students where students.id in (select scores.sid from scores) order by students.id;
select students.id, students.name from students where students.id not in (select scores.sid from scores) order by students.id;
select students.id, students.name from students where exists (select scores.sid from scores where scores.sid = students.id) order by students.id;
select students.id, students.name from students where not exists (select scores.sid from scores where scores.sid = students.id) order by students.id;
```

## 15. Task 2 Advanced：Partial ALTER TABLE

验证 `alter table add column`、新 schema 下插入和查询、`alter table drop column`。

```sql
alter table students add column bonus int;
describe students;
insert into students (id, name, age, gpa, bonus) values (37, 'kelly', 20, 3.45, 10);
select students.id, students.bonus from students where students.id = 37;
alter table students drop column bonus;
describe students;
select students.id, students.name, students.age, students.gpa from students where students.id = 37;
```

## 16. Task 3 Index：创建索引、打印 B+Tree、走索引扫描

创建索引时程序应打印 B+Tree 节点；`explain` 应显示简单索引谓词会选择 `IndexScanOperator`。

```sql
create index idx_students_id on students(id);
explain select * from students where students.id = 24;
select * from students where students.id = 24;
```

## 17. Task 3 Index：范围查询

验证索引范围查询。

```sql
select students.id, students.name from students where students.id >= 30;
select students.id, students.name from students where students.id < 5;
select students.id, students.name from students where students.id >= 10 and students.id <= 15;
```

## 18. Task 3 Index：多个索引

验证同一运行中创建和使用多个索引。

```sql
create index idx_students_age on students(age);
create index idx_scores_sid on scores(sid);
explain select students.id, students.name from students where students.age = 22;
select students.id, students.name from students where students.age = 22;
explain select scores.sid, scores.score from scores where scores.sid = 24;
select scores.sid, scores.score from scores where scores.sid = 24;
```

## 19. Task 3 Index：动态 INSERT / DELETE / UPDATE 维护索引

验证一次运行中 B+Tree 随 DML 动态更新。

```sql
insert into students (id, name, age, gpa) values (100, 'newbie', 23, 3.21);
select students.id, students.name from students where students.id = 100;
update students set students.id = 101 where students.id = 100;
select students.id, students.name from students where students.id = 100;
select students.id, students.name from students where students.id = 101;
delete from students where students.id = 101;
select students.id, students.name from students where students.id = 101;
```

## 20. Task 3 Index：DROP INDEX 和回退到普通计划

删除索引后，查询结果仍应正确；`explain` 不应再因为 `idx_students_id` 选择 `IndexScanOperator`。

```sql
drop index idx_students_id;
explain select * from students where students.id = 24;
select * from students where students.id = 24;
```

## 21. Task 4 Transaction：BEGIN 和 COMMIT

验证提交后的修改保留。

```sql
create table tx_demo(id int, note varchar);
begin;
insert into tx_demo (id, note) values (1, 'commit_row');
commit;
select * from tx_demo;
```

## 22. Task 4 Transaction：ROLLBACK

验证回滚到 `begin` 时的快照。

```sql
begin;
insert into tx_demo (id, note) values (2, 'rollback_row');
select * from tx_demo;
rollback;
select * from tx_demo;
```

## 23. Task 4 Transaction：SAVEPOINT 和 ROLLBACK TO SAVEPOINT

验证保存点和回滚到保存点。

```sql
begin;
insert into tx_demo (id, note) values (3, 'before_sp');
savepoint sp1;
insert into tx_demo (id, note) values (4, 'after_sp');
select * from tx_demo;
rollback to savepoint sp1;
select * from tx_demo;
commit;
select * from tx_demo;
```

## 24. Task 4 Transaction：RELEASE SAVEPOINT

验证释放保存点。

```sql
begin;
savepoint sp_release;
insert into tx_demo (id, note) values (5, 'release_row');
release savepoint sp_release;
commit;
select * from tx_demo;
```

## 25. Task 4 Transaction：Rollback 后重建运行时索引

验证事务回滚后，索引元数据保留，运行时内存索引会按磁盘状态重建。

```sql
create index idx_students_id on students(id);
begin;
insert into students (id, name, age, gpa) values (200, 'rollback_index', 24, 3.14);
select * from students where students.id = 200;
rollback;
select * from students where students.id = 200;
select * from students where students.id = 24;
```

## 26. Task 5 展示要求：命令行接口和异常处理

这些语句故意包含错误命令。预期：程序输出错误日志，但继续接受后续命令。

```sql
describe not_existing_table;
select * from students where students.id = 24;
drop table not_existing_table;
show tables;
```

## 27. 最终清理

演示结束后如需清理数据库，再运行这些命令。

```sql
drop index idx_students_id;
drop index idx_students_age;
drop index idx_scores_sid;
drop table tx_demo;
drop table scores;
drop table students;
show tables;
```

## 覆盖清单

| PDF 要求 | 演示章节 |
|---|---|
| LRU 页面替换 | 第 0 节 JUnit |
| Clock 页面替换 | 第 0 节 JUnit |
| CREATE TABLE 支持 int/varchar/double | 第 2 节 |
| 表和元数据持久化 | 第 2-4 节；也可在第 4 节前重启程序验证 |
| 至少 30 行数据 | 第 3 节 |
| INSERT | 第 3 节 |
| UPDATE | 第 8 节 |
| DELETE | 第 9 节 |
| SHOW TABLES | 第 4 节 |
| DESCRIBE TABLE | 第 4 节 |
| DROP TABLE | 第 27 节 |
| EXPLAIN | 第 7、16、18、20 节 |
| 投影 ProjectOperator | 第 5 节 |
| SeqScan 和 SELECT * | 第 5 节 |
| WHERE 等值、范围、AND、OR | 第 6 节 |
| COUNT 带条件 | 第 10 节 |
| MAX / MIN / GROUP BY | 第 11 节 |
| ORDER BY | 第 12 节 |
| Nested Loop Join | 第 13 节 |
| IN / NOT IN / EXISTS / NOT EXISTS | 第 14 节 |
| Partial ALTER TABLE | 第 15 节 |
| 不同 SQL 生成不同查询计划 | 第 7、16、20 节 |
| CREATE INDEX / DROP INDEX | 第 16、20 节 |
| In-memory B+Tree 节点打印 | 第 16 节创建索引时观察输出 |
| 多索引 | 第 18 节 |
| 索引动态 insert/delete/update 维护 | 第 19 节 |
| 事务 rollback/savepoint/release | 第 21-25 节 |
| 命令行接口和异常处理 | 第 26 节 |
