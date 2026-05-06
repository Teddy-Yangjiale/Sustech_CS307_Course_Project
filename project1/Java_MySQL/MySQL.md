# MySQL

#### 1. 运行环境与配置

- **数据库版本**: MySQL Server 8.4.8 LTS。
- **连接驱动**: `mysql-connector-j-8.4.0`。
- **配置优化**: 启用 JDBC 核心参数 `rewriteBatchedStatements=true`，以显著降低大批量数据写入时的网络通信延迟。

#### 2. Schema 设计与 SQL 方言适配

- **主键策略**: PostgreSQL 原生序列 `BIGSERIAL` 迁移至 MySQL `BIGINT AUTO_INCREMENT` 属性。
- **数值精度**: 统一采用 `DECIMAL(12,2)` 替代 `NUMERIC` 数据类型，以满足票价等敏感字段的十进制精度要求。
- **模式匹配**: 移除 PostgreSQL 特有的 `ILIKE` 操作符，利用 MySQL `utf8mb4_unicode_ci` 字符集的默认大小写不敏感特性，改写为标准 `LIKE` 查询。

#### 3. 业务逻辑 (Client) 重构

- **DML 语法适配**: 因 MySQL 缺乏对 `UPDATE...RETURNING` 子句的支持，涉及状态变更与数据读取的复合操作（如订单取消逻辑）被重构为两阶段事务：
  1. 执行 `SELECT` 查询目标记录（获取 `flight_id`）。
  2. 执行 `UPDATE` 变更订单状态并执行库存回滚。
- **并发控制**: 依赖 InnoDB 存储引擎的行级锁（Row-level Locking）机制，通过 `WHERE remain_count > 0` 的条件约束保障高并发场景下库存扣减的数据一致性。

#### 4. 数据导入架构与策略对比

在应用层实现上，两者均采用标准的 **JDBC 批处理 (Batch Processing)** 架构：通过构建 `PreparedStatement`，禁用 `autoCommit` 以开启手动事务控制，并设定固定阈值（如每 5000 条记录）触发 `executeBatch()`。

底层的冲突处理（Upsert）机制差异如下：

- **PostgreSQL**: 采用 `ON CONFLICT (key) DO NOTHING` 语法，依赖显式的唯一性约束声明进行严格校验。
- **MySQL**: 采用 `INSERT IGNORE` 语法，当触发主键或唯一约束异常时，默认忽略该行的插入操作。

#### 5. 导入性能基准测试

测试条件：对比 PostgreSQL 默认配置、MySQL 默认配置，以及 MySQL 启用驱动层批处理重写后的批量数据加载耗时。

| **数据表 (规模)**           | **PostgreSQL 耗时** | **MySQL (默认配置)** | **MySQL (配置优化后)** |
| --------------------------- | ------------------- | -------------------- | ---------------------- |
| **Route (2.1万条)**         | 1.4s                | 8.7s                 | 2.1s                   |
| **Flight (10.8万条)**       | 5.0s                | 40.9s                | 4.1s                   |
| **Flight Cabin (21.7万条)** | 18.2s               | 93.0s                | **8.6s**               |
| **总计估算 (约35万条)**     | **~24.8s**          | **~145.1s**          | **~16.1s**             |

#### 6. 底层执行原理剖析

- **网络 I/O 与批处理机制**: MySQL JDBC 驱动默认将 `executeBatch()` 解析为离散的单条 SQL 发送，产生高昂的网络 I/O 开销。配置 `rewriteBatchedStatements=true` 后，驱动在客户端将多行数据合并为单条多值插入语句（如 `INSERT VALUES (),(),...`），有效降低了网络通信往返次数（Round-trip time）。
- **传输协议差异**: PostgreSQL 原生支持高效的二进制流式传输（Binary Protocol），在默认配置下其大事务处理的 I/O 效率较高。
- **冲突检查开销**: 在处理超大规模数据集（如 21 万行的 `Flight Cabin` 表）时，MySQL 的 `IGNORE` 机制依赖 InnoDB 缓冲池（Buffer Pool）执行内存级校验；相较于 PostgreSQL 的 `ON CONFLICT` 机制，其在此特定高并发插入场景下的计算开销相对较低。