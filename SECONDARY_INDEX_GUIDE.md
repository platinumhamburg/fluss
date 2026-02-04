# Fluss 二级索引使用指南

## 概述

Fluss 二级索引（Secondary Index）是一种全局索引机制，允许用户通过非主键列快速查询数据。二级索引在 Fluss 中作为独立的内部表存储，支持高效的点查询操作。

## 核心特性

- **全局索引**: 索引表独立于主表存储，不受主表分区影响
- **自动同步**: 索引数据与主表数据自动保持一致
- **高可用**: 支持索引复制和故障恢复
- **透明查询**: Flink SQL 维表 JOIN 自动利用索引加速查询

## Flink SQL 使用示例

### 1. 创建带二级索引的表

二级索引通过 `table.secondary-index.columns` 配置项定义：

```sql
-- 创建 Fluss Catalog
CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'localhost:9123'
);

USE CATALOG fluss_catalog;

-- 创建数据库
CREATE DATABASE IF NOT EXISTS my_db;
USE my_db;

-- 创建带二级索引的用户表
-- 索引定义格式: 'table.secondary-index.columns' = '索引列1;索引列2,索引列3'
-- 多个索引用分号(;)分隔，复合索引的列用逗号(,)分隔
-- 注意：索引列会被自动添加 NOT NULL 约束
CREATE TABLE users (
    id INT NOT NULL PRIMARY KEY NOT ENFORCED,
    name STRING,
    email STRING,
    age INT
) WITH (
    'table.secondary-index.columns' = 'name;email',
    'table.secondary-index.bucket.num' = '3'
);
```

上述语句会创建两个索引：
- `idx_name`: 基于 `name` 列的索引
- `idx_email`: 基于 `email` 列的索引

### 2. 创建复合索引

```sql
-- 创建订单表，包含单列索引和复合索引
-- 索引列 user_id, product_id, order_status 会被自动添加 NOT NULL 约束
CREATE TABLE orders (
    order_id BIGINT NOT NULL PRIMARY KEY NOT ENFORCED,
    user_id INT,
    product_id INT,
    order_status STRING,
    order_time TIMESTAMP(3),
    amount DECIMAL(10, 2)
) WITH (
    -- user_id: 单列索引
    -- product_id,order_status: 复合索引（两列组合）
    'table.secondary-index.columns' = 'user_id;product_id,order_status',
    'table.secondary-index.bucket.num' = '6'
);
```

上述语句会创建两个索引：
- `idx_user_id`: 基于 `user_id` 列的单列索引
- `idx_product_id_order_status`: 基于 `product_id` 和 `order_status` 的复合索引

### 3. 写入数据

```sql
-- 插入用户数据（索引会自动更新）
INSERT INTO users VALUES
    (1, 'Alice', 'alice@example.com', 25),
    (2, 'Bob', 'bob@example.com', 30),
    (3, 'Charlie', 'charlie@example.com', 35),
    (4, 'Diana', 'diana@example.com', 28);

-- 插入订单数据
INSERT INTO orders VALUES
    (1001, 1, 100, 'COMPLETED', TIMESTAMP '2024-01-15 10:30:00', 99.99),
    (1002, 2, 101, 'PENDING', TIMESTAMP '2024-01-15 11:00:00', 149.99),
    (1003, 1, 102, 'COMPLETED', TIMESTAMP '2024-01-15 12:00:00', 79.99),
    (1004, 3, 100, 'SHIPPED', TIMESTAMP '2024-01-15 14:00:00', 199.99);
```

### 4. 维表 JOIN 查询（自动使用索引）

```sql
-- 创建事件流表（来自 Kafka 等）
CREATE TABLE user_events (
    event_id BIGINT,
    user_name STRING,
    event_type STRING,
    event_time TIMESTAMP(3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'user_events',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format' = 'json'
);

-- 通过 name 索引进行维表 JOIN
-- Flink 会自动使用 idx_name 索引加速查询
SELECT 
    e.event_id,
    e.user_name,
    e.event_type,
    u.id AS user_id,
    u.email,
    u.age
FROM user_events AS e
JOIN users FOR SYSTEM_TIME AS OF e.event_time AS u
    ON e.user_name = u.name;
```

### 5. 分区表的二级索引

```sql
-- 创建带二级索引的分区表
-- 索引列 customer_name, product_name 会被自动添加 NOT NULL 约束
CREATE TABLE sales (
    id INT NOT NULL,
    customer_name STRING,
    product_name STRING,
    region STRING NOT NULL,
    amount DECIMAL(10, 2),
    PRIMARY KEY (id, region) NOT ENFORCED
) PARTITIONED BY (region) WITH (
    'table.secondary-index.columns' = 'customer_name;product_name',
    'table.secondary-index.bucket.num' = '3',
    'table.auto-partition.enabled' = 'true'
    -- table.auto-partition.time-unit 默认为 'day'，分区值格式为 yyyyMMdd
);

-- 插入销售数据
INSERT INTO sales VALUES
    (1, 'CompanyA', 'ProductX', 'east', 1000.00),
    (2, 'CompanyB', 'ProductY', 'west', 2000.00),
    (3, 'CompanyA', 'ProductZ', 'east', 1500.00),
    (4, 'CompanyC', 'ProductX', 'north', 3000.00);
```

**注意**: 分区表的索引表是非分区的全局索引，索引表会包含主表的分区键列。

### 6. 复杂业务场景示例

```sql
-- 场景：实时订单 enrichment
-- 订单流关联用户信息和商品信息

-- 用户维表（带索引）
-- 索引列 username 会被自动添加 NOT NULL 约束
CREATE TABLE dim_users (
    user_id INT NOT NULL PRIMARY KEY NOT ENFORCED,
    username STRING,
    user_level STRING,
    register_date DATE
) WITH (
    'table.secondary-index.columns' = 'username',
    'table.secondary-index.bucket.num' = '3'
);

-- 商品维表（带索引）
-- 索引列 product_name, category 会被自动添加 NOT NULL 约束
CREATE TABLE dim_products (
    product_id INT NOT NULL PRIMARY KEY NOT ENFORCED,
    product_name STRING,
    category STRING,
    price DECIMAL(10, 2)
) WITH (
    'table.secondary-index.columns' = 'product_name;category',
    'table.secondary-index.bucket.num' = '3'
);

-- 订单流
CREATE TABLE order_stream (
    order_id BIGINT,
    user_id INT,
    product_id INT,
    quantity INT,
    order_time TIMESTAMP(3),
    WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'orders',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format' = 'json'
);

-- 多维表 JOIN enrichment
SELECT 
    o.order_id,
    o.order_time,
    u.username,
    u.user_level,
    p.product_name,
    p.category,
    o.quantity,
    p.price * o.quantity AS total_amount
FROM order_stream AS o
JOIN dim_users FOR SYSTEM_TIME AS OF o.order_time AS u
    ON o.user_id = u.user_id
JOIN dim_products FOR SYSTEM_TIME AS OF o.order_time AS p
    ON o.product_id = p.product_id;
```

## 配置项说明

| 配置项 | 说明 | 示例 |
|--------|------|------|
| `table.secondary-index.columns` | 定义索引列，多个索引用分号分隔，复合索引的列用逗号分隔 | `'name;age,city'` |
| `table.secondary-index.bucket.num` | 索引表的 bucket 数量 | `'3'` |

## 约束与限制

### 1. 表类型约束

| 约束 | 说明 |
|------|------|
| 仅支持主键表 | 二级索引只能在带 PRIMARY KEY 的表上创建 |
| 不支持 Log 表 | 无主键的 Log 表不支持二级索引 |

### 2. 索引列约束

| 约束 | 说明 |
|------|------|
| 自动非空约束 | 所有索引列会被自动添加 NOT NULL 约束 |
| 不能等于主键列 | 索引列不能与主键列完全相同（允许部分重叠） |
| 不能等于分区列 | 索引列不能与分区列完全相同（允许部分重叠） |
| 支持的数据类型 | STRING, INT, BIGINT, FLOAT, DOUBLE, BOOLEAN, DATE, TIMESTAMP 等 |

**示例说明**：
```sql
-- ❌ 错误：索引列完全等于主键列
CREATE TABLE t1 (
    id INT NOT NULL PRIMARY KEY NOT ENFORCED,
    name STRING
) WITH ('table.secondary-index.columns' = 'id');
-- 报错：Index columns cannot be exactly the same as primary key columns

-- ❌ 错误：索引列完全等于分区列
CREATE TABLE t2 (
    id INT NOT NULL,
    dt STRING NOT NULL,
    name STRING,
    PRIMARY KEY (id, dt) NOT ENFORCED
) PARTITIONED BY (dt) WITH (
    'table.secondary-index.columns' = 'dt',
    'table.auto-partition.enabled' = 'true'
);
-- 报错：Index columns cannot be exactly the same as partition columns

-- ✅ 正确：索引列部分包含主键列
CREATE TABLE t3 (
    id INT NOT NULL,
    status STRING NOT NULL,
    name STRING,
    PRIMARY KEY (id, status) NOT ENFORCED
) WITH ('table.secondary-index.columns' = 'id,name');
-- 允许：索引列 (id, name) 与主键列 (id, status) 部分重叠

-- ✅ 正确：索引列部分包含分区列
CREATE TABLE t4 (
    id INT NOT NULL,
    dt STRING NOT NULL,
    name STRING,
    PRIMARY KEY (id, dt) NOT ENFORCED
) PARTITIONED BY (dt) WITH (
    'table.secondary-index.columns' = 'name,dt',
    'table.auto-partition.enabled' = 'true'
);
-- 允许：索引列 (name, dt) 与分区列 (dt) 部分重叠
```

### 3. 分区表约束

| 约束 | 说明 |
|------|------|
| 索引表不分区 | 无论主表是否分区，索引表始终是非分区的全局索引 |
| 自动分区管理 | 带索引的分区表必须启用自动分区 |
| 禁止手动删除分区 | 不能对带索引的表执行 `ALTER TABLE ... DROP PARTITION` |

### 4. DDL 约束

| 约束 | 说明 |
|------|------|
| 创建时定义 | 索引必须在 CREATE TABLE 时通过配置项定义 |
| 不支持 ALTER | 不能通过 ALTER TABLE 添加或删除索引 |
| 不可修改 | 索引定义创建后不可修改 |

### 5. 查询约束

| 约束 | 说明 |
|------|------|
| 仅支持等值查询 | 索引只支持 `=` 条件，不支持范围查询 |
| 维表 JOIN | 索引主要用于加速 Lookup Join（维表 JOIN） |
| 列匹配 | JOIN 条件必须是索引列的等值匹配 |

## 性能建议

### 索引数量
```sql
-- 推荐：每个表不超过 3 个索引
CREATE TABLE recommended_table (
    id INT NOT NULL PRIMARY KEY NOT ENFORCED,
    col1 STRING,
    col2 STRING,
    col3 STRING
) WITH (
    'table.secondary-index.columns' = 'col1;col2;col3',  -- 最多 3 个
    'table.secondary-index.bucket.num' = '3'
);
```

### 选择合适的索引列
```sql
-- 好的索引列选择：查询频率高、选择性好
-- 索引列 user_id, order_status 会被自动添加 NOT NULL 约束
CREATE TABLE good_example (
    id INT NOT NULL PRIMARY KEY NOT ENFORCED,
    user_id INT,           -- 适合：经常用于 JOIN
    order_status STRING,   -- 适合：有限的枚举值
    uuid STRING            -- 不适合：唯一值，选择性太高
) WITH (
    'table.secondary-index.columns' = 'user_id;order_status',
    'table.secondary-index.bucket.num' = '3'
    -- 不建议在 uuid 上创建索引
);
```

## 内部实现

### 索引表命名规则
索引表作为内部表自动创建，命名格式：
```
__{主表名}__index__idx_{排序后的索引列名用下划线连接}
```

例如：
- 表 `users` 上基于 `name` 列的索引 → `__users__index__idx_name`
- 表 `orders` 上基于 `user_id` 列的索引 → `__orders__index__idx_user_id`
- 表 `orders` 上基于 `age,city` 的复合索引 → `__orders__index__idx_age_city`（列名按字母排序）

### 索引表结构

索引表的主键由以下列组成：
- 索引列（作为主键前缀）
- 主表的主键列（用于回查主表）

例如，主表 `users(id, name, email, age)` 上基于 `name` 列的索引表结构：
- 主键: `(name, id)`
- 列: `name`, `id`

### 查看索引表
```sql
-- 索引表是内部表，可以通过 SHOW TABLES 查看
SHOW TABLES;
-- 输出可能包含：
-- users
-- __users__index__idx_name
-- __users__index__idx_email
```

## 故障恢复

- **Leader 切换**: 索引状态会在新 Leader 上自动恢复
- **副本同步**: 索引数据自动在副本间同步
- **一致性保证**: 索引更新与主表更新保持事务一致性

## 常见问题

### Q: 如何确认查询使用了索引？
A: 当 Flink SQL 的 Lookup Join 条件匹配索引列时，会自动使用索引。例如 `ON stream.name = dim.name`，如果 `name` 列有索引，则会使用索引查询。

### Q: 为什么不能在创建表后添加索引？
A: 索引需要与表数据保持一致，后续添加索引需要全量重建，目前版本暂不支持。

### Q: 分区表的索引是如何工作的？
A: 索引表是全局的（非分区），包含所有分区的索引数据。索引表会包含主表的分区键列，查询时会自动路由到正确的分区。

### Q: 索引会影响写入性能吗？
A: 是的，每个索引会增加少量写入开销，建议控制索引数量在 3 个以内。

### Q: 复合索引和多个单列索引有什么区别？
A: 
- 复合索引 `'age,city'`: 创建一个基于 (age, city) 组合的索引，适用于同时按 age 和 city 查询
- 多个单列索引 `'age;city'`: 创建两个独立的索引，分别适用于按 age 或按 city 单独查询
