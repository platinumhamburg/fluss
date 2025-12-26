---
sidebar_label: Aggregation
title: Aggregation Merge Engine
sidebar_position: 4
---

# Aggregation Merge Engine

## Overview

The **Aggregation Merge Engine** is designed for scenarios where users only care about aggregated results rather than individual records. It aggregates each value field with the latest data one by one under the same primary key according to the specified aggregate function.

Each field not part of the primary keys can be assigned an aggregate function using the Schema API (recommended) or connector options (`'fields.<field-name>.agg'`). If no function is specified for a field, it will use `last_value_ignore_nulls` aggregation as the default behavior.

This merge engine is useful for real-time aggregation scenarios such as:
- Computing running totals and statistics
- Maintaining counters and metrics
- Tracking maximum/minimum values over time
- Building real-time dashboards and analytics

## Configuration

To enable the aggregation merge engine, set the following table property:

```
'table.merge-engine' = 'aggregation'
```

Then specify the aggregate function for each non-primary key field using connector options:

```
'fields.<field-name>.agg' = '<function-name>'
```

**Note**: The recommended way is to use Schema API (see section "API Usage" below). The connector option is provided as an alternative for connector-specific scenarios.

## API Usage

### Creating a Table with Aggregation

```java
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;

// Create connection
Connection conn = Connection.create(config);
Admin admin = conn.getAdmin();

// Define schema with aggregation functions (recommended way)
import org.apache.fluss.metadata.AggFunction;

Schema schema = Schema.newBuilder()
    .column("product_id", DataTypes.BIGINT())
    .column("price", DataTypes.DOUBLE(), AggFunction.MAX)
    .column("sales", DataTypes.BIGINT(), AggFunction.SUM)
    .column("last_update_time", DataTypes.TIMESTAMP(3))  // Defaults to LAST_VALUE_IGNORE_NULLS
    .primaryKey("product_id")
    .build();

// Create table with aggregation merge engine
TableDescriptor tableDescriptor = TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .build();

TablePath tablePath = TablePath.of("my_database", "product_stats");
admin.createTable(tablePath, tableDescriptor, false).get();
```

### Writing Data

```java
// Get table
Table table = conn.getTable(tablePath);

// Create upsert writer
UpsertWriter writer = table.newUpsert().createWriter();

// Write data - these will be aggregated
writer.upsert(row(1L, 23.0, 15L, timestamp1));
writer.upsert(row(1L, 30.2, 20L, timestamp2)); // Same primary key - triggers aggregation

writer.flush();
```

**Result after aggregation:**
- `product_id`: 1
- `price`: 30.2 (max of 23.0 and 30.2)
- `sales`: 35 (sum of 15 and 20)
- `last_update_time`: timestamp2 (last non-null value)

## Supported Aggregate Functions

Fluss currently supports the following aggregate functions:

### sum

Aggregates values by computing the sum across multiple rows.

- **Supported Data Types**: TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, DECIMAL
- **Behavior**: Adds incoming values to the accumulator
- **Null Handling**: Null values are ignored

**Example:**
```java
Schema schema = Schema.newBuilder()
    .column("id", DataTypes.BIGINT())
    .column("amount", DataTypes.DECIMAL(10, 2), AggFunction.SUM)
    .primaryKey("id")
    .build();

TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .build();

// Input: (1, 100.50), (1, 200.75)
// Result: (1, 301.25)
```

### product

Computes the product of values across multiple rows.

- **Supported Data Types**: TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, DECIMAL
- **Behavior**: Multiplies incoming values with the accumulator
- **Null Handling**: Null values are ignored

**Example:**
```java
Schema schema = Schema.newBuilder()
    .column("id", DataTypes.BIGINT())
    .column("discount_factor", DataTypes.DOUBLE(), AggFunction.PRODUCT)
    .primaryKey("id")
    .build();

TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .build();

// Input: (1, 0.9), (1, 0.8)
// Result: (1, 0.72) -- 90% * 80% = 72%
```

### max

Identifies and retains the maximum value.

- **Supported Data Types**: CHAR, STRING, TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, DECIMAL, DATE, TIME, TIMESTAMP, TIMESTAMP_LTZ
- **Behavior**: Keeps the larger value between accumulator and incoming value
- **Null Handling**: Null values are ignored

**Example:**
```java
Schema schema = Schema.newBuilder()
    .column("id", DataTypes.BIGINT())
    .column("max_temperature", DataTypes.DOUBLE(), AggFunction.MAX)
    .column("max_reading_time", DataTypes.TIMESTAMP(3), AggFunction.MAX)
    .primaryKey("id")
    .build();

TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .build();

// Input: (1, 25.5, '2024-01-01 10:00:00'), (1, 28.3, '2024-01-01 11:00:00')
// Result: (1, 28.3, '2024-01-01 11:00:00')
```

### min

Identifies and retains the minimum value.

- **Supported Data Types**: CHAR, STRING, TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, DECIMAL, DATE, TIME, TIMESTAMP, TIMESTAMP_LTZ
- **Behavior**: Keeps the smaller value between accumulator and incoming value
- **Null Handling**: Null values are ignored

**Example:**
```java
Schema schema = Schema.newBuilder()
    .column("id", DataTypes.BIGINT())
    .column("lowest_price", DataTypes.DECIMAL(10, 2), AggFunction.MIN)
    .primaryKey("id")
    .build();

TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .build();

// Input: (1, 99.99), (1, 79.99), (1, 89.99)
// Result: (1, 79.99)
```

### last_value

Replaces the previous value with the most recently received value.

- **Supported Data Types**: All data types
- **Behavior**: Always uses the latest incoming value
- **Null Handling**: Null values will overwrite previous values

**Example:**
```java
Schema schema = Schema.newBuilder()
    .column("id", DataTypes.BIGINT())
    .column("status", DataTypes.STRING(), AggFunction.LAST_VALUE)
    .column("last_login", DataTypes.TIMESTAMP(3), AggFunction.LAST_VALUE)
    .primaryKey("id")
    .build();

TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .build();

// Input: (1, 'online', '2024-01-01 10:00:00'), (1, 'offline', '2024-01-01 11:00:00')
// Result: (1, 'offline', '2024-01-01 11:00:00')
```

### last_value_ignore_nulls

Replaces the previous value with the latest non-null value. This is the **default aggregate function** when no function is specified.

- **Supported Data Types**: All data types
- **Behavior**: Uses the latest incoming value only if it's not null
- **Null Handling**: Null values are ignored, previous value is retained

**Example:**
```java
Schema schema = Schema.newBuilder()
    .column("id", DataTypes.BIGINT())
    .column("email", DataTypes.STRING(), AggFunction.LAST_VALUE_IGNORE_NULLS)
    .column("phone", DataTypes.STRING(), AggFunction.LAST_VALUE_IGNORE_NULLS)
    .primaryKey("id")
    .build();

TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .build();

// Input: (1, 'user@example.com', '123-456'), (1, null, '789-012')
// Result: (1, 'user@example.com', '789-012')
// Email remains 'user@example.com' because the second upsert had null email
```

### first_value

Retrieves and retains the first value seen for a field.

- **Supported Data Types**: All data types
- **Behavior**: Keeps the first received value, ignores all subsequent values
- **Null Handling**: Null values are retained if received first

**Example:**
```java
Schema schema = Schema.newBuilder()
    .column("id", DataTypes.BIGINT())
    .column("first_purchase_date", DataTypes.DATE(), AggFunction.FIRST_VALUE)
    .column("first_product", DataTypes.STRING(), AggFunction.FIRST_VALUE)
    .primaryKey("id")
    .build();

TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .build();

// Input: (1, '2024-01-01', 'ProductA'), (1, '2024-02-01', 'ProductB')
// Result: (1, '2024-01-01', 'ProductA')
```

### first_value_ignore_nulls

Selects the first non-null value in a data set.

- **Supported Data Types**: All data types
- **Behavior**: Keeps the first received non-null value, ignores all subsequent values
- **Null Handling**: Null values are ignored until a non-null value is received

**Example:**
```java
Schema schema = Schema.newBuilder()
    .column("id", DataTypes.BIGINT())
    .column("email", DataTypes.STRING(), AggFunction.FIRST_VALUE_IGNORE_NULLS)
    .column("verified_at", DataTypes.TIMESTAMP(3), AggFunction.FIRST_VALUE_IGNORE_NULLS)
    .primaryKey("id")
    .build();

TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .build();

// Input: (1, null, null), (1, 'user@example.com', '2024-01-01 10:00:00'), (1, 'other@example.com', '2024-01-02 10:00:00')
// Result: (1, 'user@example.com', '2024-01-01 10:00:00')
```

### listagg

Concatenates multiple string values into a single string with a delimiter.

- **Supported Data Types**: STRING, CHAR
- **Behavior**: Concatenates values using the specified delimiter
- **Null Handling**: Null values are skipped
- **Configuration**: Use `'table.merge-engine.aggregate.<field-name>.listagg-delimiter'` to specify a custom delimiter (default is comma `,`)

**Example:**
```java
Schema schema = Schema.newBuilder()
    .column("id", DataTypes.BIGINT())
    .column("tags", DataTypes.STRING(), AggFunction.LISTAGG)
    .primaryKey("id")
    .build();

TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .property("table.merge-engine.aggregate.tags.listagg-delimiter", ";")
    .build();

// Input: (1, 'developer'), (1, 'java'), (1, 'flink')
// Result: (1, 'developer;java;flink')
```

### string_agg

Alias for `listagg`. Concatenates multiple string values into a single string with a delimiter.

- **Supported Data Types**: STRING, CHAR
- **Behavior**: Same as `listagg` - concatenates values using the specified delimiter
- **Null Handling**: Null values are skipped
- **Configuration**: Use `'table.merge-engine.aggregate.<field-name>.listagg-delimiter'` to specify a custom delimiter (default is comma `,`)

**Example:**
```java
Schema schema = Schema.newBuilder()
    .column("id", DataTypes.BIGINT())
    .column("tags", DataTypes.STRING(), AggFunction.STRING_AGG)
    .primaryKey("id")
    .build();

TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .property("table.merge-engine.aggregate.tags.listagg-delimiter", ";")
    .build();

// Input: (1, 'developer'), (1, 'java'), (1, 'flink')
// Result: (1, 'developer;java;flink')
```

### bool_and

Evaluates whether all boolean values in a set are true (logical AND).

- **Supported Data Types**: BOOLEAN
- **Behavior**: Returns true only if all values are true
- **Null Handling**: Null values are ignored

**Example:**
```java
Schema schema = Schema.newBuilder()
    .column("id", DataTypes.BIGINT())
    .column("has_all_permissions", DataTypes.BOOLEAN(), AggFunction.BOOL_AND)
    .primaryKey("id")
    .build();

TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .build();

// Input: (1, true), (1, true), (1, false)
// Result: (1, false) -- Not all values are true
```

### bool_or

Checks if at least one boolean value in a set is true (logical OR).

- **Supported Data Types**: BOOLEAN
- **Behavior**: Returns true if any value is true
- **Null Handling**: Null values are ignored

**Example:**
```java
Schema schema = Schema.newBuilder()
    .column("id", DataTypes.BIGINT())
    .column("has_any_alert", DataTypes.BOOLEAN(), AggFunction.BOOL_OR)
    .primaryKey("id")
    .build();

TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .build();

// Input: (1, false), (1, false), (1, true)
// Result: (1, true) -- At least one value is true
```

## Advanced Configuration

### Default Aggregate Function

You can set a default aggregate function for all non-primary key fields that don't have an explicitly specified function:

```java
Schema schema = Schema.newBuilder()
    .column("id", DataTypes.BIGINT())
    .column("col1", DataTypes.STRING())  // Defaults to LAST_VALUE_IGNORE_NULLS
    .column("col2", DataTypes.BIGINT(), AggFunction.SUM)  // Explicitly set to SUM
    .primaryKey("id")
    .build();

TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .build();
```

In this example:
- `col2` uses `sum` aggregation (explicitly specified)
- `col1` uses `last_value_ignore_nulls` as the default

### Partial Update with Aggregation

The aggregation merge engine supports partial updates through the UpsertWriter API. When performing a partial update:

- **Target columns**: These columns will be aggregated according to their configured aggregate functions
- **Non-target columns**: These columns will retain their existing values from the old row

**Example:**

```java
Schema schema = Schema.newBuilder()
    .column("id", DataTypes.BIGINT())
    .column("count1", DataTypes.BIGINT(), AggFunction.SUM)
    .column("count2", DataTypes.BIGINT(), AggFunction.SUM)
    .column("sum1", DataTypes.DOUBLE(), AggFunction.SUM)
    .column("sum2", DataTypes.DOUBLE(), AggFunction.SUM)
    .primaryKey("id")
    .build();

TableDescriptor tableDescriptor = TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .build();

// Create partial update writer targeting only id, count1, and sum1
int[] targetColumns = new int[]{0, 1, 3}; // id, count1, sum1
UpsertWriter partialWriter = table.newUpsert()
    .withPartialUpdate(targetColumns)
    .createWriter();

// When writing:
// - count1 and sum1 will be aggregated with existing values
// - count2 and sum2 will remain unchanged
```

**Use cases for partial aggregation**:
1. **Independent metrics**: When different processes update different subsets of metrics for the same key
2. **Reduced data transfer**: Only send the columns that need to be updated
3. **Flexible pipelines**: Different data sources can contribute to different aggregated fields

### Delete Behavior

The aggregation merge engine provides limited support for delete operations. You can configure the behavior using the `'table.agg.remove-record-on-delete'` option:

```java
TableDescriptor.builder()
    .schema(schema)
    .property("table.merge-engine", "aggregation")
    .property("table.agg.remove-record-on-delete", "true")  // Default is false
    .build();
```

**Configuration options**:
- **`'table.agg.remove-record-on-delete' = 'false'`** (default): Delete operations will cause an error
- **`'table.agg.remove-record-on-delete' = 'true'`**: Delete operations will remove the entire record from the table

:::note
**Current Limitation**: The aggregation merge engine does not support retraction semantics (e.g., subtracting from a sum, reverting a max). Delete operations can only remove the entire record or be rejected.

Future versions may support fine-grained retraction by enhancing the protocol to carry row data with delete operations.
:::

## Performance Considerations

1. **Choose Appropriate Aggregate Functions**: Select functions that match your use case to avoid unnecessary computations
2. **Primary Key Design**: Use appropriate primary keys to ensure proper grouping of aggregated data
3. **Null Handling**: Be aware of how each function handles null values to avoid unexpected results
4. **Delete Handling**: If you need to handle delete operations, be aware that enabling `'table.agg.remove-record-on-delete' = 'true'` will remove entire records rather than retracting aggregated values

## Limitations

:::warning Critical Limitations
When using the `aggregation` merge engine, be aware of the following critical limitations:

### 1. Exactly-Once Semantics

**Fluss engine does not natively support transactional writes, and therefore does not directly support Exactly-Once semantics at the storage layer.**

Exactly-Once semantics should be achieved through integration with compute engines (e.g., Flink, Spark). For example, after failover, undo operations can be generated for invalid writes to achieve rollback.

For detailed information about Exactly-Once implementation, please refer to: [FIP-21: Aggregation Merge Engine](https://cwiki.apache.org/confluence/display/FLUSS/FIP-21%3A+Aggregation+Merge+Engine)

### 2. Delete Operations

By default, delete operations will cause errors:
- You must set `'table.agg.remove-record-on-delete' = 'true'` if you need to handle delete operations
- This configuration will remove the entire aggregated record, not reverse individual aggregations

### 3. Data Type Restrictions

Each aggregate function supports specific data types (see function documentation above)
:::

## See Also

- [Default Merge Engine](./default.md)
- [FirstRow Merge Engine](./first-row.md)
- [Versioned Merge Engine](./versioned.md)
- [Primary Key Tables](../index.md)
- [Fluss Client API](../../../../apis/java-client.md)
