# Presto-Kudu
The [Presto](https://prestodb.io/) Kudu connector allows querying, inserting and deleting data in [Apache Kudu](https://kudu.apache.org/) 

## Compatibility
The connector is compatible with Kudu version 1.4.0 and 1.5.0, and Presto versions >= 0.181

## Installation

Please follow the below steps to query Apache Kudu in Presto.

### Deploying Presto server
Install Presto according to the documentation: https://prestodb.io/docs/current/installation/deployment.html

### Download Presto-Kudu connector
[Download current release](https://github.com/MartinWeindel/presto-kudu/wiki/Download)

### Configuring Apache Kudu connector
* Create a folder named `kudu` at `$PRESTO_HOME$/plugin`
* Extract the content of `presto-kudu-xxx.zip` to this folder
* Create a file name `kudu.properties` in `$PRESTO_HOME/etc/catalog/`: 
  ```
  connector.name=kudu
  
  ## List of Kudu master addresses, at least one is needed (comma separated)
  ## Supported formats: example.com, example.com:7051, 192.0.2.1, 192.0.2.1:7051,
  ##                    [2001:db8::1], [2001:db8::1]:7051, 2001:db8::1
  kudu.client.master-addresses=localhost
  
  ## Optional restriction of tablets for specific tenant.
  ## If a tenant is set, only Kudu tablets starting with `<tenant>.` will
  ## be visible in Presto
  #kudu.session.tenant=mytenant
  
  #######################
  ### Advanced Kudu Java client configuration
  #######################
  
  ## Default timeout used for administrative operations (e.g. createTable, deleteTable, etc.)
  #kudu.client.defaultAdminOperationTimeout = 30s
  
  ## Default timeout used for user operations
  #kudu.client.defaultOperationTimeout = 30s
  
  ## Default timeout to use when waiting on data from a socket
  #kudu.client.defaultSocketReadTimeout = 10s
  
  ## Disable Kudu client's collection of statistics.
  #kudu.client.disableStatistics = false
  ```
  

### Query kudu in CLI of presto
* Download presto cli client following: https://prestodb.io/docs/current/installation/cli.html

* Start CLI:
  
  ```bash
  ./presto-cli --server localhost:8086 --catalog kudu --schema default
  ```
  Replace the hostname, port and schema name with your own.

## Querying Data
A Kudu table named `mytable` is available in Presto as table `kudu.default.mytable`.
A Kudu table containing a dot is considered as a schema/table combination, e.g.
`dev.mytable` is mapped to the Presto table `kudu.dev.mytable.
Only Kudu table names in lower case are currently supported.

Before using any tablets, it is needed to create the default schema, e.g.
```sql
CREATE SCHEMA default;
```

### Example
- Create default schema if needed:
```sql
CREATE SCHEMA IF NOT EXISTS default;
```

- Now you can use any Kudu table, if it is lower case and contains no dots.
- Alternatively you can create a users table with
```sql
CREATE TABLE users (
  user_id int,
  first_name varchar,
  last_name varchar
) WITH (
 column_design = '{"user_id": {"key": true}}',
 partition_design = '{"hash":[{"columns":["user_id"], "buckets": 2}]}',
 num_replicas = 1
); 
```
On creating a Kudu table you must/can specify addition information about 
the primary key, encoding, and compression of columns and hash or range partitioning,
and the number of replicas. Details see below in section "Create Kudu Table".

- The table can be described using
```sql
DESCRIBE kudu.default.users;
```
You should get something like
```
   Column   |  Type   |                               Extra                               | Comment 
------------+---------+-------------------------------------------------------------------+---------
 user_id    | integer | key, encoding=AUTO_ENCODING, compression=DEFAULT_COMPRESSION      |         
 first_name | varchar | nullable, encoding=AUTO_ENCODING, compression=DEFAULT_COMPRESSION |         
 last_name  | varchar | nullable, encoding=AUTO_ENCODING, compression=DEFAULT_COMPRESSION |         
(3 rows)
```

- Insert some data with
```sql
INSERT INTO users VALUES (1, 'Donald', 'Duck'), (2, 'Mickey', 'Mouse');
```

- Select the inserted data
```sql
SELECT * FROM users;
```
 

## Data Type Mapping
The data types of Presto and Kudu are mapped as far as possible:

| Presto Data Type | Kudu Data Type | Comment |
| ---------------- | -------------- | ------- |
| `BOOLEAN` | `BOOL` | |
| `TINYINT` | `INT8` | |
| `SMALLINT` | `INT16` | |
| `INTEGER` | `INT32` | |
| `BIGINT` | `INT64` | |
| `REAL` | `FLOAT` | |
| `DOUBLE` | `DOUBLE` | |
| `REAL` | `FLOAT` | |
| `VARCHAR` | `STRING` | see note 1 |
| `VARBINARY` | `BINARY` | see note 1 |
| `TIMESTAMP` | `UNIXTIME_MICROS` | µs resolution in Kudu column is reduced to ms resolution |
| `DECIMAL` | - | not supported |
| `CHAR` | - | not supported |
| `DATE` | - | not supported, see note 2 |
| `TIME` | - | not supported |
| `JSON` | - | not supported |
| `TIME WITH TIMEZONE` | - | not supported |
| `TIMESTAMP WITH TIMEZONE` | - | not supported |
| `INTERVAL YEAR TO MONTH` | - | not supported |
| `INTERVAL DAY TO SECOND` | - | not supported |
| `ARRAY` | - | not supported |
| `MAP` | - | not supported |
| `IPADDRESS` | - | not supported |

#### Note 1
On performing `CREATE TABLE ... AS ...` from a Presto table to Kudu, 
the optional maximum length is lost 

#### Note 2
On performing `CREATE TABLE ... AS ...` from a Presto table to Kudu, 
a `DATE` column is converted to `STRING` 

## Supported Presto SQL statements
| Presto SQL statement | Supported | Comment |
| -------------------- | --------- | ------- |
| `SELECT` | [x] | |
| `INSERT INTO ... VALUES` | [x] | behaves like `upsert` |
| `INSERT INTO ... SELECT ... ` | [x] | behaves like `upsert` |
| `DELETE` | [x] | |
| `CREATE SCHEMA` | [x] | |
| `DROP SCHEMA` | [x] | |
| `CREATE TABLE` | [x] | |
| `CREATE TABLE ... AS` | [x] | |
| `DROP TABLE` | [x] | |
| `ALTER TABLE ... RENAME TO ...` | [x] | |
| `ALTER TABLE ... RENAME COLUMN ...` | [x] | if not part of primary key |
| `ALTER TABLE ... ADD COLUMN ...` | [x] | |
| `ALTER TABLE ... DROP COLUMN ...` | [x] | if not part of primary key |
| `SHOW SCHEMAS` | [x] | |
| `SHOW TABLES` | [x] | |
| `SHOW COLUMNS FROM` | [x] | |
| `DESCRIBE` | [x] | same as `SHOW COLUMNS FROM`|

Currently not supported are `SHOW PARTITIONS FROM ...`, `ALTER SCHEMA ... RENAME`

## Create Kudu Table with `CREATE TABLE`
On creating a Kudu Table you need to provide following table properties:
- `column_design`
- `partition_design`
- `num_replicas` (optional, defaults to 3)

Example:
```sql
CREATE TABLE users (
  user_id int,
  first_name varchar,
  last_name varchar
) WITH (
 column_design = '{"user_id": {"key": true}}',
 partition_design = '{"hash":[{"columns":["user_id"], "buckets": 2}]}',
 num_replicas = 1
); 
```

### Table property `column_design`
With the column design table property you define the columns for the primary key.
Additionally you can overwrite the encoding and compression of every single column.

The value of this property must be a string of a valid JSON object.
The keys are the columns and the values is a JSON object with the columns properties 
to set, i.e. 

```
'{"<column name>": {"<column property name>": <value>, ...}, ...}'`
```

| Column property name | Value | Comment |
| -------------------- | ----- | ------- |
| `key`                | `true` or `false` | if column belongs to primary key, default: `false` |
| `nullable`           | `true` or `false` | if column is nullable, default: `true` for non-key columns, key columns must not be nullable |
| `encoding` | "string value" | See Apache Kudu documentation: [Column encoding](https://kudu.apache.org/docs/schema_design.html#encoding) | 
| `compression` | "string value" | See Apache Kudu documentation: [Column encoding](https://kudu.apache.org/docs/schema_design.html#compression) |

Example:
```
'{"column1": {"key": true, "encoding": "dictionary", "compression": "LZ4"}, "column2": {...}}'
```

### Table property `partition_design`
With the partition design table property you define the partition layout.
In Apache Kudu you can define multiple hash partitions and zero or one range partition.
Details see Apache Kudu documentation: [Partitioning](https://kudu.apache.org/docs/schema_design.html#partitioning)

The value of this property must be a string of a valid JSON object.
The keys are either `hash` or `range` or both, i.e.
 
```
'{"hash": [{...},...], "range": {...}}'`
```
#### Hash partitioning
You can provide multiple hash partition groups in Apache Kudu.
Each group consists of a list of column names and the number of buckets.

Example:
```
'{"hash": [{"columns": ["region", "name"], "buckets": 5}]}'
```
This defines a hash partition with the columns "region" and "name", 
distributed over 5 buckets. All partition columns must be part of
the primary key.

#### Range partitioning
You can provide at most one range partition in Apache Kudu.
It consists of a list of columns. The ranges themselves can
currently not be created with Presto SQL.

Example:
```
'{"range": {"columns": ["event_time"]}}'
```

Creates a range partition on the column "event".

## Known limitations
- Only lower case table and column names in Kudu are supported
- As schemas are not directly supported by Kudu, a special table named `$schemas`
  is created in Kudu when using this connector 
- Creating tables with range partitioning is incomplete. You have to use
  other Kudu clients for this case.

## Build
The Presto-Kudu connector is a standard Maven project. Simply run the following 
command from the project root directory:

```bash
mvn -DskipTests clean package
```

To run the build with tests, it is assumed that Kudu master server 
(and at least one Kudu tablet server) runs on localhost.
If you have Docker installed on your machine, you can use following steps:
```bash
docker run --rm -d --name apache-kudu --net=host usuresearch/apache-kudu
mvn clean package
docker stop apache-kudu
```
