# Presto-Kudu
The [Presto](https://prestodb.io/) Kudu connector allows querying, inserting and deleting data in [Apache Kudu](https://kudu.apache.org/) 

## Compatibility

| Version | Compatibility | Details       |
| ------- | --------------| ------------- |
| Apache Kudu 1.7.0/1.7.1 | yes | tests ok |
| Apache Kudu 1.6.0 | yes | by full API- and ABI-compatibility of Kudu Java Client 1.7.0 |
| Apache Kudu 1.5.0 | yes | by full API- and ABI-compatibility of Kudu Java Client 1.7.0 |
| Apache Kudu 1.4.0 | yes | by full API- and ABI-compatibility of Kudu Java Client 1.7.0 |
|  |  | | |
| Presto 0.208 | yes | tests ok |

Support for older Presto versions see [release history](https://github.com/MartinWeindel/presto-kudu/releases)

## Installation

Please follow the below steps to query Apache Kudu in Presto.

### Deploying Kudu server
Follow installation guide at [Apache Kudu](https://kudu.apache.org/).

If you want to deploy Kudu 1.7.1 on RHE 7 or CentOS 7, you may also be
interessed in my binary build project [kudu-rpm](https://github.com/MartinWeindel/kudu-rpm/releases/tag/v1.7.0-1).

### Deploying Presto server
Install Presto according to the documentation: https://prestodb.io/docs/current/installation/deployment.html

### Download Presto-Kudu connector
[Download current release](https://github.com/MartinWeindel/presto-kudu/wiki/Download)

### Configuring Apache Kudu connector
* Go to the directory `$PRESTO_HOME$/plugin`
* Extract the content of `presto-kudu-XXX.zip` to this folder
* Rename the extracted folder `presto-kudu-XXX` to `kudu`
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
| `VARCHAR` | `STRING` | see note 1 |
| `VARBINARY` | `BINARY` | see note 1 |
| `TIMESTAMP` | `UNIXTIME_MICROS` | µs resolution in Kudu column is reduced to ms resolution |
| `DECIMAL` | `DECIMAL` | only supported for Kudu server >= 1.7.0 |
| `CHAR` | - | not supported, see note 2 |
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
a `DATE` or `CHAR` column is converted to `STRING` 

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
| `SHOW CREATE TABLE` | [x] | |
| `SHOW COLUMNS FROM` | [x] | |
| `DESCRIBE` | [x] | same as `SHOW COLUMNS FROM`|
| `CALL kudu.system.add_range_partition` | [x] | add range partition to an existing table |
| `CALL kudu.system.drop_range_partition` | [x] | drop an existing range partition from a table |

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
| `compression` | "string value" | See Apache Kudu documentation: [Column compression](https://kudu.apache.org/docs/schema_design.html#compression) |

Example:
```
'{"column1": {"key": true, "encoding": "dictionary", "compression": "LZ4"}, "column2": {...}}'
```

### Table property `partition_design`
With the partition design table property you define the partition layout.
In Apache Kudu you can define multiple hash partitions and at most one range partition.
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
It consists of a list of columns. The ranges themselves are given either
in the table property `range_partitions`. Alternatively, the
procedures `kudu.system.add_range_partition` and `kudu.system.drop_range_partition`
can be used to manage range partitions for existing tables.
For both ways see below for more details.

Example:
```
'{"range": {"columns": ["event_time"]}}'
```

Defines range partitioning on the column "event".

To add concrete range partitions use either the table property `range_partitions`
or call the procedure .

### Table property `range_partitions`
With the `range_partitions` table property you specify the concrete range partitions to be
created. The range partition definition itself must be given in the table 
property `partition_design` separately.



Example:
```sql
CREATE TABLE events (
  serialno varchar,
  event_time timestamp,
  message varchar
) WITH (
 column_design = '{"serialno": {"key": true}, "event_time": {"key": true}}',
 partition_design = '{"hash":[{"columns":["serialno"], "buckets": 4}],
                      "range": {"columns":["event_time"]}}',
 range_partitions = '[{"lower": null, "upper": "2017-01-01T00:00:00"},
                      {"lower": "2017-01-01T00:00:00", "upper": "2017-07-01T00:00:00"},
                      {"lower": "2017-07-01T00:00:00", "upper": "2018-01-01T00:00:00"}]',
 num_replicas = 1
); 
```
This creates a table with a hash partition on column `serialno` with 4 buckets and range partitioning
on column `event_time`. Additionally three range partitions are created:
  1. for all event_times before the year 2017 (lower bound = `null` means it is unbound)
  2. for the first half of the year 2017
  3. for the second half the year 2017
This means any try to add rows with `event_time` of year 2018 or greater will fail,
as no partition is defined.

#### Managing range partitions
For existing tables, there are procedures to add and drop a range partition.

- adding a range partition
```sql
CALL kudu.system.add_range_partition(<schema>, <table>, <range_partition_as_json_string>), 
```

- dropping a range partition
```sql
CALL kudu.system.drop_range_partition(<schema>, <table>, <range_partition_as_json_string>) 
```

- `<schema>`: schema of the table
- `<table>`: table names
- `<range_partition_as_json_string>`: lower and upper bound of the range partition
  as json string in the form `'{"lower": <value>, "upper": <value>}'`, or if the range
  partition has multiple columns: `'{"lower": [<value_col1>,...], "upper": [<value_col1>,...]}'`.
  The concrete literal for lower and upper bound values are depending on the
  column types.
  
  Examples:
  
  | Presto Data Type | JSON string example |
  | ---------------- | ------------------- |
  | BIGINT           | '{"lower": 0, "upper": 1000000}' | 
  | SMALLINT         | '{"lower": 10, "upper": null}' | 
  | VARCHAR          | '{"lower": "A", "upper": "M"}' | 
  | TIMESTAMP        | '{"lower": "2018-02-01T00:00:00.000", "upper": "2018-02-01T12:00:00.000"}' | 
  | BOOLEAN          | '{"lower": false, "upper": true}' | 
  | VARBINARY        | values encoded as base64 strings |
  
  To specified an unbounded bound, use the value `null`. 


Example:
```sql
CALL kudu.system.add_range_partition('myschema', 'events', '{"lower": "2018-01-01", "upper": "2018-06-01"}')  
```
This would add a range partition for a table `events` in the schema `myschema` with
the lower bound `2018-01-01` (more exactly `2018-01-01T00:00:00.000`) and the upper bound `2018-07-01`.

Use the sql statement `SHOW CREATE TABLE` to request the existing range partitions (they are shown
in the table property `range_partitions`). 

## Known limitations
- Only lower case table and column names in Kudu are supported
- As schemas are not directly supported by Kudu, a special table named `$schemas`
  is created in Kudu when using this connector 
- Using a secured Kudu cluster has not been tested.

## Build
The Presto-Kudu connector is a standard Maven project.
Under Linux, simply run the following command from the project root directory:

```bash
mvn -DskipTests clean package
```

Building the package under Windows is currently not supported, as the maven plugin 
`maven-presto-plugin` has an open issue with Windows.

To run the build with tests, it is assumed that Kudu master server 
(and at least one Kudu tablet server) runs on localhost.
If you have Docker installed on your machine, you can use following steps:
```bash
docker run --rm -d --name apache-kudu --net=host usuresearch/kudu-docker-slim:release-v1.7.1-1
mvn clean package
docker stop apache-kudu
```
