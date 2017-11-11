# Presto-Kudu
The [Presto](https://prestodb.io/) Kudu connector allows querying, inserting and deleting data in [Apache Kudu](https://kudu.apache.org/) 

## Compatibility
The connector is compatible with Kudu version 1.4.0 and 1.5.0, and Presto versions >= 0.181

## Installation

Please follow the below steps to query Apache Kudu in Presto.

### Deploying Presto server
Install Presto according to the documentation: https://prestodb.io/docs/current/installation/deployment.html

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
On creating a Kudu table you need to specify a lot of information about 
the primary key, encoding, and compression of columns and hash or range partitioning,
and the number of replicas. 
Because of restrictions of Presto `CREATE TABLE` syntax, creating a Kudu table in Presto 
is therefore a little bit cumbersome. (TODO: provide more details)
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
 

## Data Types
As far as possible, the connector supports all Kudu data types.
TODO

