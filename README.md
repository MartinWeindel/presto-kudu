# Presto-Kudu
[Presto connector](https://prestodb.io/) for [Apache Kudu](https://kudu.apache.org/)

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
  
  ```
  $ ./presto --server localhost:8086 --catalog kudu --schema default
  ```
  Replace the hostname, port and schema name with your own.



