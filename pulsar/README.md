# Streaming Database Changes to Amazon Pulsar Using Debezium

[Debezium](http://debezium.io/) allows to capture and stream change events from multiple databases such as MySQL and Postgres.

This demo shows how to stream changes from MySQL database running on a local machine to an Apache Pulsar Topic.

## Prerequisites

* Java 8 development environment
* Local [Docker](https://www.docker.com/) installation to run the source database
* [jq](https://stedolan.github.io/jq/) 1.6 installed

## Running the Demo

### Starting the MySQL Source Database

We will start a pre-populated MySQL database that is the same as used by the Debezium [tutorial](http://debezium.io/docs/tutorial/):

```
docker run -it --rm --name mysql -p 3306:3306 -e MYSQL_ROOT_PASSWORD=debezium -e MYSQL_USER=mysqluser -e MYSQL_PASSWORD=mysqlpw debezium/example-mysql:0.8
```

### Preparing the environment


### Creating the Pulsar Topic



### Connecting the Database to Pulsar

Start the application that uses Debezium Embedded to get change events from database to Pulsar stream.
```
mvn exec:java
```

### Reading Events From the Pulsar Topic



```
{
  "before": null,
  "after": {
    "id": 1001,
    "first_name": "Sally",
    "last_name": "Thomas",
    "email": "sally.thomas@acme.com"
  },
  "source": {
    "version": "0.7.4",
    "name": "kinesis",
    "server_id": 0,
    "ts_sec": 0,
    "gtid": null,
    "file": "mysql-bin.000003",
    "pos": 154,
    "row": 0,
    "snapshot": true,
    "thread": null,
    "db": "inventory",
    "table": "customers"
  },
  "op": "c",
  "ts_ms": 1520513267424
}
{
  "before": null,
  "after": {
    "id": 1002,
    "first_name": "George",
    "last_name": "Bailey",
    "email": "gbailey@foobar.com"
  },
  "source": {
    "version": "0.7.4",
    "name": "kinesis",
    "server_id": 0,
    "ts_sec": 0,
    "gtid": null,
    "file": "mysql-bin.000003",
    "pos": 154,
    "row": 0,
    "snapshot": true,
    "thread": null,
    "db": "inventory",
    "table": "customers"
  },
  "op": "c",
  "ts_ms": 1520513267424
}
...
```
