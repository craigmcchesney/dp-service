## 0. Cloning Repositories and Building

### Environment Assumptions

These instructions assume that Maven and Java are installed on the host where the tests will be run, and that MongoDB is installed and configured for use by the tests.

### Cloning and Building the dp-grpc Repo

The prerequisite for running the tests from the dp-service repo (in addition to a running MongoDB installation) is to clone and build the dp-grpc project repo.  Here are the steps:

1. Clone the dp-grpc repo:
```bash
git clone https://github.com/osprey-dcs/dp-grpc.git
```
2. Change to the dp-grpc directory and run `mvn install`:
```bash
mvn install
```

### Cloning and Building the dp-service Repo

The tests themselves are contained in the dp-service repo.  After building and installing the dp-grpc repo, clone and build the dp-service repo:

1. Clone the dp-service repo:
```bash
git clone https://github.com/osprey-dcs/dp-service.git
```
2. Change to the dp-service directory and run `mvn compile`:
```bash
mvn compile
```



## 1. Overriding Database URI and Database Name

The benchmark servers default to the URI `mongodb://admin:admin@localhost:27017/` and the database name `dp-benchmark`.  You can override either or both of those settings as described below.

Note that unlike the ingestion benchmark (where only the server touches MongoDB), the **query benchmark client also accesses MongoDB directly** to load data before querying.  Both the query server and query client must be pointed at the same database, so the database override settings must be applied when running both applications.

**WARNING: the database with the specified name WILL BE DROPPED and recreated at the start of every benchmark run — DO NOT USE A DATABASE THAT CONTAINS DATA YOU WISH TO KEEP.**

The MongoDB user in the connection URI must have `dbOwner` (or equivalent `dbAdmin` + `readWrite`) privileges on the benchmark database.  A `readWrite`-only user is insufficient because the server drops the entire database on each run.

### Overriding via environment variables

Set `DP_MONGO_DB_URI` and/or `DP_MONGO_BENCHMARK_DB_NAME` in the environment before starting the benchmark server.  The ingestion benchmark client applications connect to the server via gRPC only and do not need these variables set.  The query benchmark client applications access MongoDB directly, so they require these variables as well — see section 6.

```bash
export DP_MONGO_DB_URI="mongodb://benchuser:benchpass@cluster-host:27017/?authSource=admin"
export DP_MONGO_BENCHMARK_DB_NAME="dp-benchmark-functional"

java -Ddp.config=~/data-platform/config/dp.yml \
  -Dlog4j.configurationFile=~/data-platform/config/log4j2.xml \
  -cp ~/data-platform/lib/dp-service.jar \
  com.ospreydcs.dp.service.ingest.benchmark.BenchmarkIngestionGrpcServer
```

Or combine them on a single line to avoid polluting the shell environment:

```bash
DP_MONGO_DB_URI="mongodb://benchuser:benchpass@cluster-host:27017/?authSource=admin" \
DP_MONGO_BENCHMARK_DB_NAME="dp-benchmark-functional" \
java -Ddp.config=~/data-platform/config/dp.yml \
  -Dlog4j.configurationFile=~/data-platform/config/log4j2.xml \
  -cp ~/data-platform/lib/dp-service.jar \
  com.ospreydcs.dp.service.ingest.benchmark.BenchmarkIngestionGrpcServer
```

### Overriding via VM options

Pass the overrides as `-D` VM options **before** the `-cp` flag and class name.  Note that these must be JVM VM options, not program arguments — they must appear before the class name on the command line.

```bash
java -Ddp.config=~/data-platform/config/dp.yml \
  -Dlog4j.configurationFile=~/data-platform/config/log4j2.xml \
  -Ddp.MongoClient.uri="mongodb://benchuser:benchpass@cluster-host:27017/?authSource=admin" \
  -Ddp.MongoClient.benchmarkDatabaseName="dp-benchmark-functional" \
  -cp ~/data-platform/lib/dp-service.jar \
  com.ospreydcs.dp.service.ingest.benchmark.BenchmarkIngestionGrpcServer
```

The `dp.` prefix is stripped by the configuration framework before matching the key, so `-Ddp.MongoClient.benchmarkDatabaseName` overrides the `MongoClient.benchmarkDatabaseName` config property.

When using an IntelliJ run configuration, enter the `-Ddp.` options in the **VM options** field, not the **Program arguments** field.



## 2. Running the Ingestion Benchmark Server

The ingestion benchmark server is a special version of the Ingestion Service that listens on port 60051 (instead of the standard 50051) and uses the benchmark database.  It must be started before running either benchmark client application.

At startup the server drops and recreates the benchmark database, then starts accepting gRPC connections.  Look for the following line in the logs to confirm it is ready:

```
Server started, listening on 60051
```

### Command line

```bash
java -Ddp.config=~/data-platform/config/dp.yml \
  -Dlog4j.configurationFile=~/data-platform/config/log4j2.xml \
  -cp ~/data-platform/lib/dp-service.jar \
  com.ospreydcs.dp.service.ingest.benchmark.BenchmarkIngestionGrpcServer
```

To override the database settings, add the environment variables or VM options as shown in section 1.

The data-platform repo includes bash scripts for starting the benchmark server — see the [data-platform benchmark scripts](https://github.com/osprey-dcs/data-platform/blob/main/README-dp-support.md#data-platform-performance-benchmarks).



## 3. Running the BenchmarkIngestDataStream Client

`BenchmarkIngestDataStream` measures unidirectional streaming ingestion performance.  It connects to the benchmark server via gRPC, sends a stream of ingestion requests using multiple concurrent threads, and prints a performance summary on completion.

The client does not connect to MongoDB directly — only the server needs the database override settings from section 1.

If the benchmark server is not running on `localhost:60051`, override the connect string via the `DP_INGESTION_BENCHMARK_GRPC_CONNECT_STRING` environment variable or the `-Ddp.IngestionBenchmark.grpcConnectString` VM option.

### Column data type options

The client accepts an optional argument selecting the column data type used for ingestion:

| Argument | Description |
|---|---|
| (none) | `DoubleColumn` structure (default) |
| `--double-column` | `DoubleColumn` structure (explicit) |
| `--serialized-column` | `SerializedDataColumn` structure |

### Command lines

Default (DoubleColumn):
```bash
java -Ddp.config=~/data-platform/config/dp.yml \
  -Dlog4j.configurationFile=~/data-platform/config/log4j2.xml \
  -cp ~/data-platform/lib/dp-service.jar \
  com.ospreydcs.dp.service.ingest.benchmark.BenchmarkIngestDataStream
```

With a non-default server address:
```bash
DP_INGESTION_BENCHMARK_GRPC_CONNECT_STRING="cluster-host:60051" \
java -Ddp.config=~/data-platform/config/dp.yml \
  -Dlog4j.configurationFile=~/data-platform/config/log4j2.xml \
  -cp ~/data-platform/lib/dp-service.jar \
  com.ospreydcs.dp.service.ingest.benchmark.BenchmarkIngestDataStream
```

### Expected output

On completion the client prints a performance summary to the console, for example:

```
======================================
Streaming Ingestion Experiment Results
======================================
numThreads: 5 numStreams: 50 writeRate: 12,345.67 values/sec
max write rate: 12345.67
min write rate: 12345.67
```



## 4. Running the BenchmarkIngestDataBidiStream Client

`BenchmarkIngestDataBidiStream` measures bidirectional streaming ingestion performance.  It is used and configured identically to `BenchmarkIngestDataStream` — the only difference is the gRPC streaming mode.  Run it instead of or in addition to `BenchmarkIngestDataStream` to compare unidirectional vs. bidirectional streaming performance.

### Command lines

Default (DoubleColumn):
```bash
java -Ddp.config=~/data-platform/config/dp.yml \
  -Dlog4j.configurationFile=~/data-platform/config/log4j2.xml \
  -cp ~/data-platform/lib/dp-service.jar \
  com.ospreydcs.dp.service.ingest.benchmark.BenchmarkIngestDataBidiStream
```

Note that each run of a client application reinitializes the benchmark database via the server (the client triggers a new `prepareBenchmarkDatabase()` call for each scenario within the experiment).  If you want to run both clients back-to-back for comparison, simply start one after the other — there is no need to restart the server between runs.



## 5. Running the Query Benchmark Server

The query benchmark server is a special version of the Query Service that listens on port 60052 (instead of the standard 50052) and uses the benchmark database.  It must be started before running any query benchmark client application.

At startup the server drops and recreates the benchmark database, then starts accepting gRPC connections.  Look for the following line in the logs to confirm it is ready:

```
Server started, listening on 60052
```

### Command line

```bash
java -Ddp.config=~/data-platform/config/dp.yml \
  -Dlog4j.configurationFile=~/data-platform/config/log4j2.xml \
  -cp ~/data-platform/lib/dp-service.jar \
  com.ospreydcs.dp.service.query.benchmark.BenchmarkQueryGrpcServer
```

To override the database settings, add the environment variables or VM options as shown in section 1.

The data-platform repo includes bash scripts for starting the benchmark server — see the [data-platform benchmark scripts](https://github.com/osprey-dcs/data-platform/blob/main/README-dp-support.md#data-platform-performance-benchmarks).



## 6. Running the Query Benchmark Client Applications

Unlike the ingestion benchmark clients, the query benchmark clients connect to MongoDB directly to load test data before querying.  This means the database override settings from section 1 must be applied when running the query client applications as well as the server.

If the benchmark server is not running on `localhost:60052`, override the connect string via the `DP_QUERY_BENCHMARK_GRPC_CONNECT_STRING` environment variable or the `-Ddp.QueryBenchmark.grpcConnectString` VM option.

There are three query benchmark client applications, each exercising a different query API:

| Class | API |
|---|---|
| `BenchmarkQueryDataStream` | `queryDataStream` — server-streaming response |
| `BenchmarkQueryDataBidiStream` | `queryDataBidiStream` — bidirectional cursor-based streaming |
| `BenchmarkQueryDataUnary` | `queryData` — unary response (use small PV counts to avoid message size limits) |

None of the query benchmark clients accept column type arguments.

### Command lines

`BenchmarkQueryDataStream`:
```bash
DP_MONGO_DB_URI="mongodb://benchuser:benchpass@cluster-host:27017/?authSource=admin" \
DP_MONGO_BENCHMARK_DB_NAME="dp-benchmark-functional" \
java -Ddp.config=~/data-platform/config/dp.yml \
  -Dlog4j.configurationFile=~/data-platform/config/log4j2.xml \
  -cp ~/data-platform/lib/dp-service.jar \
  com.ospreydcs.dp.service.query.benchmark.BenchmarkQueryDataStream
```

`BenchmarkQueryDataBidiStream`:
```bash
DP_MONGO_DB_URI="mongodb://benchuser:benchpass@cluster-host:27017/?authSource=admin" \
DP_MONGO_BENCHMARK_DB_NAME="dp-benchmark-functional" \
java -Ddp.config=~/data-platform/config/dp.yml \
  -Dlog4j.configurationFile=~/data-platform/config/log4j2.xml \
  -cp ~/data-platform/lib/dp-service.jar \
  com.ospreydcs.dp.service.query.benchmark.BenchmarkQueryDataBidiStream
```

`BenchmarkQueryDataUnary`:
```bash
DP_MONGO_DB_URI="mongodb://benchuser:benchpass@cluster-host:27017/?authSource=admin" \
DP_MONGO_BENCHMARK_DB_NAME="dp-benchmark-functional" \
java -Ddp.config=~/data-platform/config/dp.yml \
  -Dlog4j.configurationFile=~/data-platform/config/log4j2.xml \
  -cp ~/data-platform/lib/dp-service.jar \
  com.ospreydcs.dp.service.query.benchmark.BenchmarkQueryDataUnary
```

### Expected output

On completion each client prints a performance summary to the console, for example:

```
======================================
queryExperiment results
======================================
numPvs: 1000 pvsPerRequest: 10 numThreads: 5 rate: 45,678.90 values/sec
max rate: 45678.90
min rate: 45678.90
```
