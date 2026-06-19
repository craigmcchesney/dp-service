# Running MLDP Services and Applications

## core services

Below are java command lines for running the Data Platform Core Services, using the standard "dp" MongoDB database.  Use these commands for production and test systems.

The data-platform repo includes [[bash scripts](https://github.com/osprey-dcs/data-platform/blob/main/README-dp-support.md#data-platform-scripts)](https://github.com/osprey-dcs/data-platform/blob/main/README-dp-support.md#data-platform-scripts) for starting the services.

#### Ingestion Service
```
java -Ddp.config=~/data-platform/config/dp.yml -Dlog4j.configurationFile=~/data-platform/config/log4j2.xml -cp ~/data-platform/lib/dp-service.jar com.ospreydcs.dp.service.ingest.server.IngestionGrpcServer"
```

#### Query Service

```
java -Ddp.config=~/data-platform/config/dp.yml -Dlog4j.configurationFile=~/data-platform/config/log4j2.xml -cp ~/data-platform/lib/dp-service.jar com.ospreydcs.dp.service.query.server.QueryGrpcServer"
```

#### Annotation Service

```
java -Ddp.config=~/data-platform/config/dp.yml -Dlog4j.configurationFile=~/data-platform/config/log4j2.xml -cp ~/data-platform/lib/dp-service.jar com.ospreydcs.dp.service.annotation.server.AnnotationGrpcServer"
```

#### Ingestion Stream Service

```
java -Ddp.config=~/data-platform/config/dp.yml -Dlog4j.configurationFile=~/data-platform/config/log4j2.xml -cp ~/data-platform/lib/dp-service.jar com.ospreydcs.dp.service.ingestionstream.server.IngestionStreamGrpcServer"
```

## performance benchmark applications

The Data Platform includes tools for measuring performance of the Ingestion and Query Services.  There are special versions of the server applications that override the standard network port number and database, using the MongoDB database "dp-benchmark".  To run a benchmark for either service you must run both the server and client application.  Java command lines are given below.

The data-platform repo includes [bash scripts](https://github.com/osprey-dcs/data-platform/blob/main/README-dp-support.md#data-platform-performance-benchmarks) for starting the benchmark servers and running the client applications.

### ingestion performance benchmark

#### ingestion benchmark server

```
java -Ddp.config=~/data-platform/config/dp.yml -Dlog4j.configurationFile=~/data-platform/config/log4j2.xml -cp ~/data-platform/lib/dp-service.jar com.ospreydcs.dp.service.ingest.benchmark.BenchmarkIngestionGrpcServer"
```

#### ingestion benchmark client

```
java -Ddp.config=~/data-platform/config/dp.yml -Dlog4j.configurationFile=~/data-platform/config/log4j2.xml -cp ~/data-platform/lib/dp-service.jar com.ospreydcs.dp.service.ingest.benchmark.BenchmarkBidiStreamingIngestion
```

### query performance benchmark

#### query benchmark server

```
java -Ddp.config=~/data-platform/config/dp.yml -Dlog4j.configurationFile=~/data-platform/config/log4j2.xml -cp ~/data-platform/lib/dp-service.jar com.ospreydcs.dp.service.query.benchmark.BenchmarkQueryGrpcServer"
```

#### query benchmark client

```
java -Ddp.config=~/data-platform/config/dp.yml -Dlog4j.configurationFile=~/data-platform/config/log4j2.xml -cp ~/data-platform/lib/dp-service.jar com.ospreydcs.dp.service.query.benchmark.BenchmarkQueryDataStream
```

## sample data generator

Recognizing the previous use of the Ingestion Service performance benchmark to generate sample data for web development and demo purposes, a sample data generator application was created to generate sample data for one minute and 4,000 signals each sampled at 1 KHz.  The data generator uses the standard "dp" database and data is created with a fixed date/time starting at "2023-10-31T15:51:00.000+00:00".  Happy Halloween...

To use the sample data generator, run the standard Ingestion Service as shown above, and use the Java command line below to run the client application.

The data-platform repo includes [bash scripts](https://github.com/osprey-dcs/data-platform/blob/main/README-dp-support.md#sample-data-generator-scripts) for starting the standard Ingestion Service and running the sample data generator application.

```
java -Ddp.config=~/data-platform/config/dp.yml -Dlog4j.configurationFile=~/data-platform/config/log4j2.xml -cp ~/data-platform/lib/dp-service.jar com.ospreydcs.dp.service.ingest.utility.TestDataGenerator
```

# Service Configuration


## Default config file

The default configuration file for the Data Platform services is in [application.yml](https://github.com/osprey-dcs/dp-service/blob/main/src/main/resources/application.yml). The config file contains in-line comments for each section and individual configuration options.

The default settings are probably reasonable for most development systems, though you'll want to override the MongoDB URI resource to match your configuration. 

Options for overriding the config file and individual resources are discussed below.


## Overriding config file

The default configuration file can be overridden in two different ways, by specifying an alternative file on either the command line used to start the application, or as an environment variable.

To specify an alternative on the command line, add a VM option (e.g., on the command line BEFORE the class name) like the following: "-Ddp.config=/path/to/config/override.yml".

To specify an alternative via an envoronment variable, define a variable in the environment "DP.CONFIG=/path/to/config/override.yml" before running the Ingestion Service application.  For example:
```
java -Ddp.config=$DP_HOME/config/dp.yml -Dlog4j.configurationFile=$DP_HOME/config/log4j2.xml -cp $DP_HOME/lib/dp-service/dp-service.jar com.ospreydcs.dp.service.ingest.server.IngestionGrpcServer
```


## Overriding individual configuration properties on the command line

In addition to overriding the default config file, individual configuration settings can be overridden on the command line.  To do so, use a VM option (in the java command line BEFORE the class name), prefixing the configuration setting name with "dp.".  For example, to override the gRPC port for the Ingestion Service, use "-Ddp.IngestionServer.port=50052".## Custom Configuration via Environment Variables

## Overriding individual configuration properties via environment variables

- **What:** The service reads configuration values from `application.yml` using the ${ENV_VAR:default} substitution syntax. If an environment variable is set, it overrides the default value declared in the YAML. If it is not set, the YAML default is used.
- **How:** Set environment variables before starting the JVM or provide them in your container/compose environment. Example (shell):

```bash
export DP_MONGO_DB_URI=mongodb://admin:admin@localhost:27017/
java <other options> com.ospreydcs.dp.service.ingest.server.IngestionGrpcServer
```

- **Docker / docker-compose:** Put the same variables in the `environment:` block for the container. Example (docker-compose.yml):

```yaml
services:
	dp-service:
		image: your-image
		environment:
			- DP_MONGO_DB_URI=mongodb://admin:admin@localhost:27017/
```

- **Notes:**
	- Boolean values should be set as the strings `true` or `false` (unquoted or quoted are both accepted by the shell/container). Numeric values should be plain numbers. Strings containing colons or special characters should be quoted.

**Accepted environment variables**

Below is the list of environment variables referenced in the project's `application.yml`. Each variable maps to a configuration property; if not set, the YAML default applies.

- `DP_MONGO_DB_URI` : Full MongoDB connection string. Example: `mongodb://user:pass@host:27017/?authSource=admin`
- `DP_MONGO_BENCHMARK_DB_NAME` : Name of the MongoDB database used by the benchmark (default: `dp-benchmark`). **WARNING: this database is dropped and recreated at the start of every benchmark run — never point it at a database containing data you want to keep.**
- `DP_MONGO_TEST_DB_NAME` : Name of the MongoDB database used by integration tests (default: `dp-test`). **WARNING: this database is dropped and recreated at the start of every test run — never point it at a database containing data you want to keep.**
- `DP_GRPC_CLIENT_HOSTNAME` : gRPC client hostname (default: `localhost`)
- `DP_GRPC_CLIENT_KEEP_ALIVE_TIME_SECONDS` : gRPC client keepalive time in seconds (default: `45`)
- `DP_GRPC_CLIENT_KEEP_ALIVE_TIMEOUT_SECONDS` : gRPC client keepalive timeout in seconds (default: `20`)
- `DP_GRPC_CLIENT_KEEP_ALIVE_WITHOUT_CALLS` : gRPC client keepalive without calls (default: `true`)
- `DP_GRPC_SERVER_INCOMING_MESSAGE_SIZE_LIMIT_BYTES` : gRPC incoming message size limit in bytes (default: `4096000`)
- `DP_GRPC_SERVER_KEEP_ALIVE_TIME_SECONDS` : gRPC server keepalive time in seconds (default: `60`)
- `DP_GRPC_SERVER_KEEP_ALIVE_TIMEOUT_SECONDS` : gRPC server keepalive timeout in seconds (default: `20`)
- `DP_GRPC_SERVER_PERMIT_KEEP_ALIVE_TIME_SECONDS` : gRPC server permit-keepalive time in seconds (default: `30`)
- `DP_GRPC_SERVER_PERMIT_KEEP_ALIVE_WITHOUT_CALLS` : gRPC server permit keepalive without calls (default: `true`)
- `DP_INGESTION_SERVER_PORT` : Ingestion Service gRPC port (default: `50051`)
- `DP_INGESTION_HANDLER_NUM_WORKERS` : Number of ingestion handler worker threads (default: `7`)
- `DP_INGESTION_HANDLER_SOURCEMONITOR_VALIDATE_PVS` : Ingestion SourceMonitor validate PVs (default: `true`)
- `DP_INGESTION_BENCHMARK_GRPC_CONNECT_STRING` : Ingestion benchmark gRPC `host:port` connect string (default: `localhost:60051`)
- `DP_INGESTION_BENCHMARK_START_SECONDS` : Ingestion benchmark fixed start time (epoch seconds)
- `DP_QUERY_SERVER_PORT` : Query Service gRPC port (default: `50052`)
- `DP_QUERY_HANDLER_NUM_WORKERS` : Number of query handler worker threads (default: `7`)
- `DP_QUERY_HANDLER_OUTGOING_MESSAGE_SIZE_LIMIT_BYTES` : Query handler outgoing message size limit (default: `4096000`)
- `DP_QUERY_BENCHMARK_GRPC_CONNECT_STRING` : Query benchmark gRPC `host:port` connect string (default: `localhost:60052`)
- `DP_ANNOTATION_SERVER_PORT` : Annotation Service gRPC port (default: `50053`)
- `DP_ANNOTATION_HANDLER_NUM_WORKERS` : Number of annotation handler worker threads (default: `7`)
- `DP_EXPORT_SERVER_MOUNT_POINT` : Export server mount point (default: `/tmp`)
- `DP_EXPORT_SHARE_MOUNT_POINT` : Export share mount point (defaults to server mount point if unset)
- `DP_EXPORT_URL_BASE` : Base URL for accessing export files (default: `http://localhost:8081`)
- `DP_EXPORT_TABULAR_EXPORT_FILE_SIZE_LIMIT_BYTES` : Tabular export file size limit (default: `4096000`)
- `DP_INGESTION_STREAM_SERVER_PORT` : Ingestion Stream Service gRPC port (default: `50054`)
- `DP_INGESTION_STREAM_HANDLER_NUM_WORKERS` : Number of ingestion stream handler worker threads (default: `7`)
- `DP_INGESTION_STREAM_EVENTMONITOR_MAX_MESSAGE_SIZE_BYTES` : EventMonitor max message size (default: `4096000`)
- `DP_INGESTION_STREAM_DATA_BUFFER_FLUSH_INTERVAL_MILLIS` : DataBuffer flush interval in ms (default: `500`)
- `DP_INGESTION_STREAM_DATA_BUFFER_MAX_BUFFER_BYTES` : DataBuffer max bytes (default: `524288`)
- `DP_INGESTION_STREAM_DATA_BUFFER_MAX_BUFFER_ITEMS` : DataBuffer max items (default: `50`)
- `DP_INGESTION_STREAM_DATA_BUFFER_AGE_LIMIT_NANOS` : DataBuffer age limit in nanoseconds (default: `2000000000`)
- `DP_INGESTION_STREAM_DATA_BUFFER_AGE_CUSHION_NANOS` : DataBuffer age cushion in nanoseconds (default: `1000000000`)
- `DP_INGESTION_STREAM_TRIGGERED_EVENT_MANAGER_EVENT_EXPIRATION_NANOS` : TriggeredEventManager expiration in nanoseconds (default: `5000000000`)
- `DP_INGESTION_STREAM_TRIGGERED_EVENT_MANAGER_EVENT_CLEANUP_INTERVAL_MILLIS` : TriggeredEventManager cleanup interval in ms (default: `5000`)

TODO: add a short example `docker-compose.yml` snippet that demonstrates mounting configuration files while still using environment overrides, I can add that as a follow-up.

## Running integration tests against a non-local MongoDB cluster

The integration test framework is designed for development use: it drops and recreates the test database before each test class runs, starting from a completely clean state. This is intentional — the tests are not designed to tolerate pre-existing data.

### Prerequisites

- The MongoDB user in the connection URI must have **`dbOwner`** (or equivalent `dbAdmin` + `readWrite`) privileges on the test database. The framework drops the entire database on each run; a `readWrite`-only user is insufficient.
- The test database name must be a **dedicated throwaway name** that does not conflict with any production or shared database on the cluster.

### Configuration

Set two environment variables before running the tests:

```bash
# MongoDB connection URI for the target cluster
export DP_MONGO_DB_URI="mongodb://testuser:testpass@cluster-host:27017/?authSource=admin"

# Name of the dedicated test database (will be DROPPED before each test class)
export DP_MONGO_TEST_DB_NAME="dp-test-functional"

mvn test -Dtest=PvMetadataIT
```

Or to run all integration tests:

```bash
export DP_MONGO_DB_URI="mongodb://testuser:testpass@cluster-host:27017/?authSource=admin"
export DP_MONGO_TEST_DB_NAME="dp-test-functional"

mvn test
```

> **WARNING:** The database named by `DP_MONGO_TEST_DB_NAME` is **dropped without confirmation** at the start of every test run. Use a name that is reserved exclusively for this purpose.

## Running benchmarks against a non-local MongoDB cluster

The benchmark server drops and recreates the benchmark database at the start of every run, starting from a completely clean state. This is intentional — benchmarks require an empty database for accurate measurements.

### Prerequisites

- The MongoDB user in the connection URI must have **`dbOwner`** (or equivalent `dbAdmin` + `readWrite`) privileges on the benchmark database. The server drops the entire database on each run; a `readWrite`-only user is insufficient.
- The benchmark database name must be a **dedicated throwaway name** that does not conflict with any production or shared database on the cluster.

### Configuration

Set two environment variables before starting the benchmark server:

```bash
# MongoDB connection URI for the target cluster
export DP_MONGO_DB_URI="mongodb://benchuser:benchpass@cluster-host:27017/?authSource=admin"

# Name of the dedicated benchmark database (will be DROPPED at the start of every run)
export DP_MONGO_BENCHMARK_DB_NAME="dp-benchmark-functional"

java -Ddp.config=~/data-platform/config/dp.yml -Dlog4j.configurationFile=~/data-platform/config/log4j2.xml \
  -cp ~/data-platform/lib/dp-service.jar com.ospreydcs.dp.service.ingest.benchmark.BenchmarkIngestionGrpcServer
```

Then run the benchmark client (no MongoDB connection required — connects to the server via gRPC only):

```bash
java -Ddp.config=~/data-platform/config/dp.yml -Dlog4j.configurationFile=~/data-platform/config/log4j2.xml \
  -cp ~/data-platform/lib/dp-service.jar com.ospreydcs.dp.service.ingest.benchmark.BenchmarkIngestDataStream
```

> **WARNING:** The database named by `DP_MONGO_BENCHMARK_DB_NAME` is **dropped without confirmation** at the start of every benchmark server run. Use a name that is reserved exclusively for this purpose.
