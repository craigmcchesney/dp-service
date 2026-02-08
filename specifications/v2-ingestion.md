## Overview

The MLDP data ingestion gRPC API is used by clients for uploading particle accelerator instrument data to a data archive, for later in use machine learning and data-driven applications.  The data is organized by "process variable" (PV), corresponding to a readout from some instrument that we want to save in the archive.  The main use case for the ingestion API is to send PV time-series data structured in batches or "buckets".

Each ingestion request primarily includes an "IngestionDataFrame" message that includes 1) a DataTimestamps message specifying either a time range or explicit list of timestamps, and 2) PV time-series data column vectors containing a sample value for each timestamp.

The original ingestion API definition is sample-oriented.  The main data structures are the DataColumn, which is a PV data vector represented as a list of DataValue messages.  The motivation for using the DataValue message is to allow the API to handle a wide range of heterogeneous data types including simple scalar values as well as more complex data like arrays, structures, and images.  Below is the basic proto definitions for each:

```
message DataColumn {
  string name = 1; // Name of PV.
  repeated DataValue dataValues = 2; // List of heterogeneous column data values.
}
```

```
message DataValue {
  oneof value {
    string		stringValue = 1;		// character string
    bool		booleanValue = 2;		// logical Boolean
    uint32		uintValue = 3;			// unsigned integer value
    uint64		ulongValue = 4;			// unsigned long integer
    sint32		intValue = 5;			// signed integer value
    sint64		longValue = 6;			// signed long integer
    float		floatValue = 7;			// 32 byte float value
    double		doubleValue = 8;		// 64 byte float value (double)
    bytes		byteArrayValue = 9;		// raw data as byte string
    Array		arrayValue = 10;			// heterogeneous array (no dimensional restrictions as of yet)
    Structure	structureValue = 11;		// general data structure (no width or depth restrictions yet)
    Image		imageValue = 12;		// general image value
    Timestamp timestampValue = 13; // timestamp data value
  }
}
```

We are worried about the JVM memory allocation behavior for the backend service that handles the ingestion requests.  For example, if samples are batched into buckets containing 1000 values, the JVM will allocate memory for each sample value contained in that request.  Since our baseline case is to handle 4000 PVs each at 1 KHz (1000 samples per second), we are worried about memory allocation and garbage collection being a long term issue for the service.

To that end, we are changing the API data structures to be more column-oriented instead of sample-oriented.  For example, the new "DoubleColumn" message would be used to ingest a vector of double column values.  When a request arrives in the service, the values for the DoubleColumn will be read as a Java primitive array of doubles, with no per-sample memory allocation.  The proto definition for DoubleColumn is shown below:

```
message DoubleColumn {
  string name = 1; // PV name
  repeated double values = 2 [packed = true];
}
```

There are other similar new data structures for scalar sample values including:

* FloatColumn
* Int64Column
* Int32Column
* BoolColumn
* StingColumn
* EnumColumn

Some PV sample values use more complex data types like arrays, images, and structures.  These are handled by the following new data structures:

* DoubleArrayColumn
* FloatArrayColumn
* Int32ArrayColumn
* Int64ArrayColumn
* BoolArrayColumn
* ImageColumn
* StructColumn
* SerializedDataColumn

The proto definition for DoubleArrayDataColumn is shown below:

```
message DoubleArrayColumn {
  string name = 1; // PV name
  ArrayDimensions dimensions = 2;
  // Flattened: sample_count × product(dims)
  repeated double values = 3 [packed = true];
}
```

So the bottom line is that we are accomplishing the heterogeneity at the column level instead of the sample value.  This makes logical sense because we don't expect the data type for a given PV to change from sample to sample.  Coupled with the improvement in memory efficiency for both ingestion clients and the backend service handling, this is the motivation for the significant change to the ingestion API data structures.

To incorporate the new data structures in a backward compatible way, we modified the payload of the "IngestionDataFrame" message.  Previously the IngestionDataFrame could contain a list of DataColumn messages.  Now it can also include lists of each of the other column message data structures.  We are deprecating support for ingestion of DataColumn / DataValue in the current 1.13 release, and will be removing that support in the 2.0 release.

The full MLDP API definition is contained in the dp-grpc repository, which is cloned on this development machine to the following directory: ~/dp.fork/dp-java/dp-grpc/src/main/proto.

## Tasks

### 1.0 Ingestion Validation

The first task in implementing the ingestion API changes is to add validation for incoming requests.  Incoming requests are handled by the IngestionServiceImpl via one of the three service API methods: ingestData(), ingestDataStream(), and ingestDataBidiStream().  For validation, there is a common static method IngestionValidationUtility.validateIngestionRequest().  

The existing validation is fairly lightweight.  There are checks that providerId and requestId are provided, and that the request contains data to ingest (e.g., that numRequestRows and numRequestColumns are non-zero).

We want to add full validation for the new ingestion API data structures and the corresponding design assumptions.  The approach should be layered, and we want to fail early, cheaply, and deterministically.  Here are the validation rules that we want to enforce in IngestionValidationUtility.validateIngestionRequest().  We can break into sub-methods if that is useful.

### 1.0.1 Request / Frame Level Validation

Here are the request level checks that we want to perform

* providerid is specified and valid
* clientRequestId is specified
* IngestionDataFrame.dataTimestamps is provided
* DataTimestamps validation
  * DataTimestamps is exactly one of SamplingClock or explicit timestamps list
  * if sampling clock
    * sampleCount > 0
    * samplePerionNs > 0
    * start time seconds and nanos are valid
  * if explicit timestamps list
    * list length > 0
    * all timestamps valid
    * timestamps are strictly increasing or at least non-decreasing
    * sampleCount = timestamps.length

### 1.0.2 Column Level Validation

Validation for every column regardless of type:

* name is non-empty
* one column per PV name per IngestionDataFrame
* column type is valid
* column values.length matches sampleCount from DataTimestamps

### 1.0.3 Scalar Column Validation

For the scalar column data types (Applies to: DoubleColumn, FloatColumn, Int32Column, Int64Column, BoolColumn, EnumColumn, StringColumn payloads):

* values.length == sampleCount

For StringColumn only:

* max string length is 256 characters

### 1.0.4 Array Column Validation

For the array data types (DoubleArrayColumn, FloatArrayColumn, Int32ArrayColumn, Int64ArrayColumn, BoolArrayColumn):

* dimensions.dims.size() is in {1, 2, 3}
* all dimension values > 0
* where element_count = product(dimensions):
  * element count < configured maximum (let's use a constant of 10 million, and make this configurable by column type later)
  * values.length == sampleCount * element_count

### 1.0.5 ImageColumn Validation

* descriptor is specified
* width > 0 ; height > 0 ; channels > 0
* for each image payload
  * size < max configured per-image size (let's use a constant for now and make configurable later)
  * encoding is valid

### 1.0.6 StructColumn Validation

* schemaId is specified
* values.length == sampleCount
* struct payload size is less than configured max per-struct (let's use a constant for now and make configurable later)

### 1.0.7 SerializedDataColumn Validation

* encoding is specified

### clarifications

1. Ignore the GridFS part, I meant to delete it.
2. For now, just ensure the enumId is non-empty.  We don't want to place unneccesary constraints on the domain of values because that is external to the API and varies by client.  We might later add a per-pv configuration
   / registration that specifies thanks like valid enumIds.
3. Image encoding is defined external to the API because it varies by facility.  It is a contract between data producer and data consumer.  Just check that it is non-empty.
4. It's not a bad idea to maintain separate validation paths for legacy DataColumn vs. the new types.  How do you propose to handle that?
5. Yes please include field paths and values in error messages.

### 1.1 Ingestion Validation Test Coverage

Please add comprehensive test coverage for all failure scenarios and new proto column message data structures.

## 2.0 Ingestion Performance Benchmark

I've added a framework to the Ingestion Service handler that reflects the new column-oriented data structures in the gRPC ingestion API in writing BucketDocuments to MongoDB for each column contained in an IngestDataRequest's IngestionDataFrame.  The BucketDocuments now include an embedded document with the sample data values for the corresponding request column.  There is a Java POJO class hierarchy for the embedded column documents, ColumnDocumentBase, with a derived class DoubleColumnDocument to serve as the embedded column document when creating a BucketDocument for a protobuf DoubleColumn message.  I also refactored the existing code to make the previously existing DataColumnDocument extend ColumnDocumentBase.  A BucketDocument containing a DataColumnDocument is created when the ingestion request data frame includes a DataColumn protobuf message.  I will add classes to the ColumnDocumentBase hierarchy to support the other protobuf column messages as a follow on task.

Before I move forward with handling the other protobuf column messages, I want to make a benchmark for comparing the performance of ingestion using the DataColumn / DataValue protobuf messages (with double sample data values) from the original implementation with ingestion using the new DoubleColumn protobuf message.

There is already an ingestion performance benchmark framework for measuring the performance of ingestion using DataColumn / DataValue protobuf messages.  There are 3 variant ingestion benchmark applications that extend IngestionBenchmarkBase - BenchmarkIngestDataStream, BenchmarkIngestDataBidiStream, and BenchmarkIngestDataStreamBytes that cover the ingestDataStream(), ingestDataBidiStream(), and ingestDataStream() with SerializedDataColumns, respectively.

Please investigate and give me an overview of what would be required to refactor the framework so that we can pass a flag from BenchmarkIngestDataBidiStream.main() and BenchmarkIngestDataStream.main() that tells the framework to build IngestDataRequests whose IngestionDataFrame uses DoubleColumn protobuf objects instead of DataColumn / DataValue objects.  We don't want to change any code yet, just give me an approach.

It looks like one idea might be to refactor BenchmarkIngestDataBidiStream and BenchmarkIngestDataStream to be intermediate base classes, each with 2 derived classes that define a main method that uses either DataColumn ingestion or DoubleColumn ingestion.  But if you have better ideas let me know.

### 2.1 Benchmark Refactoring Using Strategy Pattern and Factory Method

I agree with your recommendation in option 1 to use the strategy pattern and factory method.  Please proceed with the suggested implementation steps:

1. Create ColumnDataType enum and ColumnBuilder interface
2. Implement DataColumnBuilder (extract existing logic)
3. Implement DoubleColumnBuilder (new logic for DoubleColumn)
4. Update IngestionTaskParams to include ColumnDataType
5. Refactor buildDataTableTemplate() to use strategy pattern
6. Update benchmark main() methods to parse column type from args
7. Add new DoubleColumnBuilder implementation:

## 3.0 Parameterized Intermediate Base Class for Scalar Columns

Please design a generic intermediate base class extending ColumnDocumentBase, "ScalarColumnDocumentBase", with a parameter for the scalar object type contained by the column.  For example, we will change the new DoubleColumnDocument to extend ScalarColumnBase and the type parameter will be "Double".  Move the "values" instance variable up from DoubleColumnDocument to the new base class and make the List of values use the parameter type as the List type.  Consider moving other methods up to the base class if their implementation is generic, or could be generic.  Don't change any code yet, just propose a design and please feel free to make suggestions for improvement to the approach.

The relevant proto column data structure messages are DoubleColumn, FloatColumn, Int64Column, Int32Column, BoolColumn, StringColumn, and EnumColumn.