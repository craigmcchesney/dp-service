package com.ospreydcs.dp.service.query.handler.mongo.dispatch;

import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.common.*;
import com.ospreydcs.dp.grpc.v1.query.QueryTableRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryTableResponse;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.grpc.GrpcUtility;
import com.ospreydcs.dp.service.common.handler.Dispatcher;
import com.ospreydcs.dp.service.common.handler.QueueHandlerBase;
import com.ospreydcs.dp.service.common.model.TimestampMap;
import com.ospreydcs.dp.service.query.service.QueryServiceImpl;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class TableResponseDispatcher extends Dispatcher {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private final StreamObserver<QueryTableResponse> responseObserver;
    private final QueryTableRequest request;

    // constants
    private final String TABLE_RESULT_TIMESTAMP_COLUMN_NAME = "timestamp";

    public TableResponseDispatcher(
            StreamObserver<QueryTableResponse> responseObserver,
            QueryTableRequest request
    ) {
        this.responseObserver = responseObserver;
        this.request = request;
    }

    private static <T> int addBucketToTable(
            int columnIndex, BucketDocument<T> bucket, TimestampMap<Map<Integer, DataValue>> tableValueMap
    ) {
        int dataValueSize = 0;
        long second = bucket.getFirstSeconds();
        long nano = bucket.getFirstNanos();
        final long delta = bucket.getSampleFrequency();
        for (T value : bucket.getColumnDataList()) {

            // generate DataValue object from column data value
            final DataValue.Builder valueBuilder = DataValue.newBuilder();
            bucket.addColumnDataValue(value, valueBuilder);
            final DataValue dataValue = valueBuilder.build();
            dataValueSize = dataValueSize + dataValue.getSerializedSize();

            // add to table data structure
            Map<Integer, DataValue> nanoValueMap = tableValueMap.get(second, nano);
            if (nanoValueMap == null) {
                nanoValueMap = new TreeMap<>();
                tableValueMap.put(second, nano, nanoValueMap);
            }
            nanoValueMap.put(columnIndex, dataValue);

            // increment nanos, and increment seconds if nanos rolled over one billion
            nano = nano + delta;
            if (nano >= 1_000_000_000) {
                second = second + 1;
                nano = nano - 1_000_000_000;
            }
        }

        return dataValueSize;
    }

    private QueryTableResponse.TableResult columnTableResultFromMap(
            List<String> columnNames, TimestampMap<Map<Integer, DataValue>> tableValueMap) {

        // create builders for table and columns, and list of timestamps
        final QueryTableResponse.TableResult.Builder tableResultBuilder =
                QueryTableResponse.TableResult.newBuilder();

        final Map<Integer, DataColumn.Builder> columnBuilderMap = new TreeMap<>();
        for (int i = 0 ; i < columnNames.size() ; ++i) {
            final DataColumn.Builder dataColumnBuilder = DataColumn.newBuilder();
            dataColumnBuilder.setName(columnNames.get(i));
            columnBuilderMap.put(i, dataColumnBuilder);
        }
        final TimestampList.Builder timestampListBuilder = TimestampList.newBuilder();

        // add data values to column builders, filter by specified time range
        final long beginSeconds = this.request.getBeginTime().getEpochSeconds();
        final long beginNanos = this.request.getBeginTime().getNanoseconds();
        final long endSeconds = this.request.getEndTime().getEpochSeconds();
        final long endNanos = this.request.getEndTime().getNanoseconds();
        for (var secondEntry : tableValueMap.entrySet()) {
            final long second = secondEntry.getKey();
            if (second < beginSeconds || second > endSeconds) {
                // ignore values that are out of query range
                continue;
            }
            final Map<Long, Map<Integer, DataValue>> secondValueMap = secondEntry.getValue();
            for (var nanoEntry : secondValueMap.entrySet()) {
                final long nano = nanoEntry.getKey();
                if ((second == beginSeconds && nano < beginNanos) || (second == endSeconds && nano >= endNanos)) {
                    // ignore values that are out of query range
                    continue;
                }
                final Map<Integer, DataValue> nanoValueMap = nanoEntry.getValue();
                final Timestamp timestamp = Timestamp.newBuilder().setEpochSeconds(second).setNanoseconds(nano).build();
                timestampListBuilder.addTimestamps(timestamp);
                for (var columnBuilderMapEntry : columnBuilderMap.entrySet()) {
                    final int columnIndex = columnBuilderMapEntry.getKey();
                    final DataColumn.Builder dataColumnBuilder = columnBuilderMapEntry.getValue();
                    DataValue columnDataValue = nanoValueMap.get(columnIndex);
                    if (columnDataValue == null) {
                        columnDataValue = DataValue.newBuilder().build();
                    }
                    dataColumnBuilder.addDataValues(columnDataValue);
                }
            }
        }

        // build timestamp list, columns, and table
        timestampListBuilder.build();
        final DataTimestamps.Builder dataTimestampsBuilder = DataTimestamps.newBuilder();
        dataTimestampsBuilder.setTimestampList(timestampListBuilder).build();
        QueryTableResponse.ColumnTable.Builder columnTableBuilder = QueryTableResponse.ColumnTable.newBuilder();
        columnTableBuilder.setDataTimestamps(dataTimestampsBuilder);
        for (DataColumn.Builder dataColumnBuilder : columnBuilderMap.values()) {
            dataColumnBuilder.build();
            columnTableBuilder.addDataColumns(dataColumnBuilder);
        }
        tableResultBuilder.setColumnTable(columnTableBuilder.build());
        return tableResultBuilder.build();
    }

    private QueryTableResponse.TableResult rowMapTableResultFromMap(
            List<String> columnNames, TimestampMap<Map<Integer, DataValue>> tableValueMap) {

        final QueryTableResponse.TableResult.Builder tableResultBuilder = QueryTableResponse.TableResult.newBuilder();
        final QueryTableResponse.RowMapTable.Builder rowMapTableBuilder = QueryTableResponse.RowMapTable.newBuilder();

        final List<String> columnNamesWithTimestamp = new ArrayList<>();
        columnNamesWithTimestamp.add(TABLE_RESULT_TIMESTAMP_COLUMN_NAME);
        columnNamesWithTimestamp.addAll(columnNames);
        rowMapTableBuilder.addAllColumnNames(columnNamesWithTimestamp);

        final long beginSeconds = this.request.getBeginTime().getEpochSeconds();
        final long beginNanos = this.request.getBeginTime().getNanoseconds();
        final long endSeconds = this.request.getEndTime().getEpochSeconds();
        final long endNanos = this.request.getEndTime().getNanoseconds();

        for (var secondEntry : tableValueMap.entrySet()) {

            final long second = secondEntry.getKey();
            if (second < beginSeconds || second > endSeconds) {
                // ignore values that are out of query range
                continue;
            }

            final Map<Long, Map<Integer, DataValue>> secondValueMap = secondEntry.getValue();
            for (var nanoEntry : secondValueMap.entrySet()) {

                final long nano = nanoEntry.getKey();
                if ((second == beginSeconds && nano < beginNanos) || (second == endSeconds && nano >= endNanos)) {
                    // ignore values that are out of query range
                    continue;
                }

                final Map<Integer, DataValue> nanoValueMap = nanoEntry.getValue();

                // create map for row's data, keys are column names, values are column values
                final QueryTableResponse.RowMapTable.DataRow.Builder dataRowBuilder =
                        QueryTableResponse.RowMapTable.DataRow.newBuilder();

                // set value for timestamp column
                final Timestamp timestamp = Timestamp.newBuilder().setEpochSeconds(second).setNanoseconds(nano).build();
                final DataValue timestampDataValue = DataValue.newBuilder()
                        .setTimestampValue(timestamp)
                        .build();
                dataRowBuilder.getColumnValuesMap().put(TABLE_RESULT_TIMESTAMP_COLUMN_NAME, timestampDataValue);

                // add map entry for each data column value
                int columnIndex = 0;
                for (String columnName : columnNames) {
                    DataValue columnDataValue = nanoValueMap.get(columnIndex);
                    if (columnDataValue == null) {
                        columnDataValue = DataValue.newBuilder().build();
                    }
                    dataRowBuilder.getColumnValuesMap().put(columnName, columnDataValue);

                    columnIndex = columnIndex + 1;
                }

                // add row to result
                rowMapTableBuilder.addRows(dataRowBuilder.build());
            }
        }

        tableResultBuilder.setRowMapTable(rowMapTableBuilder.build());
        return tableResultBuilder.build();
    }

    public void handleResult(MongoCursor<BucketDocument> cursor) {

        // send error response and close response stream if cursor is null
        if (cursor == null) {
            final String msg = "executeQuery returned null cursor";
            logger.error(msg);
            QueryServiceImpl.sendQueryTableResponseError(msg, this.responseObserver);
            return;
        }

        // send empty QueryStatus and close response stream if query matched no data
        if (!cursor.hasNext()) {
            logger.trace("processQueryRequest: query matched no data, cursor is empty");
            QueryServiceImpl.sendQueryTableResponseEmpty(this.responseObserver);
            return;
        }

        // create data structure for creating table
        final TimestampMap<Map<Integer, DataValue>> tableValueMap = new TimestampMap<>();

        // data structure for getting column index
        final List<String> columnNameList = new ArrayList<>();

        int responseMessageSize = 0;
        while (cursor.hasNext()) {
            // add buckets to table data structure
            final BucketDocument bucket = cursor.next();
            int columnIndex = columnNameList.indexOf(bucket.getColumnName());
            if (columnIndex == -1) {
                // add column to list and get index
                columnNameList.add(bucket.getColumnName());
                columnIndex = columnNameList.size() - 1;
            }
            int bucketDataSize = addBucketToTable(columnIndex, bucket, tableValueMap);
            responseMessageSize = responseMessageSize + bucketDataSize;
            if (responseMessageSize > GrpcUtility.MAX_GRPC_MESSAGE_SIZE) {
                final String msg = "result exceeds gRPC message size limit";
                logger.error(msg);
                QueryServiceImpl.sendQueryTableResponseError(msg, this.responseObserver);
                return;
            }
        }
        cursor.close();

        // create column or row-oriented table result from map as specified in request
        QueryTableResponse.TableResult tableResult = null;
        switch (request.getFormat()) {

            case TABLE_FORMAT_COLUMN -> {
                tableResult = columnTableResultFromMap(columnNameList, tableValueMap);
            }
            case TABLE_FORMAT_ROW_MAP -> {
                tableResult = rowMapTableResultFromMap(columnNameList, tableValueMap);
            }
            case TABLE_FORMAT_ROW_LIST -> {
                QueryServiceImpl.sendQueryTableResponseError(
                        "TABLE_FORMAT_ROW_LIST not yet supported", this.responseObserver);
                return;
            }
            case UNRECOGNIZED -> {
                QueryServiceImpl.sendQueryTableResponseError(
                        "QueryTableRequest.format must be specified", this.responseObserver);
                return;
            }
        }

        // create and send response, close response stream
        QueryTableResponse response = QueryServiceImpl.queryTableResponse(tableResult);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
