package com.ospreydcs.dp.service.integration.query;

import com.google.protobuf.InvalidProtocolBufferException;
import com.ospreydcs.dp.grpc.v1.common.DataBucket;
import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.query.*;
import com.ospreydcs.dp.grpc.v1.query.ProviderMetadata;
import com.ospreydcs.dp.service.common.model.TimestampMap;
import com.ospreydcs.dp.service.common.mongo.MongoTestClient;
import com.ospreydcs.dp.service.common.protobuf.AttributesUtility;
import com.ospreydcs.dp.service.common.protobuf.DataTimestampsUtility;
import com.ospreydcs.dp.service.integration.ingest.GrpcIntegrationIngestionServiceWrapper;
import com.ospreydcs.dp.service.query.QueryTestBase;
import com.ospreydcs.dp.service.query.handler.interfaces.QueryHandlerInterface;
import com.ospreydcs.dp.service.query.handler.mongo.MongoQueryHandler;
import com.ospreydcs.dp.service.query.handler.mongo.dispatch.QueryTableDispatcher;
import com.ospreydcs.dp.service.query.service.QueryServiceImpl;
import com.ospreydcs.dp.service.integration.GrpcIntegrationServiceWrapperBase;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.mock;

public class GrpcIntegrationQueryServiceWrapper extends GrpcIntegrationServiceWrapperBase<QueryServiceImpl> {

    // static variables
    private static final Logger logger = LogManager.getLogger();
    @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    // instance variables (common ones inherited from base class)

    public Channel getQueryChannel() {
        return this.channel;
    }

    @Override
    protected boolean initService() {
        QueryHandlerInterface queryHandler = MongoQueryHandler.newMongoSyncQueryHandler();
        service = new QueryServiceImpl();
        return service.init(queryHandler);
    }

    @Override
    protected void finiService() {
        service.fini();
    }

    @Override
    protected QueryServiceImpl createServiceMock(QueryServiceImpl service) {
        return mock(QueryServiceImpl.class, delegatesTo(service));
    }

    @Override
    protected GrpcCleanupRule getGrpcCleanupRule() {
        return grpcCleanup;
    }

    @Override
    protected String getServiceName() {
        return "QueryServiceImpl";
    }
    
    protected QueryTableResponse.TableResult sendQueryTable(
            QueryTableRequest request,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final DpQueryServiceGrpc.DpQueryServiceStub asyncStub = DpQueryServiceGrpc.newStub(channel);

        final QueryTestBase.QueryTableResponseObserver responseObserver =
                new QueryTestBase.QueryTableResponseObserver();

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            asyncStub.queryTable(request, responseObserver);
        }).start();

        responseObserver.await();

        if (expectReject) {
            assertTrue(responseObserver.isError());
            assertTrue(responseObserver.getErrorMessage().contains(expectedRejectMessage));
            return null;
        } else {
            final QueryTableResponse response = responseObserver.getQueryResponse();
            assertTrue(response.hasTableResult());
            return response.getTableResult();
        }
    }

    protected QueryTableResponse.TableResult queryTable(
            QueryTestBase.QueryTableRequestParams params,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final QueryTableRequest request = QueryTestBase.buildQueryTableRequest(params);
        return sendQueryTable(request, expectReject, expectedRejectMessage);
    }

    private void verifyQueryTableColumnResult(
            QueryTestBase.QueryTableRequestParams params,
            QueryTableResponse.TableResult tableResult,
            int numRowsExpected,
            List<String> pvNameList,
            Map<String, GrpcIntegrationIngestionServiceWrapper.IngestionStreamInfo> validationMap
    ) {
        // check table is correct format
        assertTrue(tableResult.hasColumnTable());
        final QueryTableResponse.ColumnTable resultColumnTable = tableResult.getColumnTable();
        
        if (numRowsExpected == 0) {
            assertEquals(0, resultColumnTable.getDataColumnsCount());
            assertFalse(resultColumnTable.hasDataTimestamps());
            return;
        }

        final List<Timestamp> timestampList =
                resultColumnTable.getDataTimestamps().getTimestampList().getTimestampsList();
        assertEquals(numRowsExpected, timestampList.size());
        assertEquals(pvNameList.size(), resultColumnTable.getDataColumnsCount());
        int rowIndex = 0;
        for (Timestamp timestamp : timestampList) {
            final long timestampSeconds = timestamp.getEpochSeconds();
            final long timestampNanos = timestamp.getNanoseconds();

            // check that timestamp is in query time range
            assertTrue(
                    (timestampSeconds > params.beginTimeSeconds())
                            || (timestampSeconds == params.beginTimeSeconds() && timestampNanos >= params.beginTimeNanos()));
            assertTrue(
                    (timestampSeconds < params.endTimeSeconds())
                            || (timestampSeconds == params.endTimeSeconds() && timestampNanos <= params.endTimeNanos()));

            for (DataColumn dataColumn : resultColumnTable.getDataColumnsList()) {
                // get column name and value from query result
                String columnName = dataColumn.getName();
                Double columnDataValue = dataColumn.getDataValues(rowIndex).getDoubleValue();

                // get expected value from validation map
                final TimestampMap<Double> columnValueMap = validationMap.get(columnName).valueMap;
                Double expectedColumnDataValue = columnValueMap.get(timestampSeconds, timestampNanos);
                if (expectedColumnDataValue != null) {
                    assertEquals(expectedColumnDataValue, columnDataValue, 0.0);
                } else {
                    assertEquals(0.0, columnDataValue, 0.0);
                }
            }
            rowIndex = rowIndex + 1;
        }
    }

    protected void sendAndVerifyQueryTablePvNameListColumnResult(
            int numRowsExpected,
            List<String> pvNameList,
            long startSeconds,
            long startNanos,
            long endSeconds,
            long endNanos,
            Map<String, GrpcIntegrationIngestionServiceWrapper.IngestionStreamInfo> validationMap,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final QueryTestBase.QueryTableRequestParams params =
                new QueryTestBase.QueryTableRequestParams(
                        QueryTableRequest.TableResultFormat.TABLE_FORMAT_COLUMN,
                        pvNameList,
                        null,
                        startSeconds,
                        startNanos,
                        endSeconds,
                        endNanos);
        final QueryTableResponse.TableResult tableResult = queryTable(params, expectReject, expectedRejectMessage);

        if (expectReject) {
            assertNull(tableResult);
            return;
        }

        // validate query result contents in tableResult
        verifyQueryTableColumnResult(params, tableResult, numRowsExpected, pvNameList, validationMap);
    }

    protected void sendAndVerifyQueryTablePvNamePatternColumnResult(
            int numRowsExpected,
            String pvNamePattern,
            List<String> expectedPvNameMatches,
            long startSeconds,
            long startNanos,
            long endSeconds,
            long endNanos,
            Map<String, GrpcIntegrationIngestionServiceWrapper.IngestionStreamInfo> validationMap
    ) {
        final QueryTestBase.QueryTableRequestParams params =
                new QueryTestBase.QueryTableRequestParams(
                        QueryTableRequest.TableResultFormat.TABLE_FORMAT_COLUMN,
                        null,
                        pvNamePattern,
                        startSeconds,
                        startNanos,
                        endSeconds,
                        endNanos);
        final QueryTableResponse.TableResult tableResult = queryTable(params, false, "");

        // validate query result contents in tableResult
        verifyQueryTableColumnResult(params, tableResult, numRowsExpected, expectedPvNameMatches, validationMap);
    }

    private void verifyQueryTableRowResult(
            QueryTestBase.QueryTableRequestParams params,
            QueryTableResponse.TableResult tableResult,
            int numRowsExpected,
            List<String> pvNameList,
            Map<String, GrpcIntegrationIngestionServiceWrapper.IngestionStreamInfo> validationMap
    ) {
        // check table is correct format
        assertTrue(tableResult.hasRowMapTable());
        final QueryTableResponse.RowMapTable resultRowMapTable = tableResult.getRowMapTable();

        if (numRowsExpected == 0) {
            assertEquals(0, resultRowMapTable.getColumnNamesCount());
            assertEquals(0, resultRowMapTable.getRowsCount());
            return;
        }

        // verify result column names matches list of pv names plus timestamp column
        final List<String> resultColumnNamesList = resultRowMapTable.getColumnNamesList();
        assertTrue(resultColumnNamesList.contains(QueryTableDispatcher.TABLE_RESULT_TIMESTAMP_COLUMN_NAME));
        for (String columnName : pvNameList) {
            assertTrue(resultColumnNamesList.contains(columnName));
        }
        assertEquals(pvNameList.size() + 1, resultColumnNamesList.size());

        // verify correct number of data rows
        assertEquals(numRowsExpected, resultRowMapTable.getRowsCount());

        // verify data row content
        for (QueryTableResponse.RowMapTable.DataRow resultDataRow : resultRowMapTable.getRowsList()) {
            final Map<String, DataValue> resultRowValueMap = resultDataRow.getColumnValuesMap();

            // check that map keys are present for each column
            assertEquals(resultColumnNamesList.size(), resultRowValueMap.keySet().size());
            for (String resultColumnName : resultColumnNamesList) {
                assertTrue(resultRowValueMap.containsKey(resultColumnName));
            }

            // get timestamp column value for row
            final DataValue resultRowTimestampDataValue =
                    resultRowValueMap.get(QueryTableDispatcher.TABLE_RESULT_TIMESTAMP_COLUMN_NAME);
            assertTrue(resultRowTimestampDataValue.hasTimestampValue());
            final Timestamp resultRowTimestamp = resultRowTimestampDataValue.getTimestampValue();

            // check that timestamp is in query time range
            final long resultRowSeconds = resultRowTimestamp.getEpochSeconds();
            final long resultRowNanos = resultRowTimestamp.getNanoseconds();
            assertTrue(
                    (resultRowSeconds > params.beginTimeSeconds())
                            || (resultRowSeconds == params.beginTimeSeconds() && resultRowNanos >= params.beginTimeNanos()));
            assertTrue(
                    (resultRowSeconds < params.endTimeSeconds())
                            || (resultRowSeconds == params.endTimeSeconds() && resultRowNanos <= params.endTimeNanos()));

            // verify value for each column in row
            for (String columnName : pvNameList) {
                final DataValue resultColumnDataValue = resultRowValueMap.get(columnName);
                assertTrue(resultColumnDataValue.hasDoubleValue());
                double resultColumnDoubleValue = resultColumnDataValue.getDoubleValue();
                final TimestampMap<Double> columnValueMap = validationMap.get(columnName).valueMap;
                final Double expectedColumnDoubleValue = columnValueMap.get(
                        resultRowTimestamp.getEpochSeconds(), resultRowTimestamp.getNanoseconds());
                if (expectedColumnDoubleValue != null) {
                    assertEquals(expectedColumnDoubleValue, resultColumnDoubleValue, 0.0);
                } else {
                    assertEquals(0.0, resultColumnDoubleValue, 0.0);
                }
            }
        }
    }

    protected void sendAndVerifyQueryTablePvNameListRowResult(
            int numRowsExpected,
            List<String> pvNameList,
            long startSeconds,
            long startNanos,
            long endSeconds,
            long endNanos,
            Map<String, GrpcIntegrationIngestionServiceWrapper.IngestionStreamInfo> validationMap,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final QueryTestBase.QueryTableRequestParams params =
                new QueryTestBase.QueryTableRequestParams(
                        QueryTableRequest.TableResultFormat.TABLE_FORMAT_ROW_MAP,
                        pvNameList,
                        null,
                        startSeconds,
                        startNanos,
                        endSeconds,
                        endNanos);
        final QueryTableResponse.TableResult tableResult =
                queryTable(params, expectReject, expectedRejectMessage);

        // validate query result contents in tableResult
        verifyQueryTableRowResult(params, tableResult, numRowsExpected, pvNameList, validationMap);
    }

    protected void sendAndVerifyQueryTablePvNamePatternRowResult(
            int numRowsExpected,
            String pvNamePattern,
            List<String> expectedPvNameMatches,
            long startSeconds,
            long startNanos,
            long endSeconds,
            long endNanos,
            Map<String, GrpcIntegrationIngestionServiceWrapper.IngestionStreamInfo> validationMap
    ) {
        final QueryTestBase.QueryTableRequestParams params =
                new QueryTestBase.QueryTableRequestParams(
                        QueryTableRequest.TableResultFormat.TABLE_FORMAT_ROW_MAP,
                        null,
                        pvNamePattern,
                        startSeconds,
                        startNanos,
                        endSeconds,
                        endNanos);
        final QueryTableResponse.TableResult tableResult = queryTable(params, false, "");

        // validate query result contents in tableResult
        verifyQueryTableRowResult(params, tableResult, numRowsExpected, expectedPvNameMatches, validationMap);
    }

    private void verifyQueryDataResult(
            int numBucketsExpected,
            int numSerializedDataColumnsExpected,
            QueryTestBase.QueryDataRequestParams params,
            Map<String, GrpcIntegrationIngestionServiceWrapper.IngestionStreamInfo> validationMap,
            List<DataBucket> dataBucketList
    ) {
        final List<String> pvNames = params.columnNames();
        final long beginSeconds = params.beginTimeSeconds();
        final long beginNanos = params.beginTimeNanos();
        final long endSeconds = params.endTimeSeconds();
        final long endNanos = params.endTimeNanos();

        // build map of buckets in query response for vallidation
        Map<String, TimestampMap<DataBucket>> responseBucketMap = new TreeMap<>();
        for (DataBucket dataBucket : dataBucketList) {

            // get column name from either regular DataColumn or SerializedDataColumn
            String bucketColumnName = "";
            if (dataBucket.hasDataColumn()) {
                bucketColumnName = dataBucket.getDataColumn().getName();
            } else if (dataBucket.hasSerializedDataColumn()) {
                bucketColumnName = dataBucket.getSerializedDataColumn().getName();
            }
            assertFalse(bucketColumnName.isBlank());

            final DataTimestampsUtility.DataTimestampsModel bucketDataTimestampsModel
                    = new DataTimestampsUtility.DataTimestampsModel(dataBucket.getDataTimestamps());
            final Timestamp bucketStartTimestamp = bucketDataTimestampsModel.getFirstTimestamp();
            final long bucketStartSeconds = bucketStartTimestamp.getEpochSeconds();
            final long bucketStartNanos = bucketStartTimestamp.getNanoseconds();
            TimestampMap<DataBucket> columnTimestampMap =
                    responseBucketMap.get(bucketColumnName);
            if (columnTimestampMap == null) {
                columnTimestampMap = new TimestampMap<>();
                responseBucketMap.put(bucketColumnName, columnTimestampMap);
            }
            columnTimestampMap.put(bucketStartSeconds, bucketStartNanos, dataBucket);
        }

        // iterate through the expected buckets for each column,
        // and validate them against the corresponding response bucket
        int validatedBuckets = 0;
        int serializedDataColumnCount = 0;
        for (var validationMapEntry : validationMap.entrySet()) {
            final String columnName = validationMapEntry.getKey();
            if ( ! pvNames.contains(columnName)) {
                // skip pv if not included in query
                continue;
            }
            final GrpcIntegrationIngestionServiceWrapper.IngestionStreamInfo columnStreamInfo = validationMapEntry.getValue();
            for (var bucketInfoMapEntry : columnStreamInfo.bucketInfoMap.entrySet()) {
                final long bucketSecond = bucketInfoMapEntry.getKey();
                final Map<Long, GrpcIntegrationIngestionServiceWrapper.IngestionBucketInfo> bucketNanoMap = bucketInfoMapEntry.getValue();
                for (GrpcIntegrationIngestionServiceWrapper.IngestionBucketInfo columnBucketInfo : bucketNanoMap.values()) {

                    // skip buckets outside the query range
                    if ((columnBucketInfo.startSeconds() > endSeconds)
                            || ((columnBucketInfo.startSeconds() == endSeconds) && (columnBucketInfo.startNanos() >= endNanos))) {
                        // bucket starts after query end time
                        continue;
                    }
                    if ((columnBucketInfo.endSeconds() < beginSeconds)
                            || ((columnBucketInfo.endSeconds() == beginSeconds) && (columnBucketInfo.endNanos() < beginNanos))) {
                        // bucket ends before query start time
                        continue;
                    }

                    // find the response bucket corresponding to the expected bucket
                    final DataBucket responseBucket =
                            responseBucketMap.get(columnName).get(columnBucketInfo.startSeconds(), beginNanos);
                    final DataTimestampsUtility.DataTimestampsModel responseBucketDataTimestampsModel =
                            new DataTimestampsUtility.DataTimestampsModel(responseBucket.getDataTimestamps());

                    assertEquals(
                            columnBucketInfo.intervalNanos(),
                            responseBucketDataTimestampsModel.getSamplePeriodNanos());
                    assertEquals(
                            columnBucketInfo.numValues(),
                            responseBucketDataTimestampsModel.getSampleCount());

                    // validate bucket data values
                    int valueIndex = 0;

                    // get DataColumn from bucket, or parse from bucket's SerializedDataColumn
                    DataColumn responseDataColumn = null;
                    if (responseBucket.hasDataColumn()) {
                        responseDataColumn = responseBucket.getDataColumn();

                    } else if (responseBucket.hasSerializedDataColumn()) {
                        try {
                            responseDataColumn = DataColumn.parseFrom(
                                    responseBucket.getSerializedDataColumn().getPayload());
                        } catch (InvalidProtocolBufferException e) {
                            fail("exception parsing DataColumn from SerializedDataColumn: " + e.getMessage());
                        }
                        serializedDataColumnCount = serializedDataColumnCount + 1;

                    } else {
                        fail("responseBucket doesn't contain either DataColumn or SerializedDataColumn");
                    }
                    assertNotNull(responseDataColumn);

                    // compare expected and actual data values
                    for (DataValue responseDataValue : responseDataColumn.getDataValuesList()) {

                        final double actualDataValue = responseDataValue.getDoubleValue();

                        Object expectedValue = columnBucketInfo.dataValues().get(valueIndex);
                        assertTrue(expectedValue instanceof Double);
                        Double expectedDataValue = (Double) expectedValue;
                        assertEquals(expectedDataValue, actualDataValue, 0.0);

                        valueIndex = valueIndex + 1;
                    }

                    // check tags
                    if (columnBucketInfo.tags() != null) {
                        assertEquals(columnBucketInfo.tags(), responseBucket.getTagsList());
                    } else {
                        assertTrue(responseBucket.getTagsList().isEmpty());
                    }

                    // check attributes
                    if (columnBucketInfo.attributes() != null) {
                        assertEquals(
                                columnBucketInfo.attributes(),
                                AttributesUtility.attributeMapFromList(responseBucket.getAttributesList()));
                    } else {
                        assertTrue(responseBucket.getAttributesList().isEmpty());
                    }

                    // check event metadata
                    if (columnBucketInfo.eventDescription() != null) {
                        assertEquals(
                                columnBucketInfo.eventDescription(),
                                responseBucket.getEventMetadata().getDescription());
                    }
                    if (columnBucketInfo.eventStartSeconds() != null) {
                        assertEquals(
                                (long) columnBucketInfo.eventStartSeconds(),
                                responseBucket.getEventMetadata().getStartTimestamp().getEpochSeconds());
                    }
                    if (columnBucketInfo.eventStartNanos() != null) {
                        assertEquals(
                                (long) columnBucketInfo.eventStartNanos(),
                                responseBucket.getEventMetadata().getStartTimestamp().getNanoseconds());
                    }
                    if (columnBucketInfo.eventStopSeconds() != null) {
                        assertEquals(
                                (long) columnBucketInfo.eventStopSeconds(),
                                responseBucket.getEventMetadata().getStopTimestamp().getEpochSeconds());
                    }
                    if (columnBucketInfo.eventStopNanos() != null) {
                        assertEquals(
                                (long) columnBucketInfo.eventStopNanos(),
                                responseBucket.getEventMetadata().getStopTimestamp().getNanoseconds());
                    }

                    validatedBuckets = validatedBuckets + 1;
                }
            }
        }

        // check that we validated all buckets returned by the query, and that query returned expected number of buckets
        assertEquals(dataBucketList.size(), validatedBuckets);
        assertEquals(numBucketsExpected, dataBucketList.size());
        assertEquals(numSerializedDataColumnsExpected, serializedDataColumnCount);
    }

    protected List<DataBucket> sendQueryData(
            QueryDataRequest request,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final DpQueryServiceGrpc.DpQueryServiceStub asyncStub = DpQueryServiceGrpc.newStub(channel);

        final QueryTestBase.QueryDataResponseStreamObserver responseObserver =
                QueryTestBase.QueryDataResponseStreamObserver.newQueryDataUnaryObserver();

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            asyncStub.queryData(request, responseObserver);
        }).start();

        responseObserver.await();

        if (expectReject) {
            assertTrue(responseObserver.isError());
            assertTrue(responseObserver.getErrorMessage().contains(expectedRejectMessage));
        } else {
            assertFalse(responseObserver.getErrorMessage(), responseObserver.isError());
        }

        return responseObserver.getDataBucketList();
    }

    protected List<DataBucket> queryData(
            QueryTestBase.QueryDataRequestParams params,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final QueryDataRequest request = QueryTestBase.buildQueryDataRequest(params);
        return sendQueryData(request, expectReject, expectedRejectMessage);
    }

    public List<DataBucket> sendAndVerifyQueryData(
            int numBucketsExpected,
            int numSerializedDataColumnsExpected,
            QueryTestBase.QueryDataRequestParams params,
            Map<String, GrpcIntegrationIngestionServiceWrapper.IngestionStreamInfo> validationMap,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final List<DataBucket> dataBucketList =
                queryData(params, expectReject, expectedRejectMessage);

        if (expectReject) {
            assertTrue(dataBucketList.isEmpty());
            return dataBucketList;
        }

        verifyQueryDataResult(
                numBucketsExpected, numSerializedDataColumnsExpected, params, validationMap, dataBucketList);

        return dataBucketList;
    }

    protected List<DataBucket> sendQueryDataStream(
            QueryDataRequest request,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final DpQueryServiceGrpc.DpQueryServiceStub asyncStub = DpQueryServiceGrpc.newStub(channel);

        final QueryTestBase.QueryDataResponseStreamObserver responseObserver =
                QueryTestBase.QueryDataResponseStreamObserver.newQueryDataStreamObserver();

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            asyncStub.queryDataStream(request, responseObserver);
        }).start();

        responseObserver.await();

        if (expectReject) {
            assertTrue(responseObserver.isError());
            assertTrue(responseObserver.getErrorMessage().contains(expectedRejectMessage));
            return null;
        } else {
            assertFalse(responseObserver.getErrorMessage(), responseObserver.isError());
            return responseObserver.getDataBucketList();
        }
    }

    public List<DataBucket> queryDataStream(
            QueryTestBase.QueryDataRequestParams params,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final QueryDataRequest request = QueryTestBase.buildQueryDataRequest(params);
        return sendQueryDataStream(request, expectReject, expectedRejectMessage);
    }

    protected void sendAndVerifyQueryDataStream(
            int numBucketsExpected,
            int numSerializedDataColumnsExpected,
            QueryTestBase.QueryDataRequestParams params,
            Map<String, GrpcIntegrationIngestionServiceWrapper.IngestionStreamInfo> validationMap,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final List<DataBucket> dataBucketList =
                queryDataStream(params, expectReject, expectedRejectMessage);

        if (expectReject) {
            assertNull(dataBucketList);
            return;
        }

        verifyQueryDataResult(
                numBucketsExpected, numSerializedDataColumnsExpected, params, validationMap, dataBucketList);
    }

    protected List<DataBucket> sendQueryDataBidiStream(
            QueryDataRequest request,
            int numBucketsExpected,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final DpQueryServiceGrpc.DpQueryServiceStub asyncStub = DpQueryServiceGrpc.newStub(channel);

        final QueryTestBase.QueryDataResponseStreamObserver responseObserver =
                QueryTestBase.QueryDataResponseStreamObserver.newQueryDataBidiStreamObserver(numBucketsExpected);

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            StreamObserver<QueryDataRequest> requestObserver = asyncStub.queryDataBidiStream(responseObserver);
            responseObserver.setRequestObserver(requestObserver);
            requestObserver.onNext(request);
        }).start();

        responseObserver.await();

        if (expectReject) {
            assertTrue(responseObserver.isError());
            assertTrue(responseObserver.getErrorMessage().contains(expectedRejectMessage));
            return null;
        } else {
            assertFalse(responseObserver.getErrorMessage(), responseObserver.isError());
            return responseObserver.getDataBucketList();
        }
    }

    protected List<DataBucket> queryDataBidiStream(
            int numBucketsExpected,
            QueryTestBase.QueryDataRequestParams params,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final QueryDataRequest request = QueryTestBase.buildQueryDataRequest(params);
        return sendQueryDataBidiStream(request, numBucketsExpected, expectReject, expectedRejectMessage);
    }

    protected void sendAndVerifyQueryDataBidiStream(
            int numBucketsExpected,
            int numSerializedDataColumnsExpected,
            QueryTestBase.QueryDataRequestParams params,
            Map<String, GrpcIntegrationIngestionServiceWrapper.IngestionStreamInfo> validationMap,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final List<DataBucket> dataBucketList =
                queryDataBidiStream(numBucketsExpected, params, expectReject, expectedRejectMessage);

        if (expectReject) {
            assertEquals(null, dataBucketList);
            return;
        }

        verifyQueryDataResult(
                numBucketsExpected,
                numSerializedDataColumnsExpected,
                params,
                validationMap,
                dataBucketList);
    }

    protected List<QueryPvMetadataResponse.MetadataResult.PvInfo> sendQueryPvMetadata(
            QueryPvMetadataRequest request, boolean expectReject, String expectedRejectMessage
    ) {
        final DpQueryServiceGrpc.DpQueryServiceStub asyncStub = DpQueryServiceGrpc.newStub(channel);

        final QueryTestBase.QueryPvMetadataResponseObserver responseObserver =
                new QueryTestBase.QueryPvMetadataResponseObserver();

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            asyncStub.queryPvMetadata(request, responseObserver);
        }).start();

        responseObserver.await();

        if (expectReject) {
            assertTrue(responseObserver.isError());
            assertTrue(responseObserver.getErrorMessage().contains(expectedRejectMessage));
        } else {
            assertFalse(responseObserver.getErrorMessage(), responseObserver.isError());
        }

        return responseObserver.getPvInfoList();
    }

    private void sendAndVerifyQueryPvMetadata(
            QueryPvMetadataRequest request,
            List<String> pvNames,
            Map<String, GrpcIntegrationIngestionServiceWrapper.IngestionStreamInfo> validationMap,
            boolean expectReject,
            String expectedRejectMessage,
            boolean expectEmpty) {
        final List<QueryPvMetadataResponse.MetadataResult.PvInfo> pvInfoList =
                sendQueryPvMetadata(request, expectReject, expectedRejectMessage);

        if (expectReject || expectEmpty) {
            assertEquals(0, pvInfoList.size());
            return;
        }

        // verify results, check that there is a ColumnInfo for each column in the query
        assertEquals(pvNames.size(), pvInfoList.size());

        // build map of pv info list for convenience
        final Map<String, QueryPvMetadataResponse.MetadataResult.PvInfo> pvInfoMap = new HashMap<>();
        for (QueryPvMetadataResponse.MetadataResult.PvInfo columnInfo : pvInfoList) {
            pvInfoMap.put(columnInfo.getPvName(), columnInfo);
        }

        // build list of pv names in response to verify against expected
        final List<String> responsePvNames = new ArrayList<>();
        for (QueryPvMetadataResponse.MetadataResult.PvInfo pvInfo : pvInfoList) {
            responsePvNames.add(pvInfo.getPvName());
        }

        // check that response pvNames are sorted (against list in sorted order)
        assertEquals(pvNames, responsePvNames);

        // check that a PvInfo was received for each name and verify its contents
        for (String pvName : pvNames) {
            final QueryPvMetadataResponse.MetadataResult.PvInfo pvInfo =
                    pvInfoMap.get(pvName);
            assertNotNull(pvInfo);
            assertEquals(pvName, pvInfo.getPvName());
            assertEquals(8, pvInfo.getLastBucketDataTypeCase());
            assertEquals("DOUBLEVALUE", pvInfo.getLastBucketDataType());
            assertEquals(1, pvInfo.getLastBucketDataTimestampsCase());
            assertEquals("SAMPLINGCLOCK", pvInfo.getLastBucketDataTimestampsType());

            // iterate through validationMap to get info for first and last bucket for pv, number of buckets
            GrpcIntegrationIngestionServiceWrapper.IngestionBucketInfo firstBucketInfo = null;
            GrpcIntegrationIngestionServiceWrapper.IngestionBucketInfo lastBucketInfo = null;
            int numBuckets = 0;
            boolean first = true;
            for (var bucketMapEntry : validationMap.get(pvName).bucketInfoMap.entrySet()) {
                final var nanoMap = bucketMapEntry.getValue();
                for (var nanoMapEntry : nanoMap.entrySet()) {
                    numBuckets = numBuckets + 1;
                    if (first) {
                        firstBucketInfo = nanoMapEntry.getValue();
                        first = false;
                    }
                    lastBucketInfo = nanoMapEntry.getValue();
                }
            }

            // verify pvInfo contents for column against last and first bucket details
            assertNotNull(lastBucketInfo);
            assertEquals(lastBucketInfo.intervalNanos(), pvInfo.getLastBucketSamplePeriod());
            assertEquals(lastBucketInfo.numValues(), pvInfo.getLastBucketSampleCount());
            assertEquals(lastBucketInfo.endSeconds(), pvInfo.getLastDataTimestamp().getEpochSeconds());
            assertEquals(lastBucketInfo.endNanos(), pvInfo.getLastDataTimestamp().getNanoseconds());
            assertNotNull(firstBucketInfo);
            assertEquals(firstBucketInfo.startSeconds(), pvInfo.getFirstDataTimestamp().getEpochSeconds());
            assertEquals(firstBucketInfo.startNanos(), pvInfo.getFirstDataTimestamp().getNanoseconds());
            assertEquals(numBuckets, pvInfo.getNumBuckets());

            // check last bucket id
            final String expectedLastBucketId =
                    pvName + "-" + lastBucketInfo.startSeconds() + "-" + lastBucketInfo.startNanos();
            assertEquals(expectedLastBucketId, pvInfo.getLastBucketId());

        }
    }

    protected void sendAndVerifyQueryPvMetadata(
            List<String> columnNames,
            Map<String, GrpcIntegrationIngestionServiceWrapper.IngestionStreamInfo> validationMap,
            boolean expectReject,
            String expectedRejectMessage,
            boolean expectEmpty
    ) {
        final QueryPvMetadataRequest request = QueryTestBase.buildQueryPvMetadataRequest(columnNames);
        sendAndVerifyQueryPvMetadata(
                request, columnNames, validationMap, expectReject, expectedRejectMessage, expectEmpty);
    }

    protected void sendAndVerifyQueryPvMetadata(
            String columnNamePattern,
            Map<String, GrpcIntegrationIngestionServiceWrapper.IngestionStreamInfo> validationMap,
            List<String> expectedColumnNames,
            boolean expectReject,
            String expectedRejectMessage,
            boolean expectEmpty
    ) {
        final QueryPvMetadataRequest request = QueryTestBase.buildQueryPvMetadataRequest(columnNamePattern);
        sendAndVerifyQueryPvMetadata(
                request, expectedColumnNames, validationMap, expectReject, expectedRejectMessage, expectEmpty);
    }

    private List<QueryProvidersResponse.ProvidersResult.ProviderInfo> sendQueryProviders(
            QueryProvidersRequest request,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final DpQueryServiceGrpc.DpQueryServiceStub asyncStub = DpQueryServiceGrpc.newStub(channel);

        final QueryTestBase.QueryProvidersResponseObserver responseObserver =
                new QueryTestBase.QueryProvidersResponseObserver();

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            asyncStub.queryProviders(request, responseObserver);
        }).start();

        responseObserver.await();

        if (expectReject) {
            assertTrue(responseObserver.isError());
            assertTrue(responseObserver.getErrorMessage().contains(expectedRejectMessage));
        } else {
            assertFalse(responseObserver.getErrorMessage(), responseObserver.isError());
        }

        return responseObserver.getProviderInfoList();
    }

    protected void sendAndVerifyQueryProviders(
            QueryTestBase.QueryProvidersRequestParams queryParams,
            int numMatchesExpected,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final QueryProvidersRequest request = QueryTestBase.buildQueryProvidersRequest(queryParams);

        final List<QueryProvidersResponse.ProvidersResult.ProviderInfo> queryResultProviderList =
                sendQueryProviders(request, expectReject, expectedRejectMessage);

        if (expectReject) {
            assertEquals(0, queryResultProviderList.size());
            return;
        }

        // verify query results
        assertEquals(numMatchesExpected, queryResultProviderList.size());

        // confirm that each query result corresponds to search criteria
        for (QueryProvidersResponse.ProvidersResult.ProviderInfo providerInfo : queryResultProviderList) {

            if (queryParams.idCriterion != null) {
                assertEquals(queryParams.idCriterion, providerInfo.getId());
            }

            if (queryParams.textCriterion != null) {
                assertTrue((providerInfo.getName().contains(queryParams.textCriterion)) ||
                        (providerInfo.getDescription().contains(queryParams.textCriterion)));
            }

            if (queryParams.tagsCriterion != null) {
                assertTrue(providerInfo.getTagsList().contains(queryParams.tagsCriterion));
            }

            if (queryParams.attributesCriterionKey != null) {
                assertNotNull(queryParams.attributesCriterionValue);
                final Map<String, String> resultAttributeMap =
                        AttributesUtility.attributeMapFromList(providerInfo.getAttributesList());
                assertEquals(
                        queryParams.attributesCriterionValue,
                        resultAttributeMap.get(queryParams.attributesCriterionKey));
            }
        }
    }

    private List<ProviderMetadata> sendQueryProviderMetadata(
            QueryProviderMetadataRequest request,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final DpQueryServiceGrpc.DpQueryServiceStub asyncStub = DpQueryServiceGrpc.newStub(channel);

        final QueryTestBase.QueryProviderMetadataResponseObserver responseObserver =
                new QueryTestBase.QueryProviderMetadataResponseObserver();

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            asyncStub.queryProviderMetadata(request, responseObserver);
        }).start();

        responseObserver.await();

        if (expectReject) {
            assertTrue(responseObserver.isError());
            assertTrue(responseObserver.getErrorMessage().contains(expectedRejectMessage));
        } else {
            assertFalse(responseObserver.getErrorMessage(), responseObserver.isError());
        }

        return responseObserver.getProviderMetadataList();
    }

    protected void sendAndVerifyQueryProviderMetadata(
            String providerId,
            GrpcIntegrationIngestionServiceWrapper.IngestionProviderInfo providerInfo,
            boolean expectReject,
            String expectedRejectMessage,
            int numMatchesExpected
    ) {

        final QueryProviderMetadataRequest request = QueryTestBase.buildQueryProviderMetadataRequest(providerId);

        final List<ProviderMetadata> providerMetadataList =
                sendQueryProviderMetadata(request, expectReject, expectedRejectMessage);

        if (expectReject || numMatchesExpected == 0) {
            assertEquals(0, providerMetadataList.size());
            return;
        }

        // verify results, check that there is a ColumnInfo for each column in the query
        assertEquals(numMatchesExpected, providerMetadataList.size());
        final ProviderMetadata responseProviderMetadata =
                providerMetadataList.get(0);
        assertEquals(providerId, responseProviderMetadata.getId());
        assertEquals(providerInfo.numBuckets(), responseProviderMetadata.getNumBuckets());
        assertEquals(providerInfo.firstTimeSeconds(), responseProviderMetadata.getFirstBucketTime().getEpochSeconds());
        assertEquals(providerInfo.firstTimeNanos(), responseProviderMetadata.getFirstBucketTime().getNanoseconds());
        assertEquals(providerInfo.lastTimeSeconds(), responseProviderMetadata.getLastBucketTime().getEpochSeconds());
        assertEquals(providerInfo.lastTimeNanos(), responseProviderMetadata.getLastBucketTime().getNanoseconds());
        assertEquals(providerInfo.pvNameSet().size(), responseProviderMetadata.getPvNamesCount());
        assertEquals(providerInfo.pvNameSet(), new HashSet<>(responseProviderMetadata.getPvNamesList()));
    }

}
