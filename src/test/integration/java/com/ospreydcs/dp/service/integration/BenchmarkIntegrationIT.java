package com.ospreydcs.dp.service.integration;

import com.ospreydcs.dp.grpc.v1.annotation.ExportDataRequest;
import com.ospreydcs.dp.grpc.v1.annotation.ExportDataResponse;
import com.ospreydcs.dp.grpc.v1.common.*;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataResponse;
import com.ospreydcs.dp.grpc.v1.query.QueryDataRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryDataResponse;
import com.ospreydcs.dp.service.annotation.AnnotationTestBase;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.bson.RequestStatusDocument;
import com.ospreydcs.dp.service.common.bson.EventMetadataDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.model.BenchmarkScenarioResult;
import com.ospreydcs.dp.service.ingest.benchmark.BenchmarkIngestDataBidiStream;
import com.ospreydcs.dp.service.ingest.benchmark.ColumnDataType;
import com.ospreydcs.dp.service.ingest.benchmark.IngestionBenchmarkBase;
import com.ospreydcs.dp.service.query.QueryTestBase;
import com.ospreydcs.dp.service.query.benchmark.*;
import io.grpc.Channel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.junit.Assert.*;

@Ignore("This test class is disabled for now because of performance considerations.  It doesn't provide unique test coverage")
@RunWith(JUnit4.class)
public class BenchmarkIntegrationIT extends GrpcIntegrationTestBase {

    // static variables
    private static final Logger logger = LogManager.getLogger();
    private static IntegrationTestIngestionGrpcClient ingestionGrpcClient;
    private static IntegrationTestQueryGrpcClient queryGrpcClient;

    // ingestion constants
    private static final int INGESTION_NUM_PVS = 4000;
    private static final int INGESTION_NUM_THREADS = 7;
    private static final int INGESTION_NUM_STREAMS = 20;
    private static final int INGESTION_NUM_ROWS = 1000;
    private static final int INGESTION_NUM_SECONDS = 60;

    // query constants
    private static final int NUM_SCENARIO_SECONDS = 60;
    private static final int QUERY_NUM_PVS = 100;
    private static final int QUERY_NUM_PVS_PER_REQUEST = 10;
    private static final int QUERY_NUM_THREADS = 7;
    private static final int QUERY_SINGLE_NUM_PVS = 10;
    private static final int QUERY_SINGLE_NUM_PVS_PER_REQUEST = 1;


    private class IntegrationTestStreamingIngestionApp extends BenchmarkIngestDataBidiStream {

        private class IntegrationTestIngestionRequestInfo {
            public final String providerId;
            public final long startSeconds;
            public boolean responseReceived = false;
            public IntegrationTestIngestionRequestInfo(String providerId, long startSeconds) {
                this.providerId = providerId;
                this.startSeconds = startSeconds;
            }
        }

        private class IntegrationTestIngestionTask
                extends BidiStreamingIngestionTask
        {
            // instance variables
            private Map<String, IntegrationTestIngestionRequestInfo> requestValidationMap = new TreeMap<>();
            private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
            private final Lock readLock = rwLock.readLock();
            private final Lock writeLock = rwLock.writeLock();
            private final AtomicInteger responseCount = new AtomicInteger(0);
            private final AtomicInteger dbBucketCount = new AtomicInteger(0);

            public IntegrationTestIngestionTask(
                    IngestionBenchmarkBase.IngestionTaskParams params,
                    DataFrame.Builder templateDataTable,
                    Channel channel) {

                super(params, templateDataTable, channel);
            }

            @Override
            protected void onRequest(IngestDataRequest request) {

                logger.trace("onRequest stream: " + this.params.streamNumber);

                // acquire writeLock for updating map
                writeLock.lock();
                try {
                    // add an entry for the request to the validation map
                    requestValidationMap.put(
                            request.getClientRequestId(),
                            new IntegrationTestIngestionRequestInfo(
                                    request.getProviderId(),
                                    request
                                            .getIngestionDataFrame()
                                            .getDataTimestamps()
                                            .getSamplingClock()
                                            .getStartTime()
                                            .getEpochSeconds()));

                } finally {
                    // using try...finally to make sure we unlock!
                    writeLock.unlock();
                }
            }

            @Override
            protected void onResponse(IngestDataResponse response) {

                logger.trace("onResponse stream: " + this.params.streamNumber);
                responseCount.incrementAndGet();

                // acquire writeLock for updating map
                writeLock.lock();
                try {
                    final String responseRequestId = response.getClientRequestId();
                    IntegrationTestIngestionRequestInfo requestInfo =
                            requestValidationMap.get(responseRequestId);

                    // check that requestId in response matches a request
                    if (requestInfo == null) {
                        fail("response contains unexpected requestId: " + responseRequestId);
                        return;
                    }

                    // check that provider in response matches request
                    final String responseProviderId = response.getProviderId();
                    if (responseProviderId != requestInfo.providerId) {
                        fail("response provider id: " + responseProviderId
                                + " mismatch request: " + requestInfo.providerId);
                        return;
                    }

                    // validate dimensions in ack
                    assertEquals(this.params.numRows, response.getAckResult().getNumRows());
                    assertEquals(this.params.numColumns, response.getAckResult().getNumColumns());

                    // set validation flag for request
                    requestInfo.responseReceived = true;

                } finally {
                    // using try...finally to make sure we unlock!
                    writeLock.unlock();
                }

            }

            private void verifyRequestDbArtifacts(
                    String requestId, IntegrationTestIngestionRequestInfo requestInfo
            ) {
                // verify request status
                RequestStatusDocument statusDocument =
                        mongoClient.findRequestStatus(requestInfo.providerId, requestId);
                assertEquals(params.numColumns, statusDocument.getIdsCreated().size());

                // verify buckets
                for (int colIndex = params.firstColumnIndex; colIndex <= params.lastColumnIndex; colIndex++) {
                    final String columnName = NAME_COLUMN_BASE + colIndex;
                    final String bucketId = columnName + "-" + requestInfo.startSeconds + "-0";
                    assertTrue(
                            "providerId: " + requestInfo.providerId
                                    + " requestId: " + requestId
                                    + " bucketId: " + bucketId,
                            statusDocument.getIdsCreated().contains(bucketId));
                    BucketDocument bucketDocument = mongoClient.findBucket(bucketId);
                    assertNotNull("bucketId: " + bucketId, bucketDocument);
                    assertEquals(columnName, bucketDocument.getPvName());
                    assertEquals(bucketId, bucketDocument.getId());
                    assertEquals(params.numRows, bucketDocument.getDataTimestamps().getSampleCount());
                    assertEquals(1000000, bucketDocument.getDataTimestamps().getSamplePeriod());
                    assertEquals(
                            requestInfo.startSeconds, bucketDocument.getDataTimestamps().getFirstTime().getSeconds());
                    assertEquals(0, bucketDocument.getDataTimestamps().getFirstTime().getNanos());
                    assertEquals(
                            Date.from(Instant.ofEpochSecond(requestInfo.startSeconds, 0L)),
                            bucketDocument.getDataTimestamps().getFirstTime().getDateTime());
                    assertEquals(requestInfo.startSeconds, bucketDocument.getDataTimestamps().getLastTime().getSeconds());
                    assertEquals(999000000L, bucketDocument.getDataTimestamps().getLastTime().getNanos());
                    assertEquals(
                            Date.from(Instant.ofEpochSecond(requestInfo.startSeconds, 999000000L)),
                            bucketDocument.getDataTimestamps().getLastTime().getDateTime());
                    final EventMetadataDocument eventMetadataDocument = bucketDocument.getEvent();
                    assertEquals("calibration test", eventMetadataDocument.getDescription());
                    assertEquals(
                            params.startSeconds,
                            eventMetadataDocument.getStartTime().getSeconds());
                    assertEquals(0, eventMetadataDocument.getStartTime().getNanos());
                    assertEquals("07", bucketDocument.getAttributes().get("sector"));
                    assertEquals("vacuum", bucketDocument.getAttributes().get("subsystem"));

                    DataColumn bucketDataColumn = null;
                    try {
                        bucketDataColumn = bucketDocument.getDataColumn().toDataColumn();
                    } catch (DpException e) {
                        throw new RuntimeException(e);
                    }
                    Objects.requireNonNull(bucketDataColumn);

                    assertEquals(params.numRows, bucketDataColumn.getDataValuesList().size());
                    // verify each value
                    for (int valIndex = 0; valIndex < bucketDocument.getDataTimestamps().getSampleCount() ; ++valIndex) {
                        final double expectedValue =
                                valIndex + (double) valIndex / bucketDocument.getDataTimestamps().getSampleCount();
                        assertEquals(expectedValue, bucketDataColumn.getDataValues(valIndex).getDoubleValue(), 0.0);
                    }
                    dbBucketCount.incrementAndGet();
                }
            }

            @Override
            protected void onCompleted() {

                logger.trace("onCompleted stream: " + this.params.streamNumber);

                readLock.lock();
                try {
                    // iterate through requestMap and make sure all requests were acked/verified
                    for (var entry : requestValidationMap.entrySet()) {
                        String requestId = entry.getKey();
                        IntegrationTestIngestionRequestInfo requestInfo = entry.getValue();
                        if (!requestInfo.responseReceived) {
                            fail("did not receive ack for request: " + entry.getKey());
                        }
                        verifyRequestDbArtifacts(requestId, requestInfo);
                    }

                } finally {
                    readLock.unlock();
                }

                logger.debug(
                        "stream: {} ingestion task verified {} IngestionResponse messages, {} mongodb buckets",
                        params.streamNumber, responseCount.get(), dbBucketCount.get());

            }

        }

        protected BidiStreamingIngestionTask newIngestionTask(
                IngestionTaskParams params, DataFrame.Builder templateDataTable, Channel channel
        ) {
            return new IntegrationTestIngestionTask(params, templateDataTable, channel);
        }
    }

    protected class IntegrationTestIngestionGrpcClient {

        final private Channel channel;

        public IntegrationTestIngestionGrpcClient(Channel channel) {
            this.channel = channel;
        }

        private void runStreamingIngestionScenario() {

            final int numColumnsPerStream = INGESTION_NUM_PVS / INGESTION_NUM_STREAMS;

            System.out.println();
            System.out.println("========== running ingestion scenario ==========");
            System.out.println("number of PVs: " + INGESTION_NUM_PVS);
            System.out.println("number of seconds (one bucket per PV per second): " + INGESTION_NUM_SECONDS);
            System.out.println("sampling interval (Hz): " + INGESTION_NUM_ROWS);
            System.out.println("number of ingestion API streams: " + INGESTION_NUM_STREAMS);
            System.out.println("number of PVs per stream: " + numColumnsPerStream);
            System.out.println("executorService thread pool size: " + INGESTION_NUM_THREADS);

            IntegrationTestStreamingIngestionApp ingestionApp = new IntegrationTestStreamingIngestionApp();
            BenchmarkScenarioResult scenarioResult = ingestionApp.ingestionScenario(
                    channel,
                    INGESTION_NUM_THREADS,
                    INGESTION_NUM_STREAMS,
                    INGESTION_NUM_ROWS,
                    numColumnsPerStream,
                    INGESTION_NUM_SECONDS,
                    false,
                    ColumnDataType.DATA_COLUMN);
            assertTrue(scenarioResult.success);

            System.out.println("========== ingestion scenario completed ==========");
            System.out.println();
        }
    }

    private static class IntegrationTestQueryTaskValidationHelper {

        // instance variables
        final QueryBenchmarkBase.QueryDataRequestTaskParams params;
        final Map<String,boolean[]> columnBucketMap = new TreeMap<>();
        private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
        private final Lock readLock = rwLock.readLock();
        private final Lock writeLock = rwLock.writeLock();
        private final AtomicInteger responseCount = new AtomicInteger(0);

        public IntegrationTestQueryTaskValidationHelper(QueryBenchmarkBase.QueryDataRequestTaskParams params) {
            this.params = params;
        }

        protected void onRequest(QueryDataRequest request) {

            writeLock.lock();
            try {
                // add data structure for tracking expected buckets for each column in request
                assertTrue(request.hasQuerySpec());
                assertTrue(request.getQuerySpec().getPvNamesCount() > 0);
                for (String columnName : request.getQuerySpec().getPvNamesList()) {
                    assertNotNull(request.getQuerySpec().getBeginTime());
                    final long startSeconds = request.getQuerySpec().getBeginTime().getEpochSeconds();
                    assertNotNull(request.getQuerySpec().getEndTime());
                    final long endSeconds = request.getQuerySpec().getEndTime().getEpochSeconds();
                    final int numSeconds = (int) (endSeconds - startSeconds);
                    assertTrue(numSeconds > 0);
                    final boolean[] columnBucketArray = new boolean[numSeconds];
                    for (int i = 0 ; i < numSeconds ; i++) {
                        columnBucketArray[i] = false;
                    }
                    columnBucketMap.put(columnName, columnBucketArray);
                }

            } finally {
                writeLock.unlock();
            }
        }

        protected void onResponse(QueryDataResponse response) {

            assertNotNull(response.getResponseTime() != null);
            assertTrue(response.getResponseTime().getEpochSeconds() > 0);
            assertTrue(response.hasQueryData());
            final QueryDataResponse.QueryData queryData = response.getQueryData();

            responseCount.incrementAndGet();

            writeLock.lock();
            try {
                // verify buckets in response
                assertTrue(queryData.getDataBucketsCount() > 0);
                for (DataBucket bucket : queryData.getDataBucketsList()) {

                    assertTrue(bucket.hasDataColumn());
                    final DataColumn dataColumn = bucket.getDataColumn();
                    final String columnName = dataColumn.getName();
                    assertNotNull(columnName);
                    assertNotNull(dataColumn.getDataValuesList());
                    assertTrue(dataColumn.getDataValuesCount() == INGESTION_NUM_ROWS);
                    for (int i = 0 ; i < INGESTION_NUM_ROWS ; ++i) {
                        final DataValue dataValue = dataColumn.getDataValues(i);
                        assertNotNull(dataValue);
                        final double actualValue = dataValue.getDoubleValue();
                        assertNotNull(actualValue);
                        final double expectedValue = i + (double) i / INGESTION_NUM_ROWS;
                        assertEquals(
                                "value mismatch: " + dataValue + " expected: " + actualValue,
                                expectedValue, actualValue, 0.0);
                    }
                    assertTrue(bucket.hasDataTimestamps());
                    assertTrue(bucket.getDataTimestamps().hasSamplingClock());
                    SamplingClock samplingClock = bucket.getDataTimestamps().getSamplingClock();
                    assertNotNull(samplingClock);
                    assertNotNull(samplingClock.getStartTime());
                    assertTrue(samplingClock.getStartTime().getEpochSeconds() > 0);
                    assertTrue(samplingClock.getPeriodNanos() > 0);
                    assertEquals(INGESTION_NUM_ROWS, samplingClock.getCount());
                    final long bucketSeconds = samplingClock.getStartTime().getEpochSeconds();
                    final int bucketIndex = (int) (bucketSeconds - params.startSeconds());
                    final boolean[] columnBucketArray = columnBucketMap.get(columnName);
                    assertNotNull(columnBucketArray);

                    // mark bucket as received in tracking data structure
                    // this code assumes that we are using whole seconds and will fail if we are using nanos
                    // because the expected number of buckets is incorrect for partial seconds
                    assert(bucketIndex < columnBucketArray.length);
                    columnBucketArray[bucketIndex] = true;
                }

            } finally {
                writeLock.unlock();
            }
        }

        protected void onCompleted() {

            readLock.lock();
            try {
                // check that we recevied all expected buckets for each column
                for (var entry : columnBucketMap.entrySet()) {
                    final String columnName = entry.getKey();
                    final boolean[] columnBucketArray = entry.getValue();
                    for (int secondOffset = 0 ; secondOffset < columnBucketArray.length ; ++secondOffset) {
                        assertTrue(
                                "no bucket received column: " + columnName + " secondOffset: " + secondOffset,
                                columnBucketArray[secondOffset]);
                    }
                }

            } finally {
                readLock.unlock();
            }

            logger.debug("stream: {} validation helper verified {} QueryResponse messages",
                    params.streamNumber(), responseCount.get());
        }

    }

    private static class IntegrationTestQueryDataBidiStreamApp extends BenchmarkQueryDataBidiStream {

        private static class IntegrationTestQueryResponseCursorTask
                extends BenchmarkQueryDataBidiStream.QueryResponseCursorTask
        {
            final private IntegrationTestQueryTaskValidationHelper helper;

            public IntegrationTestQueryResponseCursorTask(Channel channel, QueryDataRequestTaskParams params
            ) {
                super(channel, params);
                helper = new IntegrationTestQueryTaskValidationHelper(params);
            }

            @Override
            protected void onRequest(QueryDataRequest request) {
                helper.onRequest(request);
            }

            @Override
            protected void onResponse(QueryDataResponse response) {
                helper.onResponse(response);
            }

            @Override
            protected void onCompleted() {
                helper.onCompleted();
            }

        }

        @Override
        protected QueryResponseCursorTask newQueryTask(
                Channel channel, QueryDataRequestTaskParams params
        ) {
            return new IntegrationTestQueryResponseCursorTask(channel, params);
        }

    }

    private static class IntegrationTestQueryDataStreamApp extends BenchmarkQueryDataStream {

        private static class IntegrationTestQueryResponseStreamTask
                extends BenchmarkQueryDataStream.QueryResponseStreamTask {

            final private IntegrationTestQueryTaskValidationHelper helper;

            public IntegrationTestQueryResponseStreamTask(Channel channel, QueryDataRequestTaskParams params) {
                super(channel, params);
                helper = new IntegrationTestQueryTaskValidationHelper(params);
            }

            @Override
            protected void onRequest(QueryDataRequest request) {
                helper.onRequest(request);
            }

            @Override
            protected void onResponse(QueryDataResponse response) {
                helper.onResponse(response);
            }

            @Override
            protected void onCompleted() {
                helper.onCompleted();
            }

        }

        @Override
        protected QueryResponseStreamTask newQueryTask(
                Channel channel, QueryDataRequestTaskParams params
        ) {
            return new IntegrationTestQueryResponseStreamTask(channel, params);
        }
    }

    private static class IntegrationTestQueryDataApp extends BenchmarkQueryDataUnary {

        private static class IntegrationTestQueryResponseSingleTask
                extends BenchmarkQueryDataUnary.QueryResponseSingleTask {

            final private IntegrationTestQueryTaskValidationHelper helper;

            public IntegrationTestQueryResponseSingleTask(Channel channel, QueryDataRequestTaskParams params) {
                super(channel, params);
                helper = new IntegrationTestQueryTaskValidationHelper(params);
            }

            @Override
            protected void onRequest(QueryDataRequest request) {
                helper.onRequest(request);
            }

            @Override
            protected void onResponse(QueryDataResponse response) {
                helper.onResponse(response);
            }

            @Override
            protected void onCompleted() {
                helper.onCompleted();
            }
        }

        @Override
        protected IntegrationTestQueryResponseSingleTask newQueryTask(
                Channel channel, QueryDataRequestTaskParams params
        ) {
            return new IntegrationTestQueryResponseSingleTask(channel, params);
        }
    }

    protected static class IntegrationTestQueryGrpcClient {

        // instance variables
        final private Channel channel;

        public IntegrationTestQueryGrpcClient(Channel channel) {
            this.channel = channel;
        }

        private void runQueryDataBidiStreamScenario() {

            System.out.println();
            System.out.println("========== running queryResponseCursor scenario ==========");
            System.out.println("number of PVs: " + QUERY_NUM_PVS);
            System.out.println("number of PVs per request: " + QUERY_NUM_PVS_PER_REQUEST);
            System.out.println("number of threads: " + QUERY_NUM_THREADS);

            final long startSeconds = configMgr().getConfigLong(
                    IngestionBenchmarkBase.CFG_KEY_START_SECONDS,
                    IngestionBenchmarkBase.DEFAULT_START_SECONDS);

            IntegrationTestQueryDataBidiStreamApp queryDataBidiStreamApp =
                    new IntegrationTestQueryDataBidiStreamApp();
            BenchmarkScenarioResult scenarioResult = queryDataBidiStreamApp.queryScenario(
                    channel,
                    QUERY_NUM_PVS,
                    QUERY_NUM_PVS_PER_REQUEST,
                    QUERY_NUM_THREADS,
                    startSeconds,
                    NUM_SCENARIO_SECONDS, false);
            assertTrue(scenarioResult.success);

            System.out.println("========== queryResponseCursor scenario completed ==========");
            System.out.println();
        }

        private void runQueryDataStreamScenario() {

            System.out.println();
            System.out.println("========== running queryResponseStream scenario ==========");
            System.out.println("number of PVs: " + QUERY_NUM_PVS);
            System.out.println("number of PVs per request: " + QUERY_NUM_PVS_PER_REQUEST);
            System.out.println("number of threads: " + QUERY_NUM_THREADS);

            final long startSeconds = configMgr().getConfigLong(
                    IngestionBenchmarkBase.CFG_KEY_START_SECONDS,
                    IngestionBenchmarkBase.DEFAULT_START_SECONDS);

            IntegrationTestQueryDataStreamApp dataStreamApp =
                    new IntegrationTestQueryDataStreamApp();
            BenchmarkScenarioResult scenarioResult = dataStreamApp.queryScenario(
                    channel,
                    QUERY_NUM_PVS,
                    QUERY_NUM_PVS_PER_REQUEST,
                    QUERY_NUM_THREADS,
                    startSeconds,
                    NUM_SCENARIO_SECONDS, false);
            assertTrue(scenarioResult.success);

            System.out.println("========== queryResponseStream scenario completed ==========");
            System.out.println();
        }

        private void runQueryDataScenario() {

            System.out.println();
            System.out.println("========== running queryResponseSingle scenario ==========");
            System.out.println("number of PVs: " + QUERY_SINGLE_NUM_PVS);
            System.out.println("number of PVs per request: " + QUERY_SINGLE_NUM_PVS_PER_REQUEST);
            System.out.println("number of threads: " + QUERY_NUM_THREADS);

            final long startSeconds = configMgr().getConfigLong(
                    IngestionBenchmarkBase.CFG_KEY_START_SECONDS,
                    IngestionBenchmarkBase.DEFAULT_START_SECONDS);

            IntegrationTestQueryDataApp queryDataApp =
                    new IntegrationTestQueryDataApp();
            BenchmarkScenarioResult scenarioResult = queryDataApp.queryScenario(
                    channel,
                    QUERY_SINGLE_NUM_PVS,
                    QUERY_SINGLE_NUM_PVS_PER_REQUEST,
                    QUERY_NUM_THREADS,
                    startSeconds,
                    NUM_SCENARIO_SECONDS, false);
            assertTrue(scenarioResult.success);

            System.out.println("========== queryResponseSingle scenario completed ==========");
            System.out.println();
        }

    }

    @Before
    public void setUp() throws Exception {

        super.setUp();

        // Create a grpcClient using the in-process channel;
        ingestionGrpcClient = new IntegrationTestIngestionGrpcClient(ingestionServiceWrapper.getIngestionChannel());

        // Create a grpcClient using the in-process channel;
        queryGrpcClient = new IntegrationTestQueryGrpcClient(queryServiceWrapper.getQueryChannel());
    }

    @After
    public void tearDown() {
        super.tearDown();
        ingestionGrpcClient = null;
        queryGrpcClient = null;
    }

    /**
     * Provides test coverage for a valid ingestion request stream.
     */
    @Test
    public void runIntegrationTestScenarios() {

        // run and verify ingestion scenario
        ingestionGrpcClient.runStreamingIngestionScenario();

        // create dataset for large export test
        String datasetId = null;
        AnnotationTestBase.SaveDataSetParams datasetParams = null;
        int pvCount = 0;
        {
            // create list with single data block specifying data to be exported
            final List<AnnotationTestBase.AnnotationDataBlock> dataBlocks = new ArrayList<>();
            final long beginSeconds = 1698767462L;
            pvCount = 100;
            final List<String> pvNames = new ArrayList<>();
            for (int i = 1 ; i <= pvCount ; ++ i) {
                pvNames.add("dpTest_" + i);
            }
            final AnnotationTestBase.AnnotationDataBlock dataBlock =
                    new AnnotationTestBase.AnnotationDataBlock(
                            beginSeconds, 0L, beginSeconds+60, 999000000L, pvNames);
            dataBlocks.add(dataBlock);

            // create dataset containing datablock created above
            final String ownerId = "craigmcc";
            final String datasetName = "export dataset";
            final String datasetDescription = "large dataset export test";
            final AnnotationTestBase.AnnotationDataSet dataset =
                    new AnnotationTestBase.AnnotationDataSet(
                            null, datasetName, ownerId, datasetDescription, dataBlocks);
            datasetParams =
                    new AnnotationTestBase.SaveDataSetParams(dataset);
            datasetId =
                    annotationServiceWrapper.sendAndVerifySaveDataSet(datasetParams, false, false, "");
            System.out.println("created export dataset with id: " + datasetId);
        }

        // run large export to excel test, triggers export file size limit error
        {
            System.out.println();
            System.out.println("========== running large export to excel ==========");
            ExportDataResponse.ExportDataResult exportResult =
                    annotationServiceWrapper.sendAndVerifyExportData(
                            datasetId,
                            null, null, null, ExportDataRequest.ExportOutputFormat.EXPORT_FORMAT_XLSX,
                            60 * pvCount, // 60 buckets per pv
                            0, null, true,
                            "export file size limit");
            System.out.println("========== large export to excel completed ==========");
            System.out.println();
        }

        // run large export to csv test, triggers export file size limit error
        {
            System.out.println();
            System.out.println("========== running large export to csv ==========");
            ExportDataResponse.ExportDataResult exportResult =
                    annotationServiceWrapper.sendAndVerifyExportData(
                            datasetId,
                            null, null, null, ExportDataRequest.ExportOutputFormat.EXPORT_FORMAT_CSV,
                            60 * pvCount, // 60 buckets per pv
                            0, null, true,
                            "export file size limit");
            System.out.println("========== large export to csv completed ==========");
            System.out.println();
        }

// commenting this code for now because it creates a 172 MB file each run.  But can be uncommented to make a large hdf5 file.
//        // run large export to hdf5 test, succeeds because export file size limit only applies to tabular files
//        {
//            System.out.println();
//            System.out.println("========== running large export to hdf5 ==========");
//            ExportDataResponse.ExportDataResult exportResult =
//                    sendAndVerifyExportData(
//                            datasetId,
//                            ExportDataRequest.ExportOutputFormat.EXPORT_FORMAT_HDF5,
//                            60 * pvCount, // 60 buckets per pv
//                            false,
//                            "");
//            System.out.println("========== large export to hdf5 completed ==========");
//            System.out.println();
//        }

        // run and verify bidirectional stream query api scenario
        queryGrpcClient.runQueryDataBidiStreamScenario();

        // run and verify server-streaming query api scenario
        queryGrpcClient.runQueryDataStreamScenario();

        // run and verify single response query api scenario
        queryGrpcClient.runQueryDataScenario();

        // negative test for unary data query that hits response message size limit
        {
            // create list of 10 PV names
            final List<String> pvNames = new ArrayList<>();
            for (int i = 1 ; i <= 10 ; ++i) {
                pvNames.add("dpTest_" + i);
            }

            final long beginSeconds = configMgr().getConfigLong(
                    IngestionBenchmarkBase.CFG_KEY_START_SECONDS,
                    IngestionBenchmarkBase.DEFAULT_START_SECONDS);
            final long beginNanos = 0L;
            final long endSeconds = beginSeconds + 37;
            final long endNanos = 0L;

            final int numBucketsExpected = 370; // 37 buckets * 10 PVs

            final boolean expectReject = true;
            final String expectedRejectMessage =
                    "query returned more data than will fit in single QueryResponse message";

            final QueryTestBase.QueryDataRequestParams params =
                    new QueryTestBase.QueryDataRequestParams(
                            pvNames,
                            beginSeconds,
                            beginNanos,
                            endSeconds,
                            endNanos,
                            false
                    );

            queryServiceWrapper.sendAndVerifyQueryData(
                    numBucketsExpected,
                    0, params,
                    null,
                    expectReject,
                    expectedRejectMessage
            );
        }
    }

}
