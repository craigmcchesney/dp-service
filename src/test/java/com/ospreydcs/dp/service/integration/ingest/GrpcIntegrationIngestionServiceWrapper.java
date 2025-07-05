package com.ospreydcs.dp.service.integration.ingest;

import com.google.protobuf.InvalidProtocolBufferException;
import com.ospreydcs.dp.grpc.v1.common.*;
import com.ospreydcs.dp.grpc.v1.ingestion.*;
import com.ospreydcs.dp.service.common.bson.ProviderDocument;
import com.ospreydcs.dp.service.common.bson.RequestStatusDocument;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.config.ConfigurationManager;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.model.TimestampMap;
import com.ospreydcs.dp.service.common.mongo.MongoTestClient;
import com.ospreydcs.dp.service.common.protobuf.DataTimestampsUtility;
import com.ospreydcs.dp.service.ingest.IngestionTestBase;
import com.ospreydcs.dp.service.ingest.handler.interfaces.IngestionHandlerInterface;
import com.ospreydcs.dp.service.ingest.handler.mongo.MongoIngestionHandler;
import com.ospreydcs.dp.service.ingest.service.IngestionServiceImpl;
import com.ospreydcs.dp.service.ingest.utility.RegisterProviderUtility;
import com.ospreydcs.dp.service.ingest.utility.SubscribeDataUtility;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import org.junit.ClassRule;

import java.io.IOException;
import java.time.Instant;
import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.mock;

public class GrpcIntegrationIngestionServiceWrapper {

    // static variables
    @ClassRule
    public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    // constants
    protected static final int INGESTION_PROVIDER_ID = 1;
    public static final String GCC_INGESTION_PROVIDER = "GCC Provider";
    public static final String BPM_INGESTION_PROVIDER = "BPM Provider";
    public static final String CFG_KEY_START_SECONDS = "IngestionBenchmark.startSeconds";
    public static final Long DEFAULT_START_SECONDS = 1698767462L;

    // instance variables
    protected MongoTestClient mongoClient;
    private IngestionServiceImpl ingestionService;
    private IngestionServiceImpl ingestionServiceMock;
    protected ManagedChannel ingestionChannel;

    public record IngestionProviderInfo(
            String providerId,
            Set<String> pvNameSet,
            long firstTimeSeconds,
            long firstTimeNanos,
            long lastTimeSeconds,
            long lastTimeNanos,
            int numBuckets
    ) {
    }

    /**
     * @param columnName               instance variables
     * @param useSerializedDataColumns
     */
    public record IngestionColumnInfo(
            String columnName,
            String requestIdBase,
            String providerId,
            long measurementInterval,
            int numBuckets,
            int numSecondsPerBucket,
            boolean useExplicitTimestampList,
            boolean useSerializedDataColumns, List<String> tags,
            Map<String, String> attributes,
            String eventDescription,
            Long eventStartSeconds,
            Long eventStartNanos,
            Long eventStopSeconds,
            Long eventStopNanos
    ) {
    }

    /**
     * @param providerId instance variables
     */
    public record IngestionBucketInfo(
            String providerId,
            String requestId,
            long startSeconds,
            long startNanos,
            long endSeconds,
            long endNanos,
            int numValues,
            long intervalNanos,
            List<Object> dataValues,
            List<Long> timestampSecondsList,
            List<Long> timestampNanosList,
            List<String> tags,
            Map<String, String> attributes,
            String eventDescription,
            Long eventStartSeconds,
            Long eventStartNanos,
            Long eventStopSeconds,
            Long eventStopNanos
    ) {
    }

    public static class IngestionStreamInfo {

        // instance variables
        final public TimestampMap<IngestionBucketInfo> bucketInfoMap;
        final public TimestampMap<Double> valueMap;
        final List<IngestionTestBase.IngestionRequestParams> paramsList;
        final List<IngestDataRequest> requestList;
        final List<IngestDataResponse> responseList;

        public IngestionStreamInfo(
                TimestampMap<IngestionBucketInfo> bucketInfoMap,
                TimestampMap<Double> valueMap,
                List<IngestionTestBase.IngestionRequestParams> paramsList,
                List<IngestDataRequest> requestList,
                List<IngestDataResponse> responseList
        ) {
            this.bucketInfoMap = bucketInfoMap;
            this.valueMap = valueMap;
            this.paramsList = paramsList;
            this.requestList = requestList;
            this.responseList = responseList;
        }
    }

    public record IngestionScenarioResult(
            Map<String, IngestionProviderInfo> providerInfoMap,
            Map<String, IngestionStreamInfo> validationMap
    ) {
    }

    private class QueryRequestStatusResult {
        final public List<QueryRequestStatusResponse.RequestStatusResult.RequestStatus> statusList;
        final boolean noData;
        public QueryRequestStatusResult(
                List<QueryRequestStatusResponse.RequestStatusResult.RequestStatus> statusList,
                boolean noData
        ) {
            this.statusList = statusList;
            this.noData = noData;
        }
    }

    protected static ConfigurationManager configMgr() {
        return ConfigurationManager.getInstance();
    }

    public void init(MongoTestClient mongoClient) {
        this.mongoClient = mongoClient;
        IngestionHandlerInterface ingestionHandler = MongoIngestionHandler.newMongoSyncIngestionHandler();
        ingestionService = new IngestionServiceImpl();
        if (!ingestionService.init(ingestionHandler)) {
            fail("IngestionServiceImpl.init failed");
        }
        ingestionServiceMock = mock(IngestionServiceImpl.class, delegatesTo(ingestionService));
        // Generate a unique in-process server name.
        String ingestionServerName = InProcessServerBuilder.generateName();
        // Create a server, add service, start, and register for automatic graceful shutdown.
        try {
            grpcCleanup.register(InProcessServerBuilder
                    .forName(ingestionServerName).directExecutor().addService(ingestionServiceMock).build().start());
        } catch (IOException e) {
            fail("IOException creating grpc server");
        }
        // Create a client channel and register for automatic graceful shutdown.
        ingestionChannel = grpcCleanup.register(
                InProcessChannelBuilder.forName(ingestionServerName).directExecutor().build());
    }

    public ManagedChannel getIngestionChannel() {
        return this.ingestionChannel;
    }

    public void fini() {
        ingestionService.fini();
        ingestionService = null;
        ingestionServiceMock = null;
        ingestionChannel = null;
    }
    
        protected RegisterProviderResponse sendRegsiterProvider(
            RegisterProviderRequest request
    ) {
        final DpIngestionServiceGrpc.DpIngestionServiceStub asyncStub =
                DpIngestionServiceGrpc.newStub(ingestionChannel);

        final RegisterProviderUtility.RegisterProviderResponseObserver responseObserver =
                new RegisterProviderUtility.RegisterProviderResponseObserver();

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            asyncStub.registerProvider(request, responseObserver);
        }).start();

        responseObserver.await();

        if (responseObserver.isError()) {
            fail("responseObserver error: " + responseObserver.getErrorMessage());
        }

        return responseObserver.getResponseList().get(0);
    }

    public String sendAndVerifyRegisterProvider(
            RegisterProviderUtility.RegisterProviderRequestParams params,
            boolean expectExceptionalResponse,
            ExceptionalResult.ExceptionalResultStatus expectedExceptionStatus,
            String expectedExceptionMessage,
            boolean expectedIsNew,
            String expectedProviderId
    ) {
        // build request
        final RegisterProviderRequest request = RegisterProviderUtility.buildRegisterProviderRequest(params);

        // send API request
        final RegisterProviderResponse response = sendRegsiterProvider(request);

        // verify exceptional response
        if (expectExceptionalResponse) {
            assertTrue(response.hasExceptionalResult());
            final ExceptionalResult exceptionalResult = response.getExceptionalResult();
            assertEquals(expectedExceptionStatus, exceptionalResult.getExceptionalResultStatus());
            assertTrue(exceptionalResult.getMessage().contains(expectedExceptionMessage));
            return null;
        }

        // verify registration result
        assertTrue(response.hasRegistrationResult());
        final RegisterProviderResponse.RegistrationResult registrationResult = response.getRegistrationResult();
        assertEquals(params.name, registrationResult.getProviderName());
        assertEquals(expectedIsNew, registrationResult.getIsNewProvider());
        final String providerId = registrationResult.getProviderId();

        // verify ProviderDocument from database
        final ProviderDocument providerDocument = mongoClient.findProvider(providerId);
        assertEquals(params.name, providerDocument.getName());
        if (params.description != null) {
            assertEquals(params.description, providerDocument.getDescription());
        } else {
            assertEquals("", providerDocument.getDescription());
        }
        if (params.tags != null) {
            assertEquals(params.tags, providerDocument.getTags());
        } else {
            assertTrue(providerDocument.getTags() == null);
        }
        if (params.attributes != null) {
            assertEquals(params.attributes, providerDocument.getAttributes());
        } else {
            assertTrue(providerDocument.getAttributes() == null);
        }
        assertNotNull(providerDocument.getCreatedAt());
        assertNotNull(providerDocument.getUpdatedAt());

        // return id of ProviderDocument
        return providerId;
    }

    protected String registerProvider(RegisterProviderUtility.RegisterProviderRequestParams params) {

        // send and verify register provider API request
        final boolean expectExceptionalResponse = false;
        final ExceptionalResult.ExceptionalResultStatus expectedExceptionStatus = null;
        final String expectedExceptionMessage = null;
        boolean expectedIsNew = true;
        final String expectedProviderId = null;
        final String providerId = sendAndVerifyRegisterProvider(
                params,
                expectExceptionalResponse,
                expectedExceptionStatus,
                expectedExceptionMessage,
                expectedIsNew,
                expectedProviderId);
        Objects.requireNonNull(providerId);

        return providerId;
    }

    public String registerProvider(String providerName, Map<String, String> attributeMap) {

        // create register provider params
        final RegisterProviderUtility.RegisterProviderRequestParams params
                = new RegisterProviderUtility.RegisterProviderRequestParams(providerName, attributeMap);

        return registerProvider(params);
    }

    protected IngestDataResponse sendIngestData(IngestDataRequest request) {

        final DpIngestionServiceGrpc.DpIngestionServiceStub asyncStub =
                DpIngestionServiceGrpc.newStub(ingestionChannel);

        final IngestionTestBase.IngestionResponseObserver responseObserver =
                new IngestionTestBase.IngestionResponseObserver(1);

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            asyncStub.ingestData(request, responseObserver);
        }).start();

        responseObserver.await();

        if (responseObserver.isError()) {
            return null;
        } else {
            return responseObserver.getResponseList().get(0);
        }
    }

    protected IngestDataStreamResponse sendIngestDataStream(
            List<IngestDataRequest> requestList
    ) {
        final DpIngestionServiceGrpc.DpIngestionServiceStub asyncStub =
                DpIngestionServiceGrpc.newStub(ingestionChannel);

        final IngestionTestBase.IngestDataStreamResponseObserver responseObserver =
                new IngestionTestBase.IngestDataStreamResponseObserver();

        StreamObserver<IngestDataRequest> requestObserver = asyncStub.ingestDataStream(responseObserver);

        for (IngestDataRequest request : requestList) {
            requestObserver.onNext(request); // don't create a thread to send request because it will be a race condition with call to onCompleted()
        }

        requestObserver.onCompleted();
        responseObserver.await();
        return responseObserver.getResponse();
    }

    protected List<IngestDataResponse> sendIngestDataBidiStream(List<IngestDataRequest> requestList) {

        final DpIngestionServiceGrpc.DpIngestionServiceStub asyncStub =
                DpIngestionServiceGrpc.newStub(ingestionChannel);

        final IngestionTestBase.IngestionResponseObserver responseObserver =
                new IngestionTestBase.IngestionResponseObserver(requestList.size());

        StreamObserver<IngestDataRequest> requestObserver = asyncStub.ingestDataBidiStream(responseObserver);

        for (IngestDataRequest request : requestList) {
            // send request in separate thread to better simulate out of process grpc,
            // otherwise service handles request in this thread
            new Thread(() -> {
                requestObserver.onNext(request);
            }).start();
        }

        responseObserver.await();
        requestObserver.onCompleted();

        if (responseObserver.isError()) {
            return new ArrayList<>();
        } else {
            return responseObserver.getResponseList();
        }
    }

    protected List<BucketDocument> verifyIngestionHandling(
            List<IngestionTestBase.IngestionRequestParams> paramsList,
            List<IngestDataRequest> requestList,
            IngestDataStreamResponse response,
            int numSerializedDataColumnsExpected,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        // create container to hold method result
        final List<BucketDocument> bucketDocumentList = new ArrayList<>();

        if (expectReject) {
            assertTrue(response.hasExceptionalResult());
            assertEquals(expectedRejectMessage, response.getExceptionalResult().getMessage());
            assertEquals(
                    ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT,
                    response.getExceptionalResult().getExceptionalResultStatus());
        } else {
            assertTrue(response.hasIngestDataStreamResult());
            IngestDataStreamResponse.IngestDataStreamResult result = response.getIngestDataStreamResult();
            assertEquals(paramsList.size(), requestList.size());
            assertEquals(requestList.size(), result.getNumRequests());

            // verify handling for each params / request / response
            for (int listIndex = 0; listIndex < requestList.size(); ++listIndex) {

                final IngestionTestBase.IngestionRequestParams params = paramsList.get(listIndex);
                final IngestDataRequest request = requestList.get(listIndex);

                // verify database contents (request status and corresponding bucket documents)
                bucketDocumentList.addAll(verifyIngestionHandling(params, request, numSerializedDataColumnsExpected));
            }
        }

        return bucketDocumentList;
    }

    protected List<BucketDocument> verifyIngestionHandling(
            List<IngestionTestBase.IngestionRequestParams> paramsList,
            List<IngestDataRequest> requestList,
            List<IngestDataResponse> responseList,
            int numSerializedDataColumnsExpected
    ) {
        // check that parameter list sizes match
        assertEquals(paramsList.size(), requestList.size());
        assertEquals(requestList.size(), responseList.size());

        // create container to hold method result
        final List<BucketDocument> bucketDocumentList = new ArrayList<>();

        // verify handling for each params / request / response
        for (int listIndex = 0 ; listIndex < requestList.size() ; ++listIndex) {

            final IngestionTestBase.IngestionRequestParams params = paramsList.get(listIndex);
            final IngestDataRequest request = requestList.get(listIndex);
            final IngestDataResponse response = responseList.get(listIndex);

            // verify API response
            final int numPvs = params.columnNames.size();
            assertTrue(response.hasAckResult());
            final IngestDataResponse.AckResult ackResult = response.getAckResult();
            assertEquals(numPvs, ackResult.getNumColumns());
            assertEquals((int) params.samplingClockCount, ackResult.getNumRows());

            // verify database contents (request status and corresponding bucket documents)
            bucketDocumentList.addAll(verifyIngestionHandling(params, request, numSerializedDataColumnsExpected));
        }

        return bucketDocumentList;
    }

    protected List<BucketDocument> verifyIngestionHandling(
            IngestionTestBase.IngestionRequestParams params,
            IngestDataRequest request,
            int numSerializedDataColumnsExpected
    ) {
        // create container to hold method result
        final List<BucketDocument> bucketDocumentList = new ArrayList<>();

        // validate database RequestStatusDocument
        final int numPvs = params.columnNames.size();
        final RequestStatusDocument statusDocument =
                mongoClient.findRequestStatus(params.providerId, params.requestId);
        assertNotNull(statusDocument);
        assertNotNull(statusDocument.getCreatedAt());
        assertEquals(
                IngestionRequestStatus.INGESTION_REQUEST_STATUS_SUCCESS_VALUE,
                statusDocument.getRequestStatusCase());
        assertEquals(numPvs, statusDocument.getIdsCreated().size());
        final List<String> expectedBucketIds = new ArrayList<>();
        for (String pvName : params.columnNames) {
            final String expectedBucketId =
                    pvName + "-" + params.samplingClockStartSeconds + "-" + params.samplingClockStartNanos;
            assertTrue(expectedBucketId, statusDocument.getIdsCreated().contains(expectedBucketId));
            expectedBucketIds.add(expectedBucketId);
        }

        // validate database BucketDocument for each column
        int pvIndex = 0;
        int serializedDataColumnCount = 0;
        for (String expectedBucketId : expectedBucketIds) {

            final BucketDocument bucketDocument = mongoClient.findBucket(expectedBucketId);
            bucketDocumentList.add(bucketDocument);

            assertNotNull(bucketDocument);
            final String pvName = params.columnNames.get(pvIndex);
            assertEquals(pvName, bucketDocument.getPvName());
            assertEquals(expectedBucketId, bucketDocument.getId());

            // check createdAt time
            assertNotNull(bucketDocument.getCreatedAt());

            // check bucket start times
            assertEquals(
                    (long) params.samplingClockStartSeconds,
                    bucketDocument.getDataTimestamps().getFirstTime().getSeconds());
            assertEquals(
                    (long) params.samplingClockStartNanos,
                    bucketDocument.getDataTimestamps().getFirstTime().getNanos());
            assertEquals(
                    Date.from(Instant.ofEpochSecond(
                            params.samplingClockStartSeconds, params.samplingClockStartNanos)),
                    bucketDocument.getDataTimestamps().getFirstTime().getDateTime());

            // check sample count params
            assertEquals(
                    (int) params.samplingClockCount,
                    bucketDocument.getDataTimestamps().getSampleCount());
            DataColumn bucketDataColumn = null;
            try {
                bucketDataColumn = bucketDocument.getDataColumn().toDataColumn();
            } catch (DpException e) {
                throw new RuntimeException(e);
            }
            Objects.requireNonNull(bucketDataColumn);
            assertEquals(
                    (int) params.samplingClockCount,
                    bucketDataColumn.getDataValuesList().size());

            // check DataTimestamps (TimestampsList or SamplingClock depending on request)
            DataTimestamps bucketDataTimestamps = null;
            try {
                bucketDataTimestamps = bucketDocument.getDataTimestamps().toDataTimestamps();
            } catch (DpException e) {
                fail("exception deserializing DataTimestampsDocument.bytes: " + e.getMessage());
            }
            Objects.requireNonNull(bucketDataTimestamps);
            DataTimestampsUtility.DataTimestampsModel requestDataTimestampsModel =
                    new DataTimestampsUtility.DataTimestampsModel(
                            request.getIngestionDataFrame().getDataTimestamps());
            final long endSeconds = requestDataTimestampsModel.getLastTimestamp().getEpochSeconds();
            final long endNanos = requestDataTimestampsModel.getLastTimestamp().getNanoseconds();
            assertEquals(
                    requestDataTimestampsModel.getSamplePeriodNanos(),
                    bucketDocument.getDataTimestamps().getSamplePeriod());

            if (params.timestampsSecondsList != null && !params.timestampsSecondsList.isEmpty()) {
                // check explicit TimestampsList
                assertEquals(
                        DataTimestamps.ValueCase.TIMESTAMPLIST.getNumber(),
                        bucketDocument.getDataTimestamps().getValueCase());
                assertEquals(
                        DataTimestamps.ValueCase.TIMESTAMPLIST.name(),
                        bucketDocument.getDataTimestamps().getValueType());

                // compare list of timestamps in bucket vs. params
                assertTrue(bucketDataTimestamps.hasTimestampList());
                final List<Timestamp> bucketTimestampList =
                        bucketDataTimestamps.getTimestampList().getTimestampsList();
                assertEquals(params.timestampsSecondsList.size(), bucketTimestampList.size());
                assertEquals(params.timestampNanosList.size(), bucketTimestampList.size());
                for (int timestampIndex = 0; timestampIndex < bucketTimestampList.size(); ++timestampIndex) {
                    final Timestamp bucketTimestamp = bucketTimestampList.get(timestampIndex);
                    final long requestSeconds = params.timestampsSecondsList.get(timestampIndex);
                    final long requestNanos = params.timestampNanosList.get(timestampIndex);
                    assertEquals(requestSeconds, bucketTimestamp.getEpochSeconds());
                    assertEquals(requestNanos, bucketTimestamp.getNanoseconds());
                }

            } else {
                // check SamplingClock parameters
                assertEquals(
                        DataTimestamps.ValueCase.SAMPLINGCLOCK.getNumber(),
                        bucketDocument.getDataTimestamps().getValueCase());
                assertEquals(
                        DataTimestamps.ValueCase.SAMPLINGCLOCK.name(),
                        bucketDocument.getDataTimestamps().getValueType());

            }

            // check bucket end times against expected values determined above
            assertEquals(endSeconds, bucketDocument.getDataTimestamps().getLastTime().getSeconds());
            assertEquals(endNanos, bucketDocument.getDataTimestamps().getLastTime().getNanos());
            assertEquals(
                    Date.from(Instant.ofEpochSecond(endSeconds, endNanos)),
                    bucketDocument.getDataTimestamps().getLastTime().getDateTime());

            // compare data value vectors
            DataColumn requestDataColumn = null;

            if (params.useSerializedDataColumns) {
                // request contains SerializedDataColumns
                final List<SerializedDataColumn> serializedDataColumnList =
                        request.getIngestionDataFrame().getSerializedDataColumnsList();
                final SerializedDataColumn serializedDataColumn = serializedDataColumnList.get(pvIndex);
                // deserialize column for comparison
                try {
                    requestDataColumn = DataColumn.parseFrom(serializedDataColumn.getDataColumnBytes());
                } catch (InvalidProtocolBufferException e) {
                    fail("exception deserializing DataColumn: " + e.getMessage());
                }
                serializedDataColumnCount = serializedDataColumnCount + 1;

            } else {
                // request contains regular DataColumns
                final List<DataColumn> dataColumnList = request.getIngestionDataFrame().getDataColumnsList();
                requestDataColumn = dataColumnList.get(pvIndex);
            }
            // this compares each DataValue including ValueStatus, confirmed in debugger
            assertEquals(requestDataColumn, bucketDataColumn);

            // check tags
            if (params.tags != null) {
                assertEquals(params.tags, bucketDocument.getTags());
            } else {
                assertTrue(bucketDocument.getTags() == null || bucketDocument.getTags().isEmpty());
            }

            // check attributes
            if (params.attributes != null) {
                assertEquals(params.attributes, bucketDocument.getAttributes());
            } else {
                assertTrue(bucketDocument.getAttributes() == null || bucketDocument.getAttributes().isEmpty());
            }

            // check event metadata
            if (params.eventDescription != null) {
                assertNotNull(bucketDocument.getEvent());
                assertEquals(request.getEventMetadata(), bucketDocument.getEvent().toEventMetadata());
            } else {
                assertNull(bucketDocument.getEvent());
            }

            pvIndex = pvIndex + 1;
        }
        assertEquals(numSerializedDataColumnsExpected, serializedDataColumnCount);

        return bucketDocumentList;
    }

    protected List<BucketDocument> sendAndVerifyIngestData(
            IngestionTestBase.IngestionRequestParams params,
            IngestDataRequest ingestionRequest,
            int numSerializedDataColumnsExpected
    ) {
        final IngestDataResponse response = sendIngestData(ingestionRequest);
        final List<IngestionTestBase.IngestionRequestParams> paramsList = Arrays.asList(params);
        final List<IngestDataRequest> requestList = Arrays.asList(ingestionRequest);
        final List<IngestDataResponse> responseList = Arrays.asList(response);
        return verifyIngestionHandling(paramsList, requestList, responseList, numSerializedDataColumnsExpected);
    }

    public List<BucketDocument> sendAndVerifyIngestDataStream(
            List<IngestionTestBase.IngestionRequestParams> paramsList,
            List<IngestDataRequest> requestList,
            int numSerializedDataColumnsExpected,
            boolean expectReject,
            String expectedRejectMessage
    ) {

        // send request
        final IngestDataStreamResponse response = sendIngestDataStream(requestList);
        return verifyIngestionHandling(
                paramsList,
                requestList,
                response,
                numSerializedDataColumnsExpected,
                expectReject,
                expectedRejectMessage);
    }

    protected List<BucketDocument> sendAndVerifyIngestDataBidiStream(
            IngestionTestBase.IngestionRequestParams params,
            IngestDataRequest ingestionRequest,
            int numSerializedDataColumnsExpected
    ) {

        // send request
        final List<IngestionTestBase.IngestionRequestParams> paramsList = Arrays.asList(params);
        final List<IngestDataRequest> requestList = Arrays.asList(ingestionRequest);
        final List<IngestDataResponse> responseList = sendIngestDataBidiStream(requestList);
        return verifyIngestionHandling(paramsList, requestList, responseList, numSerializedDataColumnsExpected);
    }

    protected IngestionStreamInfo ingestDataBidiStream(
            long startSeconds,
            long startNanos,
            IngestionColumnInfo columnInfo
    ) {
        final String requestIdBase = columnInfo.requestIdBase;
        long measurementInterval = columnInfo.measurementInterval;
        final String columnName = columnInfo.columnName;
        final int numBuckets = columnInfo.numBuckets;
        final int numSecondsPerBucket = columnInfo.numSecondsPerBucket;

        final int numSamplesPerSecond = ((int) (1_000_000_000 / measurementInterval));
        final int numSamplesPerBucket = numSamplesPerSecond * numSecondsPerBucket;

        // create data structures for later validation
        final TimestampMap<Double> valueMap = new TimestampMap<>();
        final TimestampMap<IngestionBucketInfo> bucketInfoMap = new TimestampMap<>();

        // create requests
        final List<IngestionTestBase.IngestionRequestParams> paramsList = new ArrayList<>();
        final List<IngestDataRequest> requestList = new ArrayList<>();
        long currentSeconds = startSeconds;
        int secondsCount = 0;
        for (int bucketIndex = 0; bucketIndex < numBuckets; ++bucketIndex) {

            final String requestId = requestIdBase + bucketIndex;

            // create list of column data values for request
            final List<List<Object>> columnValues = new ArrayList<>();
            final List<Object> dataValuesList = new ArrayList<>();
            List<Long> timestampSecondsList = null;
            List<Long> timestampNanosList = null;
            if (columnInfo.useExplicitTimestampList) {
                timestampSecondsList = new ArrayList<>();
                timestampNanosList = new ArrayList<>();
            }
            for (int secondIndex = 0; secondIndex < numSecondsPerBucket; ++secondIndex) {
                long currentNanos = 0;

                for (int sampleIndex = 0; sampleIndex < numSamplesPerSecond; ++sampleIndex) {
                    final double dataValue =
                            secondsCount + (double) sampleIndex / numSamplesPerSecond;
                    dataValuesList.add(dataValue);
                    valueMap.put(currentSeconds + secondIndex, currentNanos, dataValue);
                    if (columnInfo.useExplicitTimestampList) {
                        timestampSecondsList.add(currentSeconds + secondIndex);
                        timestampNanosList.add(currentNanos);
                    }
                    currentNanos = currentNanos + measurementInterval;
                }

                secondsCount = secondsCount + 1;
            }
            columnValues.add(dataValuesList);

            // create request parameters
            final IngestionTestBase.IngestionRequestParams params =
                    new IngestionTestBase.IngestionRequestParams(
                            columnInfo.providerId,
                            requestId,
                            null,
                            null,
                            timestampSecondsList, // if not null, request will use explicit TimestampsList in DataTimestamps
                            timestampNanosList,
                            currentSeconds,
                            startNanos,
                            measurementInterval,
                            numSamplesPerBucket,
                            List.of(columnName),
                            IngestionTestBase.IngestionDataType.DOUBLE,
                            columnValues,
                            columnInfo.tags,
                            columnInfo.attributes,
                            columnInfo.eventDescription,
                            columnInfo.eventStartSeconds,
                            columnInfo.eventStartNanos,
                            columnInfo.eventStopSeconds,
                            columnInfo.eventStopNanos,
                            columnInfo.useSerializedDataColumns
                    );
            paramsList.add(params);

            final Instant startTimeInstant = Instant.ofEpochSecond(currentSeconds, startNanos);
            final Instant endTimeInstant =
                    startTimeInstant.plusNanos(measurementInterval * (numSamplesPerBucket - 1));

            // capture data for later validation
            final long bucketInfoSamplePeriod = (columnInfo.useExplicitTimestampList) ? 0 : measurementInterval;
            final IngestionBucketInfo bucketInfo =
                    new IngestionBucketInfo(
                            columnInfo.providerId,
                            requestId,
                            currentSeconds,
                            startNanos,
                            endTimeInstant.getEpochSecond(),
                            endTimeInstant.getNano(),
                            numSamplesPerBucket,
                            bucketInfoSamplePeriod,
                            dataValuesList,
                            timestampSecondsList,
                            timestampNanosList,
                            columnInfo.tags,
                            columnInfo.attributes,
                            columnInfo.eventDescription,
                            columnInfo.eventStartSeconds,
                            columnInfo.eventStartNanos,
                            columnInfo.eventStopSeconds,
                            columnInfo.eventStopNanos
                    );
            bucketInfoMap.put(currentSeconds, startNanos, bucketInfo);

            // build request
            final IngestDataRequest request = IngestionTestBase.buildIngestionRequest(params);
            requestList.add(request);

            currentSeconds = currentSeconds + numSecondsPerBucket;
        }

        // send requests
        final List<IngestDataResponse> responseList = sendIngestDataBidiStream(requestList);
        assertEquals(requestList.size(), responseList.size());
        for (IngestDataResponse response : responseList) {
            assertTrue(response.hasAckResult());
            final IngestDataResponse.AckResult ackResult = response.getAckResult();
            assertEquals(1, ackResult.getNumColumns());
            assertEquals(numSamplesPerBucket, ackResult.getNumRows());
        }

        return new IngestionStreamInfo(bucketInfoMap, valueMap, paramsList, requestList, responseList);
    }

    public Map<String, IngestionStreamInfo> ingestDataBidiStreamFromColumn(
            List<IngestionColumnInfo> columnInfoList,
            long startSeconds,
            long startNanos,
            int numSerializedDataColumnsExpected
    ) {
        // create data structure for validating query result
        Map<String, IngestionStreamInfo> validationMap = new TreeMap<>();

        for (IngestionColumnInfo columnInfo : columnInfoList) {
            final IngestionStreamInfo streamInfo =
                    ingestDataBidiStream(
                            startSeconds,
                            startNanos,
                            columnInfo);
            verifyIngestionHandling(
                    streamInfo.paramsList,
                    streamInfo.requestList,
                    streamInfo.responseList,
                    numSerializedDataColumnsExpected);
            validationMap.put(columnInfo.columnName, streamInfo);
        }

        return validationMap;
    }

    public IngestionScenarioResult simpleIngestionScenario(Long scenarioStartSeconds) {

        long startSeconds;
        if (scenarioStartSeconds == null) {
            startSeconds = configMgr().getConfigLong(CFG_KEY_START_SECONDS, DEFAULT_START_SECONDS);
        } else {
            startSeconds = scenarioStartSeconds;
        }
        final long startNanos = 0L;

        // register providers used by scenario
        final String gccProviderName = GCC_INGESTION_PROVIDER;
        final String gccProviderId = registerProvider(gccProviderName, null);
        final String bpmProviderName = BPM_INGESTION_PROVIDER;
        final String bpmProviderId = registerProvider(bpmProviderName, null);

        List<IngestionColumnInfo> ingestionColumnInfoList = new ArrayList<>();

        // create tags, attributes, and events for use in events
        final List<String> tags = List.of("gauges", "pumps");
        final Map<String, String> attributes = Map.of("sector", "01", "subsystem", "vacuum");
        final String eventDescription = "Vacuum pump maintenance";
        final long eventStartSeconds = startSeconds;
        final long eventStartNanos = startNanos;
        final long eventStopSeconds = startSeconds + 1;
        final long eventStopNanos = 0L;

        // create data for 10 sectors, each containing 3 gauges and 3 bpms with names like S01-GCC01 and S01-BPM01
        final Set<String> gccPvNames = new TreeSet<>();
        final Set<String> bpmPvNames = new TreeSet<>();
        for (int sectorIndex = 1 ; sectorIndex <= 10 ; ++sectorIndex) {
            final String sectorName = String.format("S%02d", sectorIndex);

            // create columns for 3 gccs in each sector
            for (int gccIndex = 1 ; gccIndex <= 3 ; ++ gccIndex) {
                final String gccName = sectorName + "-" + String.format("GCC%02d", gccIndex);
                final String requestIdBase = gccName + "-";
                final long interval = 100_000_000L;
                final int numBuckets = 10;
                final int numSecondsPerBucket = 1;
                final IngestionColumnInfo columnInfoTenths =
                        new IngestionColumnInfo(
                                gccName,
                                requestIdBase,
                                gccProviderId,
                                interval,
                                numBuckets,
                                numSecondsPerBucket,
                                false,
                                false, tags,
                                attributes,
                                eventDescription,
                                eventStartSeconds,
                                eventStartNanos,
                                eventStopSeconds,
                                eventStopNanos);
                gccPvNames.add(gccName);
                ingestionColumnInfoList.add(columnInfoTenths);
            }

            // create columns for 3 bpms in each sector
            for (int bpmIndex = 1 ; bpmIndex <= 3 ; ++ bpmIndex) {
                final String bpmName = sectorName + "-" + String.format("BPM%02d", bpmIndex);
                final String requestIdBase = bpmName + "-";
                final long interval = 100_000_000L;
                final int numBuckets = 10;
                final int numSecondsPerBucket = 1;
                final IngestionColumnInfo columnInfoTenths =
                        new IngestionColumnInfo(
                                bpmName,
                                requestIdBase,
                                bpmProviderId,
                                interval,
                                numBuckets,
                                numSecondsPerBucket,
                                false, false, null, null, null, null, null, null, null);
                bpmPvNames.add(bpmName);
                ingestionColumnInfoList.add(columnInfoTenths);
            }
        }
        
        // build map of provider info
        final Map<String, IngestionProviderInfo> providerInfoMap = new HashMap<>();
        final IngestionProviderInfo gccProviderInfo = new IngestionProviderInfo(
                gccProviderId,
                gccPvNames,
                startSeconds,
                startNanos,
                startSeconds + 10 - 1,
                0L,
                3 * 10 * 10);
        providerInfoMap.put(gccProviderName, gccProviderInfo);
        final IngestionProviderInfo bpmProviderInfo = new IngestionProviderInfo(
                bpmProviderId,
                bpmPvNames,
                startSeconds,
                startNanos,
                startSeconds + 10 - 1,
                0L,
                3 * 10 * 10);
        providerInfoMap.put(bpmProviderName, bpmProviderInfo);

        Map<String, IngestionStreamInfo> validationMap = null;
        {
            // perform ingestion for specified list of columns
            validationMap = ingestDataBidiStreamFromColumn(ingestionColumnInfoList, startSeconds, startNanos, 0);
        }

        return new IngestionScenarioResult(providerInfoMap, validationMap);
    }

        private QueryRequestStatusResult sendQueryRequestStatus(
            QueryRequestStatusRequest request,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final DpIngestionServiceGrpc.DpIngestionServiceStub asyncStub
                = DpIngestionServiceGrpc.newStub(ingestionChannel);

        final IngestionTestBase.QueryRequestStatusResponseObserver responseObserver =
                new IngestionTestBase.QueryRequestStatusResponseObserver();

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            asyncStub.queryRequestStatus(request, responseObserver);
        }).start();

        responseObserver.await();

        if (expectReject) {
            assertTrue(responseObserver.isError());
            assertTrue(responseObserver.getErrorMessage().contains(expectedRejectMessage));
        } else {
            assertFalse(responseObserver.getErrorMessage(), responseObserver.isError());
        }

        return new QueryRequestStatusResult(responseObserver.getRequestStatusList(), false);
    }

    protected void sendAndVerifyQueryRequestStatus(
            IngestionTestBase.QueryRequestStatusParams params,
            IngestionTestBase.QueryRequestStatusExpectedResponseMap expectedResponseMap,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final QueryRequestStatusRequest request = IngestionTestBase.buildQueryRequestStatusRequest(params);
        QueryRequestStatusResult result = sendQueryRequestStatus(request, expectReject, expectedRejectMessage);
        final List<QueryRequestStatusResponse.RequestStatusResult.RequestStatus> requestStatusList = result.statusList;

        // verify API response against expectedResponseMap
        assertEquals(expectedResponseMap.size(), requestStatusList.size());
        for (QueryRequestStatusResponse.RequestStatusResult.RequestStatus responseStatus : requestStatusList) {
            IngestionTestBase.QueryRequestStatusExpectedResponse expectedResponseStatus =
                    expectedResponseMap.get(responseStatus.getProviderId(), responseStatus.getRequestId());
            assertEquals(expectedResponseStatus.providerId, responseStatus.getProviderId());
            assertEquals(expectedResponseStatus.providerName, responseStatus.getProviderName());
            assertEquals(expectedResponseStatus.requestId, responseStatus.getRequestId());
            assertEquals(expectedResponseStatus.status, responseStatus.getIngestionRequestStatus());
//            assertEquals(responseStatus.getStatusMessage(), expectedResponseStatus.statusMessage);
            assertEquals(expectedResponseStatus.idsCreated, responseStatus.getIdsCreatedList());
        }
    }

    private SubscribeDataUtility.SubscribeDataCall sendSubscribeData(
            SubscribeDataRequest request,
            int expectedResponseCount,
            boolean expectReject,
            String expectedRejectMessage
    ) {

        final DpIngestionServiceGrpc.DpIngestionServiceStub asyncStub =
                DpIngestionServiceGrpc.newStub(ingestionChannel);

        final IngestionTestBase.SubscribeDataResponseObserver responseObserver =
                new IngestionTestBase.SubscribeDataResponseObserver(expectedResponseCount);

        // invoke subscribeData() API method, get handle to request stream
        StreamObserver<SubscribeDataRequest> requestObserver = asyncStub.subscribeData(responseObserver);

        // send NewSubscription message in request stream
        new Thread(() -> {
            requestObserver.onNext(request);
        }).start();

        // wait for ack response
        responseObserver.awaitAckLatch();

        if (expectReject) {
            assertTrue(responseObserver.isError());
            assertTrue(responseObserver.getErrorMessage().contains(expectedRejectMessage));
        } else {
            assertFalse(responseObserver.getErrorMessage(), responseObserver.isError());
        }

        return new SubscribeDataUtility.SubscribeDataCall(requestObserver, responseObserver);
    }

    protected SubscribeDataUtility.SubscribeDataCall initiateSubscribeDataRequest(
            SubscribeDataRequest request,
            int expectedResponseCount,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        return sendSubscribeData(request, expectedResponseCount, expectReject, expectedRejectMessage);
    }

    protected SubscribeDataUtility.SubscribeDataCall initiateSubscribeDataRequest(
            List<String> pvNameList,
            int expectedResponseCount,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final SubscribeDataRequest request = SubscribeDataUtility.buildSubscribeDataRequest(pvNameList);
        return sendSubscribeData(request, expectedResponseCount, expectReject, expectedRejectMessage);
    }

    record SubscriptionResponseColumn(
            DataColumn dataColumn,
            boolean isSerialized
    ) {
    }

    private void addPvTimestampColumnMapEntry(
            Map<String, TimestampMap<SubscriptionResponseColumn>> pvTimestampColumnMap,
            long responseSeconds,
            long responseNanos,
            DataColumn dataColumn,
            boolean isSerialized
    ) {
        final String pvName = dataColumn.getName();
        TimestampMap<SubscriptionResponseColumn> pvTimestampMap = pvTimestampColumnMap.get(pvName);
        if (pvTimestampMap == null) {
            pvTimestampMap = new TimestampMap<>();
            pvTimestampColumnMap.put(pvName, pvTimestampMap);
        }
        pvTimestampMap.put(responseSeconds, responseNanos, new SubscriptionResponseColumn(dataColumn, isSerialized));
    }

    protected void verifySubscribeDataResponse(
            IngestionTestBase.SubscribeDataResponseObserver responseObserver,
            List<String> pvNameList,
            Map<String, IngestionStreamInfo> ingestionValidationMap,
            int numExpectedSerializedColumns
    ) {
        // wait for completion of API method response stream and confirm not in error state
        responseObserver.awaitResponseLatch();
        assertFalse(responseObserver.isError());

        // get subscription responses for verification of expected contents
        final List<SubscribeDataResponse> responseList = responseObserver.getResponseList();

        // create map of response DataColumns organized by PV name and timestamp
        Map<String, TimestampMap<SubscriptionResponseColumn>> pvTimestampColumnMap = new HashMap<>();
        int responseColumnCount = 0;
        for (SubscribeDataResponse response : responseList) {

            assertTrue(response.hasSubscribeDataResult());
            final SubscribeDataResponse.SubscribeDataResult responseResult = response.getSubscribeDataResult();
            final DataTimestamps responseDataTimestamps = responseResult.getDataTimestamps();
            final DataTimestampsUtility.DataTimestampsModel responseTimestampsModel = 
                    new DataTimestampsUtility.DataTimestampsModel(responseDataTimestamps);
            final long responseSeconds = responseTimestampsModel.getFirstTimestamp().getEpochSeconds();
            final long responseNanos = responseTimestampsModel.getFirstTimestamp().getNanoseconds();

            // add entries to pvTimestampColumnMap for regular DataColumns in response
            for (DataColumn dataColumn : responseResult.getDataColumnsList()) {
                addPvTimestampColumnMapEntry(
                        pvTimestampColumnMap, responseSeconds, responseNanos, dataColumn, false);
                responseColumnCount = responseColumnCount + 1;
            }

            // add entries to pvTimestampColumnMap for SerializedDataColumns in response
            for (SerializedDataColumn serializedDataColumn : responseResult.getSerializedDataColumnsList()) {
                DataColumn deserializedDataColumn = null;
                try {
                    deserializedDataColumn = DataColumn.parseFrom(serializedDataColumn.getDataColumnBytes());
                } catch (InvalidProtocolBufferException e) {
                    fail("exception deserializing response SerializedDataColumn: " + e.getMessage());
                }
                assertNotNull(deserializedDataColumn);
                addPvTimestampColumnMapEntry(
                        pvTimestampColumnMap, responseSeconds, responseNanos, deserializedDataColumn, true);
                responseColumnCount = responseColumnCount + 1;
            }

        }

        // confirm that we received the expected DataColumns in subscription responses
        // by comparing to ingestion requests for subscribed PVs
        int requestColumnCount = 0;
        int serializedColumnCount = 0;
        for (String pvName : pvNameList) {
            for (IngestDataRequest pvIngestionRequest : ingestionValidationMap.get(pvName).requestList) {

                // check that pvTimestampColumnMap contains an entry for each PV column in request's data frame
                final IngestDataRequest.IngestionDataFrame requestFrame = pvIngestionRequest.getIngestionDataFrame();
                final DataTimestamps requestDataTimestamps = requestFrame.getDataTimestamps();
                final DataTimestampsUtility.DataTimestampsModel requestTimestampsModel = 
                        new DataTimestampsUtility.DataTimestampsModel(requestDataTimestamps);
                final long requestSeconds = requestTimestampsModel.getFirstTimestamp().getEpochSeconds();
                final long requestNanos = requestTimestampsModel.getFirstTimestamp().getNanoseconds();
                final TimestampMap<SubscriptionResponseColumn> responsePvTimestampMap = pvTimestampColumnMap.get(pvName);
                assertNotNull(responsePvTimestampMap);
                final SubscriptionResponseColumn responsePvTimestampColumn =
                        responsePvTimestampMap.get(requestSeconds, requestNanos);
                assertNotNull(responsePvTimestampColumn);
                final DataColumn responseDataColumn = responsePvTimestampColumn.dataColumn;
                final boolean responseColumnIsSerialized = responsePvTimestampColumn.isSerialized();

                // check response received for each regular DataColumns for subscribed PV in request
                for (DataColumn requestDataColumn : requestFrame.getDataColumnsList()) {
                    // ignore ingestion request columns not for this PV, which shouldn't be the case, but...
                    if ( ! requestDataColumn.getName().equals(pvName)) {
                        continue;
                    }
                    assertEquals(requestDataColumn, responseDataColumn);
                    assertFalse(responseColumnIsSerialized);
                    requestColumnCount = requestColumnCount + 1;
                }

                for (SerializedDataColumn requestSerializedColumn : requestFrame.getSerializedDataColumnsList()) {
                    // ignore ingestion request columns not for this PV, which shouldn't be the case, but...
                    if ( ! requestSerializedColumn.getName().equals(pvName)) {
                        continue;
                    }
                    try {
                        assertEquals(DataColumn.parseFrom(requestSerializedColumn.getDataColumnBytes()), responseDataColumn);
                    } catch (InvalidProtocolBufferException e) {
                        fail("exception deserializing request SerializedDatacolumn: " + e.getMessage());
                    }
                    assertTrue(responseColumnIsSerialized);
                    requestColumnCount = requestColumnCount + 1;
                    serializedColumnCount = serializedColumnCount + 1;
                }

            }
        }
        assertEquals(requestColumnCount, responseColumnCount);
        assertEquals(numExpectedSerializedColumns, serializedColumnCount);
    }

    protected void cancelSubscribeDataCall(SubscribeDataUtility.SubscribeDataCall subscribeDataCall) {

        final SubscribeDataRequest request = SubscribeDataUtility.buildSubscribeDataCancelRequest();

        // send NewSubscription message in request stream
        new Thread(() -> {
            subscribeDataCall.requestObserver().onNext(request);
        }).start();

        // wait for ack response stream to close
        final IngestionTestBase.SubscribeDataResponseObserver responseObserver =
                (IngestionTestBase.SubscribeDataResponseObserver) subscribeDataCall.responseObserver();
        responseObserver.awaitCloseLatch();

    }

    protected void closeSubscribeDataCall(SubscribeDataUtility.SubscribeDataCall subscribeDataCall) {

        // close the request stream
        new Thread(subscribeDataCall.requestObserver()::onCompleted).start();

        // wait for ack response stream to close
        final IngestionTestBase.SubscribeDataResponseObserver responseObserver =
                (IngestionTestBase.SubscribeDataResponseObserver) subscribeDataCall.responseObserver();
        responseObserver.awaitCloseLatch();
    }

}
