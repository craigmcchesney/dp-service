package com.ospreydcs.dp.service.integration.ingestionstream;

import com.google.protobuf.InvalidProtocolBufferException;
import com.ospreydcs.dp.grpc.v1.common.DataBucket;
import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataTimestamps;
import com.ospreydcs.dp.grpc.v1.ingestionstream.DpIngestionStreamServiceGrpc;
import com.ospreydcs.dp.grpc.v1.ingestionstream.PvConditionTrigger;
import com.ospreydcs.dp.grpc.v1.ingestionstream.SubscribeDataEventRequest;
import com.ospreydcs.dp.grpc.v1.ingestionstream.SubscribeDataEventResponse;
import com.ospreydcs.dp.service.common.mongo.MongoTestClient;
import com.ospreydcs.dp.service.common.protobuf.DataTimestampsUtility;
import com.ospreydcs.dp.service.common.protobuf.TimestampUtility;
import com.ospreydcs.dp.service.ingestionstream.IngestionStreamTestBase;
import com.ospreydcs.dp.service.ingestionstream.handler.IngestionStreamHandler;
import com.ospreydcs.dp.service.ingestionstream.handler.interfaces.IngestionStreamHandlerInterface;
import com.ospreydcs.dp.service.ingestionstream.service.IngestionStreamServiceImpl;
import com.ospreydcs.dp.service.integration.GrpcIntegrationServiceWrapperBase;
import com.ospreydcs.dp.service.integration.ingest.GrpcIntegrationIngestionServiceWrapper;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import org.apache.commons.collections4.list.TreeList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.ClassRule;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.mock;

public class GrpcIntegrationIngestionStreamServiceWrapper extends GrpcIntegrationServiceWrapperBase<IngestionStreamServiceImpl> {

    // static variables
    private static final Logger logger = LogManager.getLogger();
    @ClassRule
    public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    // instance variables (common ones inherited from base class)
    private ManagedChannel ingestionChannel;

    public void init(MongoTestClient mongoClient, ManagedChannel ingestionChannel) {
        this.ingestionChannel = ingestionChannel;
        super.init(mongoClient);
    }

    @Override
    protected boolean initService() {
        IngestionStreamHandler ingestionStreamHandlerInstance = new IngestionStreamHandler(ingestionChannel);
        IngestionStreamHandlerInterface ingestionStreamHandler = ingestionStreamHandlerInstance;
        service = new IngestionStreamServiceImpl();
        return service.init(ingestionStreamHandler);
    }

    @Override
    protected void finiService() {
        service.fini();
    }

    @Override
    protected IngestionStreamServiceImpl createServiceMock(IngestionStreamServiceImpl service) {
        return mock(IngestionStreamServiceImpl.class, delegatesTo(service));
    }

    @Override
    protected GrpcCleanupRule getGrpcCleanupRule() {
        return grpcCleanup;
    }

    @Override
    protected String getServiceName() {
        return "IngestionStreamServiceImpl";
    }
    
    private IngestionStreamTestBase.SubscribeDataEventCall sendSubscribeDataEvent(
            SubscribeDataEventRequest request,
            int expectedEventResponseCount,
            int expectedDataBucketCount,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final DpIngestionStreamServiceGrpc.DpIngestionStreamServiceStub asyncStub =
                DpIngestionStreamServiceGrpc.newStub(channel);

        final IngestionStreamTestBase.SubscribeDataEventResponseObserver responseObserver =
                new IngestionStreamTestBase.SubscribeDataEventResponseObserver(
                        expectedEventResponseCount + expectedDataBucketCount);

        // invoke subscribeDataEvent() API method, get handle to request stream
        StreamObserver<SubscribeDataEventRequest> requestObserver = asyncStub.subscribeDataEvent(responseObserver);

        // send request message in request stream
        new Thread(() -> {
            requestObserver.onNext(request);
        }).start();

        // wait for ack response
        responseObserver.awaitAckLatch();

        if (expectReject) {
            assertFalse(expectedRejectMessage.isBlank());
            assertTrue(responseObserver.isError());
            assertTrue(responseObserver.getErrorMessage().contains(expectedRejectMessage));
        } else {
            assertFalse(responseObserver.getErrorMessage(), responseObserver.isError());
        }

        return new IngestionStreamTestBase.SubscribeDataEventCall(requestObserver, responseObserver);
    }

    protected IngestionStreamTestBase.SubscribeDataEventCall initiateSubscribeDataEventRequest(
            IngestionStreamTestBase.SubscribeDataEventRequestParams requestParams,
            int expectedEventResponseCount,
            int expectedDataBucketCount,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final SubscribeDataEventRequest request = IngestionStreamTestBase.buildSubscribeDataEventRequest(requestParams);
        return sendSubscribeDataEvent(
                request,
                expectedEventResponseCount,
                expectedDataBucketCount,
                expectReject,
                expectedRejectMessage);
    }

    protected void verifySubscribeDataEventError(
            IngestionStreamTestBase.SubscribeDataEventResponseObserver responseObserver,
            String errorMsg
    ) {
        // wait for error in API response stream
        responseObserver.awaitCloseLatch();
        assertTrue(responseObserver.isError());
        assertTrue(responseObserver.getErrorMessage().contains(errorMsg));
    }

    protected void verifySubscribeDataEventResponse(
            IngestionStreamTestBase.SubscribeDataEventResponseObserver responseObserver,
            IngestionStreamTestBase.SubscribeDataEventRequestParams requestParams,
            Map<String, GrpcIntegrationIngestionServiceWrapper.IngestionStreamInfo> ingestionValidationMap,
            Map<PvConditionTrigger, List<SubscribeDataEventResponse.Event>> expectedEventResponses,
            Map<SubscribeDataEventResponse.Event, Map<String, List<Instant>>> expectedEventDataResponses,
            int numExpectedSerializedColumns
    ) {
        // wait for completion of API method response stream and confirm not in error state
        responseObserver.awaitResponseLatch();
        assertFalse(responseObserver.isError());

        // get subscription responses for verification of expected contents
        final List<SubscribeDataEventResponse> responseList = responseObserver.getResponseList();

        // create lists of different response types for further verification
        final Map<PvConditionTrigger, List<SubscribeDataEventResponse.Event>> actualEventResponses = new HashMap<>();
        Map<SubscribeDataEventResponse.Event, Map<String, List<Instant>>> actualEventDataResponses = new HashMap<>();
        int actualSerializedColumnCount = 0;
        for (SubscribeDataEventResponse response : responseList) {

            switch (response.getResultCase()) {
                case EXCEPTIONALRESULT -> {
                    fail("received ExceptionalResult");
                }
                case ACK -> {
                    // responseList doesn't contain acks
                }
                case EVENT -> {
                    final SubscribeDataEventResponse.Event event = response.getEvent();
                    final PvConditionTrigger trigger = event.getTrigger();
                    final List<SubscribeDataEventResponse.Event> actualTriggerEvents =
                            actualEventResponses.computeIfAbsent(trigger, k -> new ArrayList<>());
                    actualTriggerEvents.add(event);
                }
                case EVENTDATA -> {
                    final SubscribeDataEventResponse.EventData eventData = response.getEventData();
                    assertTrue(eventData.hasEvent());
                    final SubscribeDataEventResponse.Event event = eventData.getEvent();
                    final Map<String, List<Instant>> actualEventBucketTimesMap =
                            actualEventDataResponses.computeIfAbsent(event, k -> new HashMap<>());
                    final List<DataBucket> eventBuckets = eventData.getDataBucketsList();
                    for (DataBucket dataBucket : eventBuckets) {
                        DataColumn bucketColumn = null;
                        if (dataBucket.hasDataColumn()) {
                            bucketColumn = dataBucket.getDataColumn();
                        } else if (dataBucket.hasSerializedDataColumn()) {
                            actualSerializedColumnCount = actualSerializedColumnCount + 1;
                            try {
                                bucketColumn =
                                        DataColumn.parseFrom(dataBucket.getSerializedDataColumn().getPayload());
                            } catch (InvalidProtocolBufferException e) {
                                fail(("InvalidProtocolException parsing SerializedDataColumn: " + e.getMessage()));
                            }
                        } else {
                            fail("unknown column type in DataBucket");
                        }
                        assertNotNull(bucketColumn);
                        final DataTimestamps bucketDataTimestamps = dataBucket.getDataTimestamps();
                        final DataTimestampsUtility.DataTimestampsModel bucketDataTimestampsModel =
                                new DataTimestampsUtility.DataTimestampsModel(bucketDataTimestamps);
                        final String bucketPvName = bucketColumn.getName();
                        final List<Instant> bucketInstants =
                                actualEventBucketTimesMap.computeIfAbsent(bucketPvName, k -> new TreeList<>());
                        bucketInstants.add(
                                TimestampUtility.instantFromTimestamp(bucketDataTimestampsModel.getFirstTimestamp()));
                    }
                }
                case RESULT_NOT_SET -> {
                    fail("received response with result not set");
                }
            }
        }

        // check TriggeredEvent responses against expected
        //assertEquals(expectedEventResponses, actualEventResponses); // maps in different order
        assertEquals(expectedEventResponses.size(), actualEventResponses.size());
        for (Map.Entry<PvConditionTrigger, List<SubscribeDataEventResponse.Event>> entry : expectedEventResponses.entrySet()) {
            assertTrue(actualEventResponses.containsKey(entry.getKey()));
            assertEquals(entry.getValue().size(), actualEventResponses.get(entry.getKey()).size());
            assertEquals(entry.getValue(), actualEventResponses.get(entry.getKey()));
        }

        // check DataEvent responses against expected
        if (expectedEventDataResponses != null && !expectedEventDataResponses.isEmpty()) {
            //assertEquals(expectedEventDataResponses, actualEventDataResponses); // maps in different order
            for (Map.Entry<SubscribeDataEventResponse.Event, Map<String, List<Instant>>> entry : expectedEventDataResponses.entrySet()) {
                assertTrue(actualEventDataResponses.containsKey(entry.getKey()));
                assertEquals(entry.getValue().size(), actualEventDataResponses.get(entry.getKey()).size());
                final Map<String, List<Instant>> expectedPvMap = entry.getValue();
                final Map<String, List<Instant>> actualPvMap = actualEventDataResponses.get(entry.getKey());
                for (var expectedPvEntry : expectedPvMap.entrySet()) {
                    assertTrue(actualPvMap.containsKey(expectedPvEntry.getKey()));
                    assertEquals(expectedPvEntry.getValue().size(), actualPvMap.get(expectedPvEntry.getKey()).size());
                    final List<Instant> expectedInstantList = expectedPvEntry.getValue();
                    final List<Instant> actualInstantList = actualPvMap.get(expectedPvEntry.getKey());
                    for (Instant expectedInstant : expectedInstantList) {
                        assertTrue(actualInstantList.contains(expectedInstant));
                    }
                }
            }
        }

        // check actual number of SerializedDataColumns in response matches expected
        assertEquals(numExpectedSerializedColumns, actualSerializedColumnCount);
    }
    
    protected void cancelSubscribeDataEventCall(IngestionStreamTestBase.SubscribeDataEventCall subscribeDataEventCall) {

        final SubscribeDataEventRequest request = IngestionStreamTestBase.buildSubscribeDataEventCancelRequest();

        // send NewSubscription message in request stream
        new Thread(() -> {
            subscribeDataEventCall.requestObserver().onNext(request);
        }).start();

        // wait for response stream to close
        final IngestionStreamTestBase.SubscribeDataEventResponseObserver responseObserver =
                (IngestionStreamTestBase.SubscribeDataEventResponseObserver) subscribeDataEventCall.responseObserver();
        responseObserver.awaitCloseLatch();

    }

    protected void closeSubscribeDataEventCall(IngestionStreamTestBase.SubscribeDataEventCall subscribeDataEventCall) {

        // close the request stream
        new Thread(subscribeDataEventCall.requestObserver()::onCompleted).start();

        // wait for response stream to close
        final IngestionStreamTestBase.SubscribeDataEventResponseObserver responseObserver =
                (IngestionStreamTestBase.SubscribeDataEventResponseObserver) subscribeDataEventCall.responseObserver();
        responseObserver.awaitCloseLatch();
    }

}
