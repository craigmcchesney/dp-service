package com.ospreydcs.dp.service.ingest;

import com.ospreydcs.dp.grpc.v1.common.*;
import com.ospreydcs.dp.grpc.v1.ingestion.*;
import com.ospreydcs.dp.service.common.grpc.AttributesUtility;
import com.ospreydcs.dp.service.common.grpc.TimestampUtility;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

/**
 * Provides features and utilities for testing of ingestion service by inheritance to derived classes.
 */
public class IngestionTestBase {

    public static enum IngestionDataType {
        STRING,
        DOUBLE,
        INT,
        BYTE_ARRAY,
        BOOLEAN,
        IMAGE,
        STRUCTURE,
        ARRAY_DOUBLE
    }

    /**
     * Encapsulates the parameters for creating an IngestionRequest API object.
     */
    public static class IngestionRequestParams {

        public String providerId = null;
        public String requestId = null;
        public boolean setRequestTime = true;
        public Long snapshotStartTimestampSeconds = null;
        public Long snapshotStartTimestampNanos = null;
        public List<Long> timestampsSecondsList = null;
        public List<Long> timestampNanosList = null;
        public Long samplingClockStartSeconds = null;
        public Long samplingClockStartNanos = null;
        public Long samplingClockPeriodNanos = null;
        public Integer samplingClockCount = null;
        public List<String> columnNames = null;
        public IngestionDataType dataType = null;
        public List<List<Object>> values = null;
        public List<List<DataValue.ValueStatus>> valuesStatus = null;
        public Map<String, String> attributes = null;
        public String eventDescription = null;
        public Long eventStartSeconds = null;
        public Long eventStartNanos = null;
        public Long eventStopSeconds = null;
        public Long eventStopNanos = null;

        public IngestionRequestParams(
                String providerId,
                String requestId,
                Long snapshotStartTimestampSeconds,
                Long snapshotStartTimestampNanos,
                List<Long> timestampsSecondsList,
                List<Long> timestampNanosList,
                Long samplingClockStartSeconds,
                Long samplingClockStartNanos,
                Long samplingClockPeriodNanos,
                Integer samplingClockCount,
                List<String> columnNames,
                IngestionDataType dataType,
                List<List<Object>> values,
                List<List<DataValue.ValueStatus>> valuesStatus
        ) {
            this.providerId = providerId;
            this.requestId = requestId;
            this.snapshotStartTimestampSeconds = snapshotStartTimestampSeconds;
            this.snapshotStartTimestampNanos = snapshotStartTimestampNanos;
            this.timestampsSecondsList = timestampsSecondsList;
            this.timestampNanosList = timestampNanosList;
            this.samplingClockStartSeconds = samplingClockStartSeconds;
            this.samplingClockStartNanos = samplingClockStartNanos;
            this.samplingClockPeriodNanos = samplingClockPeriodNanos;
            this.samplingClockCount = samplingClockCount;
            this.columnNames = columnNames;
            this.dataType = dataType;
            this.values = values;
            this.valuesStatus = valuesStatus;
        }

        public IngestionRequestParams(
                String providerId,
                String requestId,
                Long snapshotStartTimestampSeconds,
                Long snapshotStartTimestampNanos,
                List<Long> timestampsSecondsList,
                List<Long> timestampNanosList,
                Long samplingClockStartSeconds,
                Long samplingClockStartNanos,
                Long samplingClockPeriodNanos,
                Integer samplingClockCount,
                List<String> columnNames,
                IngestionDataType dataType,
                List<List<Object>> values,
                Map<String, String> attributes,
                String eventDescription,
                Long eventStartSeconds,
                Long eventStartNanos,
                Long eventStopSeconds,
                Long eventStopNanos) {

            this(
                    providerId,
                    requestId,
                    snapshotStartTimestampSeconds,
                    snapshotStartTimestampNanos,
                    timestampsSecondsList,
                    timestampNanosList,
                    samplingClockStartSeconds,
                    samplingClockStartNanos,
                    samplingClockPeriodNanos,
                    samplingClockCount,
                    columnNames,
                    dataType,
                    values,
                    null);

            this.attributes = attributes;
            this.eventDescription = eventDescription;
            this.eventStartSeconds = eventStartSeconds;
            this.eventStartNanos = eventStartNanos;
            this.eventStopSeconds = eventStopSeconds;
            this.eventStopNanos = eventStopNanos;
        }

        public void setRequestTime(boolean setRequestTime) {
            this.setRequestTime = setRequestTime;
        }
    }

    public static IngestDataRequest buildIngestionRequest(IngestionRequestParams params) {
        return buildIngestionRequest(params, null);
    }
    /**
     * Builds an IngestionRequest gRPC API object from an IngestionRequestParams object.
     * This utility avoids having code to build API requests scattered around the test methods.
     *
     * @param params
     * @return
     */
    public static IngestDataRequest buildIngestionRequest(
            IngestionRequestParams params,
            List<DataColumn> dataColumnList
    ) {
        IngestDataRequest.Builder requestBuilder = IngestDataRequest.newBuilder();

        if (params.providerId != null) {
            requestBuilder.setProviderId(params.providerId);
        }
        if (params.requestId != null) {
            requestBuilder.setClientRequestId(params.requestId);
        }
        if (params.setRequestTime) {
            requestBuilder.setRequestTime(TimestampUtility.getTimestampNow());
        }

        IngestDataRequest.IngestionDataFrame.Builder dataFrameBuilder
                = IngestDataRequest.IngestionDataFrame.newBuilder();
        DataTimestamps.Builder dataTimestampsBuilder = DataTimestamps.newBuilder();

        // set DataTimestamps for request
        if (params.timestampsSecondsList != null) {
            // use explicit timestamp list in DataTimestamps if specified in params

            assertTrue(params.timestampNanosList != null);
            assertTrue(params.timestampsSecondsList.size() == params.timestampNanosList.size());
            TimestampList.Builder timestampListBuilder = TimestampList.newBuilder();
            for (int i = 0; i < params.timestampsSecondsList.size(); i++) {
                long seconds = params.timestampsSecondsList.get(i);
                long nanos = params.timestampNanosList.get(i);
                Timestamp.Builder timestampBuilder = Timestamp.newBuilder();
                timestampBuilder.setEpochSeconds(seconds);
                timestampBuilder.setNanoseconds(nanos);
                timestampBuilder.build();
                timestampListBuilder.addTimestamps(timestampBuilder);
            }
            timestampListBuilder.build();
            dataTimestampsBuilder.setTimestampList(timestampListBuilder);
            dataTimestampsBuilder.build();
            dataFrameBuilder.setDataTimestamps(dataTimestampsBuilder);

        } else if (params.samplingClockStartSeconds != null) {
            // otherwise use Samplingclock for DataTimestamps

            assertTrue(params.samplingClockStartNanos != null);
            assertTrue(params.samplingClockPeriodNanos != null);
            assertTrue(params.samplingClockCount != null);
            Timestamp.Builder startTimeBuilder = Timestamp.newBuilder();
            startTimeBuilder.setEpochSeconds(params.samplingClockStartSeconds);
            startTimeBuilder.setNanoseconds(params.samplingClockStartNanos);
            startTimeBuilder.build();
            SamplingClock.Builder samplingClockBuilder = SamplingClock.newBuilder();
            samplingClockBuilder.setStartTime(startTimeBuilder);
            samplingClockBuilder.setPeriodNanos(params.samplingClockPeriodNanos);
            samplingClockBuilder.setCount(params.samplingClockCount);
            samplingClockBuilder.build();
            dataTimestampsBuilder.setSamplingClock(samplingClockBuilder);
            dataTimestampsBuilder.build();
            dataFrameBuilder.setDataTimestamps(dataTimestampsBuilder);
        }

        // create list of columns if specified
        if (dataColumnList != null) {
            // caller can override building data columns by providing dataColumnList
            for (DataColumn column : dataColumnList) {
                dataFrameBuilder.addDataColumns(column);
            }

        } else if (params.columnNames != null) {
            assertTrue(params.values != null);
            assertEquals(params.columnNames.size(), params.values.size());
            if (params.valuesStatus != null) {
                assertEquals(params.columnNames.size(), params.valuesStatus.size());
            }
            for (int i = 0 ; i < params.columnNames.size() ; i++) {
                DataColumn.Builder dataColumnBuilder = DataColumn.newBuilder();
                dataColumnBuilder.setName(params.columnNames.get(i));
                DataValue.Builder dataValueBuilder = null;
                if (params.valuesStatus != null) {
                    assertEquals(params.values.get(i).size(), params.valuesStatus.get(i).size());
                }
                int valueIndex = 0;
                for (Object value : params.values.get(i)) {
                    switch (params.dataType) {
                        case STRING -> {
                            dataValueBuilder = DataValue.newBuilder().setStringValue((String) value);
                        }
                        case DOUBLE -> {
                            dataValueBuilder = DataValue.newBuilder().setDoubleValue((Double) value);
                        }
                        case INT -> {
                            dataValueBuilder = DataValue.newBuilder().setLongValue((Long) value);
                        }
                        case BYTE_ARRAY -> {
                        }
                        case BOOLEAN -> {
                            dataValueBuilder = DataValue.newBuilder().setBooleanValue((Boolean) value);
                        }
                        case IMAGE -> {
                        }
                        case STRUCTURE -> {
                        }
                        case ARRAY_DOUBLE -> {
                            List<?> valueList = null;
                            if (value instanceof List) {
                                valueList = (List<?>) value;
                            } else {
                                fail("unexpected value list type: " + value.getClass().getName());
                            }
                            Array.Builder arrayBuilder = Array.newBuilder();
                            for (var listElement : valueList) {
                                if (!(listElement instanceof Double)) {
                                    fail("unexpected value list element type: " + listElement.getClass().getName());
                                }
                                arrayBuilder.addDataValues(
                                        DataValue.newBuilder()
                                                .setDoubleValue((Double) listElement)
                                                .build());
                            }
                            arrayBuilder.build();
                            dataValueBuilder = DataValue.newBuilder().setArrayValue(arrayBuilder);
                        }
                    }

                    if (params.valuesStatus != null) {
                        DataValue.ValueStatus valueStatus = params.valuesStatus.get(i).get(valueIndex);
                        dataValueBuilder.setValueStatus(valueStatus);
                    }

                    dataColumnBuilder.addDataValues(dataValueBuilder.build());
                    valueIndex++;
                }

                dataColumnBuilder.build();
                dataFrameBuilder.addDataColumns(dataColumnBuilder);
            }
        }

        // add attributes if specified
        if (params.attributes != null) {
            for (var attributeEntry : params.attributes.entrySet()) {
                String attributeKey = attributeEntry.getKey();
                String attributeValue = attributeEntry.getValue();
                final Attribute.Builder attributeBuilder = Attribute.newBuilder();
                attributeBuilder.setName(attributeKey);
                attributeBuilder.setValue(attributeValue);
                attributeBuilder.build();
                requestBuilder.addAttributes(attributeBuilder);
            }
        }

        // set event metadata if specified
        if (params.eventDescription != null ||  params.eventStartSeconds != null || params.eventStartNanos != null) {

            EventMetadata.Builder eventMetadataBuilder = EventMetadata.newBuilder();

            if (params.eventDescription != null) {
                eventMetadataBuilder.setDescription(params.eventDescription);
            }

            if (params.eventStartSeconds != null || params.eventStartNanos != null) {
                Timestamp.Builder eventStartTimeBuilder = Timestamp.newBuilder();
                if (params.eventStartSeconds != null) {
                    eventStartTimeBuilder.setEpochSeconds(params.eventStartSeconds);
                }
                if (params.eventStartNanos != null) {
                    eventStartTimeBuilder.setNanoseconds(params.eventStartNanos);
                }
                eventStartTimeBuilder.build();
                eventMetadataBuilder.setStartTimestamp(eventStartTimeBuilder);
            }

            if (params.eventStopSeconds != null || params.eventStopNanos != null) {
                Timestamp.Builder eventStopTimeBuilder = Timestamp.newBuilder();
                if (params.eventStopSeconds != null) {
                    eventStopTimeBuilder.setEpochSeconds(params.eventStopSeconds);
                }
                if (params.eventStopNanos != null) {
                    eventStopTimeBuilder.setNanoseconds(params.eventStopNanos);
                }
                eventStopTimeBuilder.build();
                eventMetadataBuilder.setStopTimestamp(eventStopTimeBuilder);
            }

            eventMetadataBuilder.build();
            requestBuilder.setEventMetadata(eventMetadataBuilder);
        }

        dataFrameBuilder.build();
        requestBuilder.setIngestionDataFrame(dataFrameBuilder);
        return requestBuilder.build();
    }

    /**
     * This class implements the StreamObserver interface for IngestionResponse objects for testing
     * IngestionHandler.handleStreamingIngestionRequest().  The constructor specifies the number of
     * IngestionResponse messages expected by the observer.  A CountDownLatch of the specified size is created
     * and decremented for each message received.  The user can use await() to know when all responses have been
     * received.
     */
    public static class IngestionResponseObserver implements StreamObserver<IngestDataResponse> {

        // instance variables
        CountDownLatch finishLatch = null;
        private final List<IngestDataResponse> responseList = Collections.synchronizedList(new ArrayList<>());
        private final AtomicBoolean isError = new AtomicBoolean(false);

        public IngestionResponseObserver(int expectedResponseCount) {
            this.finishLatch = new CountDownLatch(expectedResponseCount);
        }

        public void await() {
            try {
                finishLatch.await(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                System.err.println("InterruptedException waiting for finishLatch");
                isError.set(true);
            }
        }

        public List<IngestDataResponse> getResponseList() {
            return responseList;
        }

        public boolean isError() { return isError.get(); }

        @Override
        public void onNext(IngestDataResponse ingestionResponse) {
            responseList.add(ingestionResponse);
            finishLatch.countDown();
        }

        @Override
        public void onError(Throwable t) {
            Status status = Status.fromThrowable(t);
            System.err.println("IngestionResponseObserver error: " + status);
            isError.set(true);
        }

        @Override
        public void onCompleted() {
        }
    }

    public static class IngestDataStreamResponseObserver implements StreamObserver<IngestDataStreamResponse> {

        // instance variables
        CountDownLatch finishLatch = null;
        private final List<IngestDataStreamResponse> responseList = Collections.synchronizedList(new ArrayList<>());
        private final AtomicBoolean isError = new AtomicBoolean(false);

        public IngestDataStreamResponseObserver() {
            this.finishLatch = new CountDownLatch(1);
        }

        public void await() {
            try {
                finishLatch.await(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                System.err.println("InterruptedException waiting for finishLatch");
                isError.set(true);
            }
        }

        public IngestDataStreamResponse getResponse() {
            if (responseList.size() != 1) {
                fail("response list size != 1");
            }
            return responseList.get(0);
        }

        public boolean isError() { return isError.get(); }

        public void onNext(IngestDataStreamResponse ingestionResponse) {
            responseList.add(ingestionResponse);
            finishLatch.countDown();
        }

        @Override
        public void onError(Throwable t) {
            Status status = Status.fromThrowable(t);
            System.err.println("IngestDataStreamResponseObserver error: " + status);
            isError.set(true);
        }

        @Override
        public void onCompleted() {
        }
    }
    
    public static class QueryRequestStatusParams {
        
        public final String providerId;
        public final String providerName;
        public final String requestId;
        public final List<IngestionRequestStatus> status;
        public final Long beginSeconds;
        public final Long beginNanos;
        public final Long endSeconds;
        public final Long endNanos;

        public QueryRequestStatusParams(
                String providerId,
                String providerName,
                String requestId,
                List<IngestionRequestStatus> status,
                Long beginSeconds,
                Long beginNanos,
                Long endSeconds,
                Long endNanos
        ) {
            this.providerId = providerId;
            this.providerName = providerName;
            this.requestId = requestId;
            this.status = status;
            this.beginSeconds = beginSeconds;
            this.beginNanos = beginNanos;
            this.endSeconds = endSeconds;
            this.endNanos = endNanos;
        }
    }

    public static class QueryRequestStatusExpectedResponse {
        public final String providerId;
        public final String requestId;
        public final IngestionRequestStatus status;
        public final String statusMessage;
        public final List<String> idsCreated;

        public QueryRequestStatusExpectedResponse(
                String providerId,
                String requestId,
                IngestionRequestStatus status,
                String statusMessage,
                List<String> idsCreated
        ) {
            this.providerId = providerId;
            this.requestId = requestId;
            this.status = status;
            this.statusMessage = statusMessage;
            this.idsCreated = idsCreated;
        }
    }

    public static class QueryRequestStatusExpectedResponseMap {

        public final Map<String, Map<String, QueryRequestStatusExpectedResponse>> expectedResponses = new HashMap<>();

        public void addExpectedResponse(QueryRequestStatusExpectedResponse response) {
            final String providerId = response.providerId;
            final String requestId = response.requestId;
            Map<String, QueryRequestStatusExpectedResponse> providerResponseMap;
            if (expectedResponses.containsKey(providerId)) {
                providerResponseMap = expectedResponses.get(providerId);
            } else {
                providerResponseMap = new HashMap<>();
                expectedResponses.put(providerId, providerResponseMap);
            }
            providerResponseMap.put(requestId, response);
        }

        public int size() {
            int size = 0;
            for (Map<String, QueryRequestStatusExpectedResponse> providerMap : expectedResponses.values()) {
                size = size + providerMap.size();
            }
            return size;
        }

        public QueryRequestStatusExpectedResponse get(String providerId, String requestId) {
            if (expectedResponses.containsKey(providerId)) {
                return expectedResponses.get(providerId).get(requestId);
            } else {
                return null;
            }
        }
    }

    public static class QueryRequestStatusResponseObserver implements StreamObserver<QueryRequestStatusResponse> {

        // instance variables
        private final CountDownLatch finishLatch = new CountDownLatch(1);
        private final AtomicBoolean isError = new AtomicBoolean(false);
        private final List<String> errorMessageList = Collections.synchronizedList(new ArrayList<>());
        private final List<QueryRequestStatusResponse.RequestStatusResult.RequestStatus> requestStatusList =
                Collections.synchronizedList(new ArrayList<>());

        public void await() {
            try {
                finishLatch.await(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                final String errorMsg = "InterruptedException waiting for finishLatch";
                System.err.println(errorMsg);
                isError.set(true);
                errorMessageList.add(errorMsg);
            }
        }

        public boolean isError() { return isError.get(); }
        public String getErrorMessage() {
            if (!errorMessageList.isEmpty()) {
                return errorMessageList.get(0);
            } else {
                return "";
            }
        }

        public List<QueryRequestStatusResponse.RequestStatusResult.RequestStatus> getRequestStatusList() {
            return requestStatusList;
        }

        @Override
        public void onNext(QueryRequestStatusResponse response) {

            // handle response in separate thread to better simulate out of process grpc,
            // otherwise response is handled in same thread as service handler that sent it
            new Thread(() -> {

                if (response.hasExceptionalResult()) {
                    final String errorMsg = "onNext received exception response: "
                            + response.getExceptionalResult().getMessage();
                    System.err.println(errorMsg);
                    isError.set(true);
                    errorMessageList.add(errorMsg);
                    finishLatch.countDown();
                    return;
                }

                assertTrue(response.hasRequestStatusResult());
                final QueryRequestStatusResponse.RequestStatusResult requestStatusResult =
                        response.getRequestStatusResult();
                assertNotNull(requestStatusResult);

                // flag error if already received a response
                if (!requestStatusList.isEmpty()) {
                    final String errorMsg = "onNext received more than one response";
                    System.err.println(errorMsg);
                    isError.set(true);
                    errorMessageList.add(errorMsg);

                } else {
                    for (QueryRequestStatusResponse.RequestStatusResult.RequestStatus requestStatus :
                            requestStatusResult.getRequestStatusList()) {
                        requestStatusList.add(requestStatus);
                    }
                    finishLatch.countDown();
                }
            }).start();

        }

        @Override
        public void onError(Throwable t) {
            // handle response in separate thread to better simulate out of process grpc,
            // otherwise response is handled in same thread as service handler that sent it
            new Thread(() -> {
                final Status status = Status.fromThrowable(t);
                final String errorMsg = "onError: " + status;
                System.err.println(errorMsg);
                isError.set(true);
                errorMessageList.add(errorMsg);
                finishLatch.countDown();
            }).start();
        }

        @Override
        public void onCompleted() {
        }
    }


    public static QueryRequestStatusRequest buildQueryRequestStatusRequest(QueryRequestStatusParams params) {

        QueryRequestStatusRequest.Builder requestBuilder = QueryRequestStatusRequest.newBuilder();

        if (params.providerId != null) {
            QueryRequestStatusRequest.QueryRequestStatusCriterion.ProviderIdCriterion providerIdCriterion =
                    QueryRequestStatusRequest.QueryRequestStatusCriterion.ProviderIdCriterion.newBuilder()
                            .setProviderId(params.providerId)
                            .build();
            QueryRequestStatusRequest.QueryRequestStatusCriterion criterion
                    = QueryRequestStatusRequest.QueryRequestStatusCriterion.newBuilder()
                    .setProviderIdCriterion(providerIdCriterion)
                    .build();
            requestBuilder.addCriteria(criterion);
        }

        if (params.providerName != null) {
            QueryRequestStatusRequest.QueryRequestStatusCriterion.ProviderNameCriterion providerNameCriterion =
                    QueryRequestStatusRequest.QueryRequestStatusCriterion.ProviderNameCriterion.newBuilder()
                            .setProviderName(params.providerName)
                            .build();
            QueryRequestStatusRequest.QueryRequestStatusCriterion criterion
                    = QueryRequestStatusRequest.QueryRequestStatusCriterion.newBuilder()
                    .setProviderNameCriterion(providerNameCriterion)
                    .build();
            requestBuilder.addCriteria(criterion);
        }

        if (params.requestId != null) {
            QueryRequestStatusRequest.QueryRequestStatusCriterion.RequestIdCriterion requestIdCriterion =
                    QueryRequestStatusRequest.QueryRequestStatusCriterion.RequestIdCriterion.newBuilder()
                            .setRequestId(params.requestId)
                            .build();
            QueryRequestStatusRequest.QueryRequestStatusCriterion criterion
                    = QueryRequestStatusRequest.QueryRequestStatusCriterion.newBuilder()
                    .setRequestIdCriterion(requestIdCriterion)
                    .build();
            requestBuilder.addCriteria(criterion);
        }

        if (params.status != null) {
            QueryRequestStatusRequest.QueryRequestStatusCriterion.StatusCriterion statusCriterion =
                    QueryRequestStatusRequest.QueryRequestStatusCriterion.StatusCriterion.newBuilder()
                            .addAllStatus(params.status)
                            .build();
            QueryRequestStatusRequest.QueryRequestStatusCriterion criterion
                    = QueryRequestStatusRequest.QueryRequestStatusCriterion.newBuilder()
                    .setStatusCriterion(statusCriterion)
                    .build();
            requestBuilder.addCriteria(criterion);
        }

        if (params.beginSeconds != null) {
            QueryRequestStatusRequest.QueryRequestStatusCriterion.TimeRangeCriterion.Builder timeRangeCriterionBuilder =
                    QueryRequestStatusRequest.QueryRequestStatusCriterion.TimeRangeCriterion.newBuilder();
            Timestamp beginTimestamp = Timestamp.newBuilder()
                    .setEpochSeconds(params.beginSeconds)
                    .setNanoseconds(params.beginNanos)
                    .build();
            timeRangeCriterionBuilder.setBeginTime(beginTimestamp);
            if (params.endSeconds != null) {
                Timestamp endTimestamp = Timestamp.newBuilder()
                        .setEpochSeconds(params.endSeconds)
                        .setNanoseconds(params.endNanos)
                        .build();
                timeRangeCriterionBuilder.setEndTime(endTimestamp);
            }
            QueryRequestStatusRequest.QueryRequestStatusCriterion criterion
                    = QueryRequestStatusRequest.QueryRequestStatusCriterion.newBuilder()
                    .setTimeRangeCriterion(timeRangeCriterionBuilder.build())
                    .build();
            requestBuilder.addCriteria(criterion);
        }

        return requestBuilder.build();
    }

}
