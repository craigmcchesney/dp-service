package com.ospreydcs.dp.service.ingest;

import com.ospreydcs.dp.grpc.v1.common.*;
import com.ospreydcs.dp.grpc.v1.ingestion.*;
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
     * Encapsulates the parameters for creating an IngestDataRequest API object.
     *
     * The caller should create an instance with the desired fields set, and then call one of the setter methods with
     * a list of columns of the appropriate type.  The DataColumns handling from the original implementation is preserved
     * for backward compatibility.
     */
    public static final class IngestionRequestParams {

        private final String providerId;
        private final String requestId;

        // fields for explicit list of timestamps in request DataTimestamps
        private final List<Long> timestampsSecondsList;
        private final List<Long> timestampNanosList;

        // fields for Sampling Clock in request DataTimestamps
        private final Long samplingClockStartSeconds;
        private final Long samplingClockStartNanos;
        private final Long samplingClockPeriodNanos;
        private final Integer samplingClockCount;

        // list of column names
        private final List<String> columnNames;

        // fields for building request list of DataColumns with DataValue / ValueStatus objects
        private final IngestionDataType dataType;
        private final List<List<Object>> values;
        private final List<List<DataValue.ValueStatus>> valuesStatus;

        // specifies to use SerializedDataColumns instead of regular DataColumns in the request
        private final boolean useSerializedDataColumns;

        // explicit list of prebuilt DataColumns, instead of construcint them from the dataType / values fields
        private final List<DataColumn> dataColumnList;

        // data column lists corresponding to the protobuf column types for ingestion
        // TODO: add other new protobuf column types
        private List<DoubleColumn> doubleColumnList = null;

        public IngestionRequestParams(
                String providerId,
                String requestId,
                List<Long> timestampsSecondsList,
                List<Long> timestampNanosList,
                Long samplingClockStartSeconds,
                Long samplingClockStartNanos,
                Long samplingClockPeriodNanos,
                Integer samplingClockCount,
                List<String> columnNames,
                IngestionDataType dataType,
                List<List<Object>> values,
                List<List<DataValue.ValueStatus>> valuesStatus,
                boolean useSerializedDataColumns,
                List<DataColumn> dataColumnList
        ) {
            this.providerId = providerId;
            this.requestId = requestId;
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
            this.useSerializedDataColumns = useSerializedDataColumns;
            this.dataColumnList = dataColumnList;
        }

        public String providerId() {
            return providerId;
        }

        public String requestId() {
            return requestId;
        }

        public List<Long> timestampsSecondsList() {
            return timestampsSecondsList;
        }

        public List<Long> timestampNanosList() {
            return timestampNanosList;
        }

        public Long samplingClockStartSeconds() {
            return samplingClockStartSeconds;
        }

        public Long samplingClockStartNanos() {
            return samplingClockStartNanos;
        }

        public Long samplingClockPeriodNanos() {
            return samplingClockPeriodNanos;
        }

        public Integer samplingClockCount() {
            return samplingClockCount;
        }

        public List<String> columnNames() {
            return columnNames;
        }

        public IngestionDataType dataType() {
            return dataType;
        }

        public List<List<Object>> values() {
            return values;
        }

        public List<List<DataValue.ValueStatus>> valuesStatus() {
            return valuesStatus;
        }

        public boolean useSerializedDataColumns() {
            return useSerializedDataColumns;
        }

        public List<DataColumn> dataColumnList() {
            return dataColumnList;
        }

        public void setDoubleColumnList(List<DoubleColumn> doubleColumnList) {
            this.doubleColumnList = doubleColumnList;
        }

        public List<DoubleColumn> doubleColumnList() {
            return doubleColumnList;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) return true;
            if (obj == null || obj.getClass() != this.getClass()) return false;
            var that = (IngestionRequestParams) obj;
            return Objects.equals(this.providerId, that.providerId) &&
                    Objects.equals(this.requestId, that.requestId) &&
                    Objects.equals(this.timestampsSecondsList, that.timestampsSecondsList) &&
                    Objects.equals(this.timestampNanosList, that.timestampNanosList) &&
                    Objects.equals(this.samplingClockStartSeconds, that.samplingClockStartSeconds) &&
                    Objects.equals(this.samplingClockStartNanos, that.samplingClockStartNanos) &&
                    Objects.equals(this.samplingClockPeriodNanos, that.samplingClockPeriodNanos) &&
                    Objects.equals(this.samplingClockCount, that.samplingClockCount) &&
                    Objects.equals(this.columnNames, that.columnNames) &&
                    Objects.equals(this.dataType, that.dataType) &&
                    Objects.equals(this.values, that.values) &&
                    Objects.equals(this.valuesStatus, that.valuesStatus) &&
                    this.useSerializedDataColumns == that.useSerializedDataColumns &&
                    Objects.equals(this.dataColumnList, that.dataColumnList) &&
                    Objects.equals(this.doubleColumnList, that.doubleColumnList);
        }

        @Override
        public int hashCode() {
            return Objects.hash(providerId, requestId, timestampsSecondsList, timestampNanosList, samplingClockStartSeconds, samplingClockStartNanos, samplingClockPeriodNanos, samplingClockCount, columnNames, dataType, values, valuesStatus, useSerializedDataColumns, dataColumnList, doubleColumnList);
        }

        @Override
        public String toString() {
            return "IngestionRequestParams[" +
                    "providerId=" + providerId + ", " +
                    "requestId=" + requestId + ", " +
                    "timestampsSecondsList=" + timestampsSecondsList + ", " +
                    "timestampNanosList=" + timestampNanosList + ", " +
                    "samplingClockStartSeconds=" + samplingClockStartSeconds + ", " +
                    "samplingClockStartNanos=" + samplingClockStartNanos + ", " +
                    "samplingClockPeriodNanos=" + samplingClockPeriodNanos + ", " +
                    "samplingClockCount=" + samplingClockCount + ", " +
                    "columnNames=" + columnNames + ", " +
                    "dataType=" + dataType + ", " +
                    "values=" + values + ", " +
                    "valuesStatus=" + valuesStatus + ", " +
                    "useSerializedDataColumns=" + useSerializedDataColumns + ", " +
                    "dataColumnList=" + dataColumnList + ", " +
                    "doubleColumnList=" + doubleColumnList + ']';
        }

        }

    /**
     * Builds an IngestDataRequest API object from an IngestionRequestParams object.
     * This utility avoids having code to build API requests scattered around the test methods.
     * If params object contains a list of column names and column data values, there is special handling
     * to create DataColumns containing the appropriate DataValues (depending on the type of Objects supplied in the
     * params).  Otherwise, the caller supplies a prebuilt list of protobuf columns of one of the supported types
     * and those columns are simply passed through to the resulting request object.
     *
     * @param params
     * @return
     */
    public static IngestDataRequest buildIngestionRequest(IngestionRequestParams params) {

        IngestDataRequest.Builder requestBuilder = IngestDataRequest.newBuilder();

        if (params.providerId != null) {
            requestBuilder.setProviderId(params.providerId);
        }
        if (params.requestId != null) {
            requestBuilder.setClientRequestId(params.requestId);
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

        // create list of columns
        final List<DataColumn> frameColumns = new ArrayList<>();

        // DataColumn handling: use explicit list if provided, otherwise build list of DataColumns if appropriate
        // params are specified.
        if (params.dataColumnList() != null) {
            // use explicit list of DataColumns provided by caller
            frameColumns.addAll(params.dataColumnList());

        } else if (params.columnNames != null && params.values != null) {
            // otherwise create DataColumns from column names and values from params

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

                frameColumns.add(dataColumnBuilder.build());
            }
        }

        // Determine whether to add DataColumns or SerializedDataColumns
        if (params.useSerializedDataColumns) {
            // add SerializedDataColumns as specified in params
            for (DataColumn dataColumn : frameColumns) {
                final SerializedDataColumn serializedDataColumn =
                        SerializedDataColumn.newBuilder()
                                .setName(dataColumn.getName())
                                .setEncoding("proto:DataColumn")
                                .setPayload(dataColumn.toByteString())
                                .build();
                dataFrameBuilder.addSerializedDataColumns(serializedDataColumn);
            }

        } else {
            // add regular DataColumns
            dataFrameBuilder.addAllDataColumns(frameColumns);
        }

        // pass through lists of data columns for various supported protobuf column types to request

        if (params.doubleColumnList() != null) {
            // use list of DoubleColumns provided by caller
            dataFrameBuilder.addAllDoubleColumns(params.doubleColumnList());
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
            System.err.println("IngestDataResponseObserver error: " + status);
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

    public record QueryRequestStatusParams(String providerId, String providerName, String requestId,
                                           List<IngestionRequestStatus> status, Long beginSeconds, Long beginNanos,
                                           Long endSeconds, Long endNanos) {

    }

    public static class QueryRequestStatusExpectedResponse {
        public final String providerId;
        public final String providerName;
        public final String requestId;
        public final IngestionRequestStatus status;
        public final String statusMessage;
        public final List<String> idsCreated;

        public QueryRequestStatusExpectedResponse(
                String providerId,
                String providerName,
                String requestId,
                IngestionRequestStatus status,
                String statusMessage,
                List<String> idsCreated
        ) {
            this.providerId = providerId;
            this.providerName = providerName;
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

        public boolean isError() {
            return isError.get();
        }

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

    public static class SubscribeDataResponseObserver implements StreamObserver<SubscribeDataResponse> {

        // instance variables
        CountDownLatch ackLatch = null;
        CountDownLatch responseLatch = null;
        CountDownLatch closeLatch = null;
        private final List<String> errorMessageList = Collections.synchronizedList(new ArrayList<>());
        private final List<SubscribeDataResponse> responseList = Collections.synchronizedList(new ArrayList<>());
        private final AtomicBoolean isError = new AtomicBoolean(false);

        public SubscribeDataResponseObserver(int expectedResponseCount) {
            this.ackLatch = new CountDownLatch(1);
            this.responseLatch = new CountDownLatch(expectedResponseCount);
            this.closeLatch = new CountDownLatch(1);
        }

        public void awaitAckLatch() {
            try {
                ackLatch.await(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                final String errorMsg = "InterruptedException waiting for ackLatch";
                System.err.println(errorMsg);
                isError.set(true);
                errorMessageList.add(errorMsg);
            }
        }

        public void awaitResponseLatch() {
            try {
                responseLatch.await(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                final String errorMsg = "InterruptedException waiting for responseLatch";
                System.err.println(errorMsg);
                isError.set(true);
                errorMessageList.add(errorMsg);
            }
        }

        public void awaitCloseLatch() {
            try {
                closeLatch.await(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                final String errorMsg = "InterruptedException waiting for closeLatch";
                System.err.println(errorMsg);
                isError.set(true);
                errorMessageList.add(errorMsg);
            }
        }

        public List<SubscribeDataResponse> getResponseList() {
            return responseList;
        }

        public boolean isError() {
            return isError.get();
        }

        public String getErrorMessage() {
            if (!errorMessageList.isEmpty()) {
                return errorMessageList.get(0);
            } else {
                return "";
            }
        }

        @Override
        public void onNext(SubscribeDataResponse response) {

            if (response.hasExceptionalResult()) {

                final String errorMsg = response.getExceptionalResult().getMessage();
                System.err.println(errorMsg);
                isError.set(true);
                errorMessageList.add(errorMsg);

                if (ackLatch.getCount() > 0) {
                    // decrement ackLatch if initial response in stream
                    ackLatch.countDown();
                }

            } else if (response.hasAckResult()) {
                // decrement ackLatch for ack response
                ackLatch.countDown();

            } else {
                // decrement responseLatch for all other responses
                responseList.add(response);
                responseLatch.countDown();
            }
        }

        @Override
        public void onError(Throwable t) {
            Status status = Status.fromThrowable(t);
            final String errorMsg = "SubscribeDataResponseObserver onError: " + status;
            System.err.println(errorMsg);
            isError.set(true);
            errorMessageList.add(errorMsg);
        }

        @Override
        public void onCompleted() {
            System.out.println("SubscribeDataResponseObserver onCompleted");
            closeLatch.countDown();
        }

    }

}
