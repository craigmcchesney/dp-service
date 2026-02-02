package com.ospreydcs.dp.client;

import com.ospreydcs.dp.client.result.IngestDataApiResult;
import com.ospreydcs.dp.client.result.RegisterProviderApiResult;
import com.ospreydcs.dp.grpc.v1.common.*;
import com.ospreydcs.dp.grpc.v1.ingestion.*;
import com.ospreydcs.dp.service.common.protobuf.AttributesUtility;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class IngestionClient extends ServiceApiClientBase {

    public static class RegisterProviderRequestParams {

        public final String name;
        public final String description;
        public final List<String> tags;
        public final Map<String, String> attributes;

        public RegisterProviderRequestParams(String name, Map<String, String> attributes) {
            this.name = name;
            this.description = null;
            this.tags = null;
            this.attributes = attributes;
        }

        public RegisterProviderRequestParams(
                String name,
                String description,
                List<String> tags,
                Map<String, String> attributes
        ) {
            this.name = name;
            this.description = description;
            this.tags = tags;
            this.attributes = attributes;
        }
    }

    public static class RegisterProviderResponseObserver implements StreamObserver<RegisterProviderResponse> {

        // instance variables
        private final CountDownLatch finishLatch = new CountDownLatch(1);
        private final AtomicBoolean isError = new AtomicBoolean(false);
        private final List<String> errorMessageList = Collections.synchronizedList(new ArrayList<>());
        private final List<RegisterProviderResponse> responseList = Collections.synchronizedList(new ArrayList<>());

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

        public List<RegisterProviderResponse> getResponseList() {
            return responseList;
        }

        @Override
        public void onNext(RegisterProviderResponse response) {

            // handle response in separate thread to better simulate out of process grpc,
            // otherwise response is handled in same thread as service handler that sent it
            new Thread(() -> {

                if (response.hasExceptionalResult()) {
                    final String errorMsg = "onNext received exceptional response: "
                            + response.getExceptionalResult().getMessage();
                    System.err.println(errorMsg);
                    isError.set(true);
                    errorMessageList.add(errorMsg);
                    finishLatch.countDown();
                    return;
                }

                // flag error if already received a response
                if (!responseList.isEmpty()) {
                    final String errorMsg = "onNext received more than one response";
                    System.err.println(errorMsg);
                    isError.set(true);
                    errorMessageList.add(errorMsg);

                } else {
                    responseList.add(response);
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

    public static enum IngestionDataType {
        STRING,
        BOOLEAN,
        UINT,
        ULONG,
        INT,
        LONG,
        FLOAT,
        DOUBLE,
        BYTE_ARRAY,
        ARRAY,
        ARRAY_DOUBLE,
        IMAGE,
        STRUCTURE,
        TIMESTAMP
    }

    public static class IngestionRequestParams {

        public String providerId = null;
        public String requestId = null;
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
        public Boolean useSerializedDataColumns = false;

        public IngestionRequestParams(
                String providerId,
                String requestId
        ) {
            this.providerId = providerId;
            this.requestId = requestId;
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
                boolean useSerializedDataColumns
        ) {
            this(providerId, requestId);

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
            this.useSerializedDataColumns = useSerializedDataColumns;
        }
    }

    public static class IngestDataResponseObserver implements StreamObserver<IngestDataResponse> {

        // instance variables
        CountDownLatch finishLatch = null;
        private final List<IngestDataResponse> responseList = Collections.synchronizedList(new ArrayList<>());
        private final AtomicBoolean isError = new AtomicBoolean(false);
        private final List<String> errorMessageList = Collections.synchronizedList(new ArrayList<>());

        public IngestDataResponseObserver(int expectedResponseCount) {
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

        public String getErrorMessage() {
            if (!errorMessageList.isEmpty()) {
                return errorMessageList.get(0);
            } else {
                return "";
            }
        }

        @Override
        public void onNext(IngestDataResponse response) {

            if (response.hasExceptionalResult()) {
                final String errorMsg = "onNext received exceptional response: "
                        + response.getExceptionalResult().getMessage();
                System.err.println(errorMsg);
                isError.set(true);
                errorMessageList.add(errorMsg);
                finishLatch.countDown();
                return;
            }

            responseList.add(response);
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

    // static variables
    private static final Logger logger = LogManager.getLogger();

    public IngestionClient(ManagedChannel channel) {
        super(channel);
    }

    public static RegisterProviderRequest buildRegisterProviderRequest(RegisterProviderRequestParams params) {

        RegisterProviderRequest.Builder builder = RegisterProviderRequest.newBuilder();

        if (params.name != null) {
            builder.setProviderName(params.name);
        }

        if (params.description != null) {
            builder.setDescription(params.description);
        }

        if (params.tags != null) {
            builder.addAllTags(params.tags);
        }

        if (params.attributes != null) {
            builder.addAllAttributes(AttributesUtility.attributeListFromMap(params.attributes));
        }

        return builder.build();
    }

    public RegisterProviderApiResult sendRegisterProvider(
            RegisterProviderRequest request
    ) {
        final DpIngestionServiceGrpc.DpIngestionServiceStub asyncStub =
                DpIngestionServiceGrpc.newStub(channel);

        final RegisterProviderResponseObserver responseObserver =
                new RegisterProviderResponseObserver();

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            asyncStub.registerProvider(request, responseObserver);
        }).start();

        responseObserver.await();

        if (responseObserver.isError()) {
            return new RegisterProviderApiResult(true, responseObserver.getErrorMessage());
        } else {
            return new RegisterProviderApiResult(responseObserver.getResponseList().get(0));
        }
    }

    public RegisterProviderApiResult registerProvider(RegisterProviderRequestParams params) {
        // build request
        final RegisterProviderRequest request = buildRegisterProviderRequest(params);
        return sendRegisterProvider(request);
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
            List<Timestamp> timestamps,
            List<DataColumn> dataColumns
    ) {
        IngestDataRequest.Builder requestBuilder = IngestDataRequest.newBuilder();

        if (params.providerId != null) {
            requestBuilder.setProviderId(params.providerId);
        }
        if (params.requestId != null) {
            requestBuilder.setClientRequestId(params.requestId);
        }

        final IngestDataRequest.IngestionDataFrame.Builder dataFrameBuilder
                = IngestDataRequest.IngestionDataFrame.newBuilder();
        final DataTimestamps.Builder dataTimestampsBuilder = DataTimestamps.newBuilder();

        // set DataTimestamps for request
        if (timestamps != null) {
            final TimestampList timestampList = TimestampList.newBuilder().addAllTimestamps(timestamps).build();
            dataTimestampsBuilder.setTimestampList(timestampList);
            dataTimestampsBuilder.build();
            dataFrameBuilder.setDataTimestamps(dataTimestampsBuilder);

        } else if (params.timestampsSecondsList != null) {
            // use explicit timestamp list in DataTimestamps if specified in params

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

        if (dataColumns != null) {
            // use list of columns if provided by caller
            frameColumns.addAll(dataColumns);

        } else if (params.columnNames != null) {
            // otherwise create columns from params

            for (int i = 0 ; i < params.columnNames.size() ; i++) {
                DataColumn.Builder dataColumnBuilder = DataColumn.newBuilder();
                dataColumnBuilder.setName(params.columnNames.get(i));
                DataValue.Builder dataValueBuilder = null;
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
                            dataValueBuilder = DataValue.newBuilder().setIntValue((Integer) value);
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
                                logger.error("unexpected value list type: " + value.getClass().getName());
                            }
                            Array.Builder arrayBuilder = Array.newBuilder();
                            for (var listElement : valueList) {
                                if (!(listElement instanceof Double)) {
                                    logger.error("unexpected value list element type: " + listElement.getClass().getName());
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

        // add DataColumns or SerializedDataColumns
        if (params.useSerializedDataColumns) {
            // add SerializedDataColumns as specified in params
            for (DataColumn dataColumn : frameColumns) {
                final SerializedDataColumn serializedDataColumn =
                        SerializedDataColumn.newBuilder()
                                .setName(dataColumn.getName())
                                .setPayload(dataColumn.toByteString())
                                .build();
                dataFrameBuilder.addSerializedDataColumns(serializedDataColumn);
            }

        } else {
            // add regular DataColumns
            dataFrameBuilder.addAllDataColumns(frameColumns);
        }

        dataFrameBuilder.build();
        requestBuilder.setIngestionDataFrame(dataFrameBuilder);
        return requestBuilder.build();
    }

    public IngestDataApiResult sendIngestData(IngestDataRequest request) {


        final DpIngestionServiceGrpc.DpIngestionServiceStub asyncStub =
                DpIngestionServiceGrpc.newStub(channel);

        final IngestDataResponseObserver responseObserver = new IngestDataResponseObserver(1);

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            asyncStub.ingestData(request, responseObserver);
        }).start();

        responseObserver.await();

        if (responseObserver.isError()) {
            return new IngestDataApiResult(true, responseObserver.getErrorMessage());
        } else {
            return new IngestDataApiResult(responseObserver.getResponseList().get(0));
        }
    }

    public IngestDataApiResult ingestData(
            IngestionRequestParams params,
            List<Timestamp> timestamps,
            List<DataColumn> dataColumns
    ) {
        final IngestDataRequest request = buildIngestionRequest(params, timestamps, dataColumns);
        return sendIngestData(request);
    }

}
