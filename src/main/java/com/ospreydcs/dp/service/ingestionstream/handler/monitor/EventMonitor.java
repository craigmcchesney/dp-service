package com.ospreydcs.dp.service.ingestionstream.handler.monitor;

import com.google.protobuf.InvalidProtocolBufferException;
import com.ospreydcs.dp.grpc.v1.common.*;
import com.ospreydcs.dp.grpc.v1.ingestion.SubscribeDataResponse;
import com.ospreydcs.dp.grpc.v1.ingestionstream.DataEventOperation;
import com.ospreydcs.dp.grpc.v1.ingestionstream.PvConditionTrigger;
import com.ospreydcs.dp.grpc.v1.ingestionstream.SubscribeDataEventRequest;
import com.ospreydcs.dp.grpc.v1.ingestionstream.SubscribeDataEventResponse;
import com.ospreydcs.dp.service.common.config.ConfigurationManager;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import com.ospreydcs.dp.service.common.protobuf.DataTimestampsUtility;
import com.ospreydcs.dp.service.common.protobuf.TimestampUtility;
import com.ospreydcs.dp.service.ingest.utility.IngestionServiceClientUtility;
import com.ospreydcs.dp.service.ingestionstream.handler.IngestionStreamHandler;
import com.ospreydcs.dp.service.ingestionstream.handler.interfaces.IngestionStreamHandlerInterface;
import com.ospreydcs.dp.service.ingestionstream.service.IngestionStreamServiceImpl;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class EventMonitor {

    // EventMonitor constants
    private static final long DEFAULT_MAX_MESSAGE_SIZE_BYTES = 4096000L; // 4MB message size limit
    private static final String CFG_KEY_MAX_MESSAGE_SIZE_BYTES = "IngestionStreamHandler.EventMonitor.maxMessageSizeBytes";

    // DataBuffer constants
    private static final long DEFAULT_FLUSH_INTERVAL_MILLIS = 500L; // 500ms flush interval
    private static final String CFG_KEY_FLUSH_INTERVAL_MILLIS = "IngestionStreamHandler.EventMonitor.DataBuffer.flushIntervalMillis";
    private static final long DEFAULT_MAX_BUFFER_BYTES = 512 * 1024L; // 512KB max buffer size
    private static final String CFG_KEY_MAX_BUFFER_BYTES = "IngestionStreamHandler.EventMonitor.DataBuffer.maxBufferBytes";
    private static final int DEFAULT_MAX_BUFFER_ITEMS = 50; // 50 max items
    private static final String CFG_KEY_MAX_BUFFER_ITEMS = "IngestionStreamHandler.EventMonitor.DataBuffer.maxBufferItems";
    private static final long DEFAULT_BUFFER_AGE_LIMIT_NANOS = DataBuffer.DataBufferConfig.secondsToNanos(2); // 2 seconds max item age
    private static final String CFG_KEY_BUFFER_AGE_LIMIT_NANOS = "IngestionStreamHandler.EventMonitor.DataBuffer.ageLimitNanos";
    private static final long DEFAULT_BUFFER_AGE_CUSHION_NANOS = DataBuffer.DataBufferConfig.secondsToNanos(1); // 1 second cushion added to negative trigger time offset buffer age limit
    private static final String CFG_KEY_BUFFER_AGE_CUSHION_NANOS = "IngestionStreamHandler.EventMonitor.DataBuffer.ageCushionNanos";

    // TriggeredEventManager constants
    private static final long DEFAULT_EVENT_EXPIRATION_NANOS = DataBuffer.DataBufferConfig.secondsToNanos(5);
    private static final String CFG_KEY_EVENT_EXPIRATION_NANOS = "IngestionStreamHandler.EventMonitor.TriggeredEventManager.eventExpirationNanos";
    private static final long DEFAULT_EVENT_CLEANUP_INTERVAL_MILLIS = 5000L;
    private static final String CFG_KEY_EVENT_CLEANUP_INTERVAL_MILLIS = "IngestionStreamHandler.EventMonitor.TriggeredEventManager.eventCleanupIntervalMillis";

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    protected final SubscribeDataEventRequest.NewSubscription requestSubscription;
    public final StreamObserver<SubscribeDataEventResponse> responseObserver;
    private final IngestionStreamHandlerInterface handler;
    protected final Map<String, PvConditionTrigger> pvTriggerMap = new HashMap<>();
    protected final Set<String> targetPvNames = new HashSet<>();
    protected final DataBufferManager bufferManager;
    protected final TriggeredEventManager triggeredEventManager;
    protected final SubscribeDataCallManager subscribeDataCallManager;
    private final AtomicBoolean shutdownRequested = new AtomicBoolean(false);


    // configuration accessors
    protected static ConfigurationManager configMgr() {
        return ConfigurationManager.getInstance();
    }

    private long getMaxMessageSizeBytes() {
        return configMgr().getConfigLong(CFG_KEY_MAX_MESSAGE_SIZE_BYTES, DEFAULT_MAX_MESSAGE_SIZE_BYTES);
    }

    private long getFlushIntervalMillis() {
        return configMgr().getConfigLong(CFG_KEY_FLUSH_INTERVAL_MILLIS, DEFAULT_FLUSH_INTERVAL_MILLIS);
    }

    private long getMaxBufferBytes() {
        return configMgr().getConfigLong(CFG_KEY_MAX_BUFFER_BYTES, DEFAULT_MAX_BUFFER_BYTES);
    }

    private int getMaxBufferItems() {
        return configMgr().getConfigInteger(CFG_KEY_MAX_BUFFER_ITEMS, DEFAULT_MAX_BUFFER_ITEMS);
    }

    private long getBufferAgeLimitNanos() {
        return configMgr().getConfigLong(CFG_KEY_BUFFER_AGE_LIMIT_NANOS, DEFAULT_BUFFER_AGE_LIMIT_NANOS);
    }

    private long getBufferAgeCushionNanos() {
        return configMgr().getConfigLong(CFG_KEY_BUFFER_AGE_CUSHION_NANOS, DEFAULT_BUFFER_AGE_CUSHION_NANOS);
    }

    private long getEventExpirationNanos() {
        return configMgr().getConfigLong(CFG_KEY_EVENT_EXPIRATION_NANOS, DEFAULT_EVENT_EXPIRATION_NANOS);
    }

    private long getCleanupIntervalMillis() {
        return configMgr().getConfigLong(CFG_KEY_EVENT_CLEANUP_INTERVAL_MILLIS, DEFAULT_EVENT_CLEANUP_INTERVAL_MILLIS);
    }

    // local type defs
    private static record TriggerResult(
            boolean isTriggered,
            boolean isError,
            String errorMsg
    ) {}


    public EventMonitor(
            SubscribeDataEventRequest.NewSubscription requestSubscription,
            StreamObserver<SubscribeDataEventResponse> responseObserver,
            IngestionStreamHandler handler,
            IngestionServiceClientUtility.IngestionServiceGrpcClient ingestionServiceGrpcClient
    ) {
        this.requestSubscription = requestSubscription;
        this.responseObserver = responseObserver;
        this.handler = handler;
        this.subscribeDataCallManager = new SubscribeDataCallManager(this, handler, ingestionServiceGrpcClient);

        // use negative offset value from request to determine buffer data age limit (plus a cushion)
        long negativeOffset = 0L;
        if (requestSubscription.hasOperation()) {
            final DataEventOperation operation = requestSubscription.getOperation();
            if (operation.hasWindow()) {
                final DataEventOperation.DataEventWindow window = operation.getWindow();
                if (window.hasTimeInterval()) {
                    final DataEventOperation.DataEventWindow.TimeInterval interval = window.getTimeInterval();
                    if (interval.getOffset() < 0) {
                        negativeOffset = Math.abs(interval.getOffset());
                        negativeOffset = negativeOffset + getBufferAgeCushionNanos();
                    }
                }
            }
        }

        // Take the maximum of (negative offset + cushion) and the default buffer age limit to set the bufferAgeLimit.
        // Note that when the offset in the request is non-negative, the value of negativeOffset is zero, and we use
        // the default buffer age limit.
        long bufferAgeLimit = Math.max(negativeOffset, getBufferAgeLimitNanos());

        // create buffer config using age limit
        final DataBuffer.DataBufferConfig dataBufferConfig = new DataBuffer.DataBufferConfig(
                getFlushIntervalMillis(),
                getMaxBufferBytes(),
                getMaxBufferItems(),
                bufferAgeLimit);

        // Create buffer manager with callback to process data
        this.bufferManager = new DataBufferManager(this::processBufferedData, dataBufferConfig);

        // Create and start triggered event manager
        TriggeredEventManager.TriggeredEventManagerConfig eventManagerConfig =
            new TriggeredEventManager.TriggeredEventManagerConfig(
                    getEventExpirationNanos(),
                    getCleanupIntervalMillis());
        this.triggeredEventManager = new TriggeredEventManager(eventManagerConfig);
        this.triggeredEventManager.start();

        this.initialize(requestSubscription);
    }

    private void initialize(SubscribeDataEventRequest.NewSubscription request) {

        // initialize pvTriggerMap from request
        for (PvConditionTrigger trigger : request.getTriggersList()) {
            pvTriggerMap.put(trigger.getPvName(), trigger);
        }

        // initialize targetPvNames from request
        targetPvNames.addAll(request.getOperation().getTargetPvsList());
    }

    public void handleReject(String errorMsg) {

        if (!safeToSendResponse()) {
            return;
        }

        logger.debug("handleReject msg: {}", errorMsg);

        // dispatch reject message but don't close response stream with onCompleted()
        IngestionStreamServiceImpl.sendSubscribeDataEventResponseReject(errorMsg, responseObserver);

        initiateShutdown();
    }

    public void handleError(
            String errorMsg
    ) {
        if (!safeToSendResponse()) {
            return;
        }

        logger.debug("handleError msg: {}", errorMsg);

        // dispatch error message but don't close response stream with onCompleted()
        IngestionStreamServiceImpl.sendSubscribeDataEventResponseError(errorMsg, responseObserver);

        initiateShutdown();
    }

    private boolean safeToSendResponse() {
        if (shutdownRequested.get()) {
            return false;
        }
        
        ServerCallStreamObserver<SubscribeDataEventResponse> serverCallStreamObserver =
                (ServerCallStreamObserver<SubscribeDataEventResponse>) responseObserver;
        return !serverCallStreamObserver.isCancelled();
    }

    public void initiateShutdown() {
        // remove monitor from manager
        handler.terminateEventMonitor(this);
    }

    private Set<String> triggerPvNames() {
        return pvTriggerMap.keySet();
    }

    private Set<String> targetPvNames() {
        return targetPvNames;
    }

    public Set<String> getPvNames() {
        final Set<String> pvNames = new HashSet<>();
        // add names for trigger PVs
        pvNames.addAll(triggerPvNames());
        // add names for target PVs
        pvNames.addAll(targetPvNames());
        return pvNames;
    }

    public ResultStatus initiateSubscription() {
        return subscribeDataCallManager.initiateSubscription();
    }

    public void handleSubscribeDataResponse(SubscribeDataResponse subscribeDataResponse) {
        // Early exit if shutdown requested or stream closed
        if (!safeToSendResponse()) {
            return;
        }
        
        logger.debug(
                "handleSubscribeDataResponse type: {} id: {}",
                subscribeDataResponse.getResultCase().name(),
                this.hashCode());

        switch (subscribeDataResponse.getResultCase()) {
            case EXCEPTIONALRESULT -> {
                logger.debug("received exceptional result");
                handleExceptionalResult(subscribeDataResponse.getExceptionalResult());
            }
            case ACKRESULT -> {
                logger.trace("received ack result");
                // nothing to do
            }
            case SUBSCRIBEDATARESULT -> {
                logger.trace("received subscribeData result");
                handleSubscribeDataResult(subscribeDataResponse.getSubscribeDataResult());
            }
            case RESULT_NOT_SET -> {
                logger.trace("received result not set");
                handleError("result not set in SubscribeDataResponse");
            }
        }
    }

    public void handleExceptionalResult(ExceptionalResult exceptionalResult) {
        handleError(exceptionalResult.getMessage());
    }

    private static <T extends Comparable<T>> TriggerResult checkTrigger(
            T typedDataValue,
            T typedTriggerValue,
            PvConditionTrigger.PvCondition triggerCondition
    ) {
        final int compareResult = typedDataValue.compareTo(typedTriggerValue);

        switch (triggerCondition) {
            case PV_CONDITION_UNSPECIFIED -> {
                final String errorMsg = "PvConditionTrigger.condition must be specified";
                return new TriggerResult(false, true, errorMsg);
            }
            case PV_CONDITION_EQUAL_TO -> {
                return new TriggerResult(compareResult == 0, false, "");
            }
            case PV_CONDITION_GREATER -> {
                return new TriggerResult(compareResult > 0, false, "");
            }
            case PV_CONDITION_GREATER_EQ -> {
                return new TriggerResult(compareResult >= 0, false, "");
            }
            case PV_CONDITION_LESS -> {
                return new TriggerResult(compareResult < 0, false, "");
            }
            case PV_CONDITION_LESS_EQ -> {
                return new TriggerResult(compareResult <= 0, false, "");
            }
            case UNRECOGNIZED -> {
                final String errorMsg = "PvConditionTrigger.condition unrecognized enum value";
                return new TriggerResult(false, true, errorMsg);
            }
        }

        final String errorMsg = "PvConditionTrigger.condition unhandled condition: " + triggerCondition;
        return new TriggerResult(false, true, errorMsg);
    }

    private void handleTriggerPVData(
            PvConditionTrigger trigger,
            DataColumn dataColumn,
            DataTimestamps dataTimestamps
    ) {
        final String columnPvName = dataColumn.getName();

        // check if each column data value triggers the event
        int columnValueIndex = 0;
        for (DataValue dataValue : dataColumn.getDataValuesList()) {

            final PvConditionTrigger.PvCondition triggerCondition = trigger.getCondition();
            final DataValue triggerValue = trigger.getValue();

            // check for type mismatch between column data value and trigger value
            if (dataValue.getValueCase() != triggerValue.getValueCase()) {
                final String errorMsg = "PvConditionTrigger type mismatch PV name: " + columnPvName
                        + " PV data type: " + dataValue.getValueCase().name()
                        + " trigger value data type: " + triggerValue.getValueCase().name();
                handleError(errorMsg);
                return;
            }

            // check if event condition is triggered by data value
            boolean isTriggered = false;
            boolean isError = false;
            String resultErrorMsg = "";
            TriggerResult triggerResult = null;
            switch (dataValue.getValueCase()) {
                
                case STRINGVALUE -> {
                    final String typedDataValue = dataValue.getStringValue();
                    final String typedTriggerValue = triggerValue.getStringValue();
                    triggerResult = checkTrigger(typedDataValue, typedTriggerValue, triggerCondition);
                }
                
                case BOOLEANVALUE -> {
                    final Boolean typedDataValue = dataValue.getBooleanValue();
                    final Boolean typedTriggerValue = triggerValue.getBooleanValue();
                    triggerResult = checkTrigger(typedDataValue, typedTriggerValue, triggerCondition);
                }
                
                case UINTVALUE -> {
                    final int typedDataValue = dataValue.getUintValue();
                    final int typedTriggerValue = triggerValue.getUintValue();
                    triggerResult = checkTrigger(typedDataValue, typedTriggerValue, triggerCondition);
                }
                
                case ULONGVALUE -> {
                    final long typedDataValue = dataValue.getUlongValue();
                    final long typedTriggerValue = triggerValue.getUlongValue();
                    triggerResult = checkTrigger(typedDataValue, typedTriggerValue, triggerCondition);
                }
                
                case INTVALUE -> {
                    final int typedDataValue = dataValue.getIntValue();
                    final int typedTriggerValue = triggerValue.getIntValue();
                    triggerResult = checkTrigger(typedDataValue, typedTriggerValue, triggerCondition);
                }

                case LONGVALUE -> {
                    final long typedDataValue = dataValue.getLongValue();
                    final long typedTriggerValue = triggerValue.getLongValue();
                    triggerResult = checkTrigger(typedDataValue, typedTriggerValue, triggerCondition);
                }
                
                case FLOATVALUE -> {
                    final float typedDataValue = dataValue.getFloatValue();
                    final float typedTriggerValue = triggerValue.getFloatValue();
                    triggerResult = checkTrigger(typedDataValue, typedTriggerValue, triggerCondition);
                }

                case DOUBLEVALUE -> {
                    final double typedDataValue = dataValue.getDoubleValue();
                    final double typedTriggerValue = triggerValue.getDoubleValue();
                    triggerResult = checkTrigger(typedDataValue, typedTriggerValue, triggerCondition);
                }

                case TIMESTAMPVALUE -> {
                    final Instant dataValueInstant =
                            TimestampUtility.instantFromTimestamp(dataValue.getTimestampValue());
                    final Instant triggerValueInstant =
                            TimestampUtility.instantFromTimestamp(triggerValue.getTimestampValue());
                    triggerResult = checkTrigger(dataValueInstant, triggerValueInstant, triggerCondition);
                }

                case BYTEARRAYVALUE, ARRAYVALUE, STRUCTUREVALUE, IMAGEVALUE -> {
                    resultErrorMsg = "PvConditionTrigger PV data type not supported: " + columnPvName
                            + " PV data type: " + dataValue.getValueCase().name();
                    isError = true;
                }

                case VALUE_NOT_SET -> {
                    resultErrorMsg = "PvConditionTrigger PV data type not specified: " + columnPvName;
                    isError = true;
                }

            }

            if (triggerResult != null) {
                isTriggered = triggerResult.isTriggered();
                isError = triggerResult.isError();
                resultErrorMsg = triggerResult.errorMsg();
            }

            if (isError) {
                final String errorMsg =
                        "PvConditionTrigger error comparing data value for PV name: " + columnPvName
                                + " msg: " + resultErrorMsg;
                handleError(errorMsg);
                return;
            }

            if (isTriggered) {
                final Timestamp triggerTimestamp =
                        DataTimestampsUtility.timestampForIndex(dataTimestamps, columnValueIndex);
                if (triggerTimestamp == null) {
                    final String errorMsg = "PvConditionTrigger error getting timestamp for PV: " + columnPvName;
                    handleError(errorMsg);
                }
                handleTriggeredEvent(triggerTimestamp, trigger, dataValue);
            }

            columnValueIndex = columnValueIndex + 1;
        }
    }

    private void handleTriggeredEvent(
            Timestamp triggerTimestamp,
            PvConditionTrigger trigger,
            DataValue dataValue
    ) {
        if (!safeToSendResponse()) {
            return;
        }

        // only add an event to triggered event list if the request includes a DataOperation
        if (requestSubscription.hasOperation()) {
            final TriggeredEvent triggeredEvent = new TriggeredEvent(
                triggerTimestamp, 
                trigger, 
                requestSubscription.getOperation(), 
                dataValue,
                triggeredEventManager.getConfig().getExpirationDelayNanos()
            );
            this.triggeredEventManager.addTriggeredEvent(triggeredEvent);
        }

        // send an event message in the response stream
        IngestionStreamServiceImpl.sendSubscribeDataEventResponseEvent(
                triggerTimestamp,
                trigger,
                dataValue,
                this.responseObserver);
    }

    private void handleTargetPvData(
            String pvName,
            List<DataBuffer.BufferedData> bufferedDataList
    ) {
        if (bufferedDataList.isEmpty()) {
            return;
        }

        // iterate through each triggered event, dispatching messages in the response stream for data targeting that event
        List<TriggeredEvent> activeEvents = this.triggeredEventManager.getActiveEvents();
        for (TriggeredEvent triggeredEvent : activeEvents) {

            final List<DataBucket> currentDataBuckets = new ArrayList<>();
            long currentMessageSize = 0;
            final long baseMessageOverhead = 200; // Base overhead for EventData message structure

            // iterate through each data item, check if the data time targets the triggeredEvent
            for (DataBuffer.BufferedData bufferedData : bufferedDataList) {

                // only dispatch bufferedData in response stream if the data time targets this TriggeredEvent
                if ( ! triggeredEvent.isTargetedByData(bufferedData)) {
                    continue;
                }

                // Create DataBucket for this BufferedData item
                final DataBucket.Builder dataBucketBuilder = DataBucket.newBuilder();
                dataBucketBuilder.setDataTimestamps(bufferedData.getDataTimestamps());
                if (bufferedData.getDataColumn() != null) {
                    dataBucketBuilder.setDataColumn(bufferedData.getDataColumn());
                }
                if (bufferedData.getSerializedDataColumn() != null) {
                    dataBucketBuilder.setSerializedDataColumn(bufferedData.getSerializedDataColumn());
                }
                final DataBucket dataBucket = dataBucketBuilder.build();

                final long bucketSize = bufferedData.getEstimatedSize();

                // Check if adding this bucket would exceed message size limit
                if (!currentDataBuckets.isEmpty() &&
                        (currentMessageSize + bucketSize + baseMessageOverhead) > getMaxMessageSizeBytes()) {

                    // Send current batch and start a new one
                    sendDataEventMessage(triggeredEvent.getEvent(), currentDataBuckets);
                    currentDataBuckets.clear();
                    currentMessageSize = 0;
                }

                currentDataBuckets.add(dataBucket);
                currentMessageSize += bucketSize;

                logger.debug("Added DataBucket for PV: {}, current message size: {} bytes, {} buckets",
                        pvName, currentMessageSize, currentDataBuckets.size());
            }

            // Send any remaining data buckets
            if (!currentDataBuckets.isEmpty()) {
                sendDataEventMessage(triggeredEvent.getEvent(), currentDataBuckets);
            }
        }
    }

    private void sendDataEventMessage(
            SubscribeDataEventResponse.Event event,
            List<DataBucket> dataBuckets
    ) {
        if (!safeToSendResponse()) {
            return;
        }

        if (dataBuckets.isEmpty()) {
            logger.debug("sendDataEventMessage received empty dataBuckets list");
            return;
        }

        // Create EventData with TriggeredEvent placeholder and DataBuckets
        SubscribeDataEventResponse.EventData eventData = SubscribeDataEventResponse.EventData.newBuilder()
                .setEvent(event)
                .addAllDataBuckets(dataBuckets)
                .build();

        IngestionStreamServiceImpl.sendSubscribeDataEventResponseEventData(eventData, responseObserver);

        logger.debug("Sent EventData message with {} data buckets", dataBuckets.size());
    }

    private void handleSubscribeDataResult(SubscribeDataResponse.SubscribeDataResult result) {

        // Handle each DataColumn from result.  A PV might be treated as both a trigger and target PV.
        for (DataColumn dataColumn : result.getDataColumnsList()) {
            final String columnName = dataColumn.getName();

            // handle trigger PVs immediately
            final PvConditionTrigger pvConditionTrigger = pvTriggerMap.get(columnName);
            if (pvConditionTrigger != null) {
                handleTriggerPVData(pvConditionTrigger, dataColumn, result.getDataTimestamps());
            }

            // Buffer the data for target PVs instead of processing immediately
            if (targetPvNames().contains(columnName)) {
                bufferManager.bufferData(columnName, dataColumn, result.getDataTimestamps());
            }
        }

        // Handle each SerializedDataColumn from result.  A PV might be treated as both a trigger and target PV.
        for (SerializedDataColumn serializedDataColumn : result.getSerializedDataColumnsList()) {
            final String columnName = serializedDataColumn.getName();

            // handle trigger PVs immediately
            final PvConditionTrigger pvConditionTrigger = pvTriggerMap.get(columnName);
            if (pvConditionTrigger != null) {
                final DataColumn dataColumn;
                try {
                    dataColumn = DataColumn.parseFrom(serializedDataColumn.getPayload());
                } catch (InvalidProtocolBufferException e) {
                    final String errorMsg = "InvalidProtocolBufferException msg: " + e.getMessage();
                    logger.error(errorMsg + " id: " + responseObserver.hashCode());
                    handleError(errorMsg);
                    return;
                }
                handleTriggerPVData(pvConditionTrigger, dataColumn, result.getDataTimestamps());
            }

            // Buffer the data for target PVs instead of processing immediately.
            if (targetPvNames().contains(columnName)) {
                bufferManager.bufferSerializedData(columnName, serializedDataColumn, result.getDataTimestamps());
            }
        }
    }

    private void processBufferedData(String pvName, List<DataBuffer.BufferedData> results) {

    if (targetPvNames().contains(pvName)) {
            // handle target PVs
            handleTargetPvData(pvName, results);

        } else {
            // this shouldn't happen, indicates we subscribed the EventMonitor to the wrong PVs...
            final String errorMsg = "unexpected PV received by EventMonitor: " + pvName;
            handleError(errorMsg);
        }
    }

    public void requestShutdown() {

        // use AtomicBoolean flag to control cancel, we only need one caller thread cleaning things up
        if (shutdownRequested.compareAndSet(false, true)) {

            logger.debug("requestShutdown id: {}", responseObserver.hashCode());

            // terminate subscribeData() subscription
            subscribeDataCallManager.terminateSubscription();

            // shutdown DataBufferManager
            if (bufferManager != null) {
                bufferManager.shutdown();
            }

            // shutdown TriggeredEventManager
            if (triggeredEventManager != null) {
                triggeredEventManager.shutdown();
            }

            // close API response stream
            logger.debug("closing subscribeData() API subscription id: {}", responseObserver.hashCode());
            ServerCallStreamObserver<SubscribeDataEventResponse> serverCallStreamObserver =
                    (ServerCallStreamObserver<SubscribeDataEventResponse>) responseObserver;
            if (!serverCallStreamObserver.isCancelled()) {
                logger.debug(
                        "requestShutdown() calling responseObserver.onCompleted id: {}",
                        responseObserver.hashCode());
                responseObserver.onCompleted();
            } else {
                logger.debug(
                        "requestShutdown() responseObserver already closed id: {}",
                        responseObserver.hashCode());
            }
        }
    }

}
