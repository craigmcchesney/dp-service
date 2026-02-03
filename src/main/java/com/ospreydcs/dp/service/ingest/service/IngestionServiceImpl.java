package com.ospreydcs.dp.service.ingest.service;

import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataTimestamps;
import com.ospreydcs.dp.grpc.v1.common.ExceptionalResult;
import com.ospreydcs.dp.grpc.v1.common.SerializedDataColumn;
import com.ospreydcs.dp.grpc.v1.ingestion.*;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest.IngestionDataFrame;
import com.ospreydcs.dp.service.common.protobuf.TimestampUtility;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import com.ospreydcs.dp.service.ingest.handler.IngestionValidationUtility;
import com.ospreydcs.dp.service.ingest.handler.model.HandlerIngestionRequest;
import com.ospreydcs.dp.service.ingest.handler.interfaces.IngestionHandlerInterface;
import com.ospreydcs.dp.service.ingest.service.request.IngestDataBidiStreamRequestObserver;
import com.ospreydcs.dp.service.ingest.service.request.IngestDataStreamRequestObserver;
import com.ospreydcs.dp.service.ingest.service.request.SubscribeDataRequestObserver;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class IngestionServiceImpl extends DpIngestionServiceGrpc.DpIngestionServiceImplBase {

    private static final Logger logger = LogManager.getLogger();

    private IngestionHandlerInterface handler;

    public boolean init(IngestionHandlerInterface handler) {
        this.handler = handler;
        if (!handler.init()) {
            logger.error("handler.init failed");
            return false;
        }
        if (!handler.start()) {
            logger.error("handler.start failed");
        }
        return true;
    }

    public void fini() {
        if (handler != null) {
            handler.stop();
            handler.fini();
            handler = null;
        }
    }

    public static int getNumRequestRows(IngestDataRequest request) {
        int numRequestValues = 0;
        switch (request.getIngestionDataFrame().getDataTimestamps().getValueCase()) {
            case SAMPLINGCLOCK -> {
                numRequestValues =
                        request.getIngestionDataFrame().getDataTimestamps().getSamplingClock().getCount();
            }
            case TIMESTAMPLIST -> {
                numRequestValues =
                        request.getIngestionDataFrame().getDataTimestamps().getTimestampList().getTimestampsCount();
            }
            case VALUE_NOT_SET -> {
                numRequestValues = 0;
            }
        }
        return numRequestValues;
    }

    public static IngestDataResponse ingestionResponseReject(
            IngestDataRequest request, String msg) {

        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT)
                .setMessage(msg)
                .build();
        final IngestDataResponse response = IngestDataResponse.newBuilder()
                .setProviderId(request.getProviderId())
                .setClientRequestId(request.getClientRequestId())
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();
        return response;
    }

    public static IngestDataStreamResponse ingestDataStreamResponseStreamReject(
            IngestDataRequest request, String msg) {

        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT)
                .setMessage(msg)
                .build();
        final IngestDataStreamResponse response = IngestDataStreamResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();
        return response;
    }

    public static IngestDataResponse ingestionResponseAck(IngestDataRequest request) {

        final int numRows = getNumRequestRows(request);

        IngestionDataFrame frame = request.getIngestionDataFrame();
        int numColumns = frame.getDataColumnsCount()
                + frame.getSerializedDataColumnsCount()
                + frame.getDoubleColumnsCount()
                + frame.getFloatColumnsCount()
                + frame.getInt64ColumnsCount()
                + frame.getInt32ColumnsCount()
                + frame.getBoolColumnsCount()
                + frame.getStringColumnsCount()
                + frame.getEnumColumnsCount()
                + frame.getImageColumnsCount()
                + frame.getStructColumnsCount()
                + frame.getDoubleArrayColumnsCount()
                + frame.getFloatArrayColumnsCount()
                + frame.getInt32ArrayColumnsCount()
                + frame.getInt64ArrayColumnsCount()
                + frame.getBoolArrayColumnsCount();

        final IngestDataResponse.AckResult ackResult = IngestDataResponse.AckResult.newBuilder()
                .setNumRows(numRows)
                .setNumColumns(numColumns)
                .build();
        final IngestDataResponse response = IngestDataResponse.newBuilder()
                .setProviderId(request.getProviderId())
                .setClientRequestId(request.getClientRequestId())
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setAckResult(ackResult)
                .build();
        return response;
    }

    private static RegisterProviderResponse registerProviderResponseExceptionalResult(
            String msg, ExceptionalResult.ExceptionalResultStatus status
    ) {
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(status)
                .setMessage(msg)
                .build();

        final RegisterProviderResponse response = RegisterProviderResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();

        return response;
    }

    private static RegisterProviderResponse registerProviderResponseReject(String msg) {

        return registerProviderResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT);
    }

    private static RegisterProviderResponse registerProviderResponseError(String msg) {

        return registerProviderResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR);
    }

    private static RegisterProviderResponse registerProviderResponseSuccess(
            String providerName,
            String providerId,
            boolean isNewProvider
    ) {
        final RegisterProviderResponse.RegistrationResult registrationResult =
                RegisterProviderResponse.RegistrationResult.newBuilder()
                        .setProviderName(providerName)
                        .setProviderId(providerId)
                        .setIsNewProvider(isNewProvider)
                        .build();

        final RegisterProviderResponse response = RegisterProviderResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setRegistrationResult(registrationResult)
                .build();

        return response;
    }

    public static void sendRegisterProviderResponseReject(
            String msg, StreamObserver<RegisterProviderResponse> responseObserver
    ) {
        final RegisterProviderResponse response = registerProviderResponseReject(msg);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public static void sendRegisterProviderResponseError(
            String msg, StreamObserver<RegisterProviderResponse> responseObserver
    ) {
        final RegisterProviderResponse response = registerProviderResponseError(msg);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public static void sendRegisterProviderResponseSuccess(
            String providerName,
            String providerId,
            boolean isNewProvider,
            StreamObserver<RegisterProviderResponse> responseObserver
    ) {
        final RegisterProviderResponse response
                = registerProviderResponseSuccess(providerName, providerId, isNewProvider);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void registerProvider(
            RegisterProviderRequest request,
            StreamObserver<RegisterProviderResponse> responseObserver
    ) {
        logger.info(
                "id: {} registerProvider request received, provider: {}",
                responseObserver.hashCode(),
                request.getProviderName());

        if (handler.getShutdownRequested()) {
            sendRegisterProviderResponseReject("service is shutdown", responseObserver);
            return;
        }

        // validate request
        if (request.getProviderName().isBlank()) {
            final String errorMsg = "RegisterProviderRequest.providerName must be specified";
            sendRegisterProviderResponseReject(errorMsg, responseObserver);
            return;
        }

        // handle request
        handler.handleRegisterProvider(request, responseObserver);
    }

    @Override
    public void ingestData(IngestDataRequest request, StreamObserver<IngestDataResponse> responseObserver) {

        logger.debug(
                "ingestData providerId: {} requestId: {}",
                request.getProviderId(), request.getClientRequestId());

        // handle ingestion request
        handleIngestionRequest(request, responseObserver);

        // close response stream, this is a unary single-response rpc
        responseObserver.onCompleted();
    }

    @Override
    public StreamObserver<IngestDataRequest> ingestDataStream(StreamObserver<IngestDataStreamResponse> responseObserver) {
        logger.debug("ingestDataStream");
        return new IngestDataStreamRequestObserver(responseObserver, handler, this);
    }

    @Override
    public StreamObserver<IngestDataRequest> ingestDataBidiStream(StreamObserver<IngestDataResponse> responseObserver) {
        logger.debug("ingestDataBidiStream");
        return new IngestDataBidiStreamRequestObserver(responseObserver, handler, this);
    }
    
    public void handleIngestionRequest(
            IngestDataRequest request,
            StreamObserver<IngestDataResponse> responseObserver
    ) {
        if (handler.getShutdownRequested()) {
            final IngestDataResponse rejectResponse = ingestionResponseReject(request, "service is shutdown");
            responseObserver.onNext(rejectResponse);
            responseObserver.onCompleted();
            return;
        }

        // validate request, send error response for invalid request
        final ResultStatus resultStatus = IngestionValidationUtility.validateIngestionRequest(request);
        boolean validationError = false;
        String validationMsg = "";

        if (resultStatus.isError) {
            // send error reject
            validationError = true;
            validationMsg = resultStatus.msg;
            final IngestDataResponse rejectResponse = ingestionResponseReject(request, validationMsg);
            responseObserver.onNext(rejectResponse);

        } else {
            // send ack response
            final IngestDataResponse ackResponse = ingestionResponseAck(request);
            responseObserver.onNext(ackResponse);
        }

        // handle the request, even if rejected
        final HandlerIngestionRequest handlerIngestionRequest =
                new HandlerIngestionRequest(request, validationError, validationMsg);
        handler.handleIngestionRequest(handlerIngestionRequest);
    }

    public void sendIngestDataStreamResponse(
            StreamObserver<IngestDataStreamResponse> responseObserver,
            List<String> requestIdList,
            List<String> rejectedIdList
    ) {
        // build response object
        IngestDataStreamResponse.Builder responseBuilder = IngestDataStreamResponse.newBuilder();
        responseBuilder.setResponseTime(TimestampUtility.getTimestampNow());
        responseBuilder.addAllClientRequestIds(requestIdList);
        responseBuilder.addAllRejectedRequestIds(rejectedIdList);

        if (rejectedIdList.size() > 0) {
            // send ExceptionalResult payload indicating failure
            ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                    .setExceptionalResultStatus(ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT)
                    .setMessage("one or more requests were rejected")
                    .build();
            responseBuilder.setExceptionalResult(exceptionalResult);

        } else {
            // send IngestDataStreamResult payload indicating success
            IngestDataStreamResponse.IngestDataStreamResult successfulResult =
                    IngestDataStreamResponse.IngestDataStreamResult.newBuilder()
                            .setNumRequests(requestIdList.size())
                            .build();
            responseBuilder.setIngestDataStreamResult(successfulResult);
        }

        IngestDataStreamResponse response = responseBuilder.build();
        responseObserver.onNext(response);
    }

    private static QueryRequestStatusResponse queryRequestStatusResponseExceptionalResult(
            String msg, ExceptionalResult.ExceptionalResultStatus status
    ) {
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(status)
                .setMessage(msg)
                .build();

        final QueryRequestStatusResponse response = QueryRequestStatusResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();

        return response;
    }

    private static QueryRequestStatusResponse queryRequestStatusResponseReject(String msg
    ) {
        return queryRequestStatusResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT);
    }

    private static QueryRequestStatusResponse queryRequestStatusResponseError(String msg
    ) {
        return queryRequestStatusResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR);
    }

    public static QueryRequestStatusResponse queryRequestStatusResponse(
            QueryRequestStatusResponse.RequestStatusResult result
    ) {
        return QueryRequestStatusResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setRequestStatusResult(result)
                .build();
    }

    public static void sendQueryRequestStatusResponseReject(
            String msg, StreamObserver<QueryRequestStatusResponse> responseObserver
    ) {
        final QueryRequestStatusResponse response = queryRequestStatusResponseReject(msg);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public static void sendQueryRequestStatusResponseError(
            String msg, StreamObserver<QueryRequestStatusResponse> responseObserver
    ) {
        final QueryRequestStatusResponse response = queryRequestStatusResponseError(msg);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public static void sendQueryRequestStatusResponse(
            QueryRequestStatusResponse.RequestStatusResult requestStatusResult,
            StreamObserver<QueryRequestStatusResponse> responseObserver
    ) {
        final QueryRequestStatusResponse response  = queryRequestStatusResponse(requestStatusResult);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void queryRequestStatus(
            QueryRequestStatusRequest request,
            StreamObserver<QueryRequestStatusResponse> responseObserver
    ) {
        logger.info("id: {} queryRequestStatus request received", responseObserver.hashCode());

        if (handler.getShutdownRequested()) {
            sendQueryRequestStatusResponseReject("service is shutdown", responseObserver);
            return;
        }

        // check that request contains non-empty list of criteria
        final List<QueryRequestStatusRequest.QueryRequestStatusCriterion> criterionList = request.getCriteriaList();
        if (criterionList.size() == 0) {
            final String errorMsg = "QueryRequestStatusRequest.criteria list must not be empty";
            sendQueryRequestStatusResponseReject(errorMsg, responseObserver);
        }

        // validate query criteria
        for (QueryRequestStatusRequest.QueryRequestStatusCriterion criterion : criterionList) {

            switch (criterion.getCriterionCase()) {

                case PROVIDERIDCRITERION -> {
                    final QueryRequestStatusRequest.QueryRequestStatusCriterion.ProviderIdCriterion providerIdCriterion = 
                            criterion.getProviderIdCriterion();
                    if (providerIdCriterion.getProviderId().isBlank()) {
                        final String errorMsg = "QueryRequestStatusRequest.ProviderIdCriterion.providerId must be greater than 0";
                        sendQueryRequestStatusResponseReject(errorMsg, responseObserver);
                        return;
                    }
                }
                
                case PROVIDERNAMECRITERION -> {
                    final QueryRequestStatusRequest.QueryRequestStatusCriterion.ProviderNameCriterion providerNameCriterion =
                            criterion.getProviderNameCriterion();
                    if (providerNameCriterion.getProviderName().isBlank()) {
                        final String errorMsg = "QueryRequestStatusRequest.ProviderNameCriterion.providerName must not be blank";
                        sendQueryRequestStatusResponseReject(errorMsg, responseObserver);
                        return;
                    }
                }
                
                case REQUESTIDCRITERION -> {
                    final QueryRequestStatusRequest.QueryRequestStatusCriterion.RequestIdCriterion requestIdCriterion =
                            criterion.getRequestIdCriterion();
                    if (requestIdCriterion.getRequestId().isBlank()) {
                        final String errorMsg = "QueryRequestStatusRequest.RequestIdCriterion.requestId must not be blank";
                        sendQueryRequestStatusResponseReject(errorMsg, responseObserver);
                        return;
                    }
                }
                
                case STATUSCRITERION -> {
                    final QueryRequestStatusRequest.QueryRequestStatusCriterion.StatusCriterion statusCriterion =
                            criterion.getStatusCriterion();
                    if (statusCriterion.getStatusList().size() == 0) {
                        final String errorMsg = "QueryRequestStatusRequest.StatusCriterion.status must not be empty";
                        sendQueryRequestStatusResponseReject(errorMsg, responseObserver);
                        return;
                    }
                }
                
                case TIMERANGECRITERION -> {
                    final QueryRequestStatusRequest.QueryRequestStatusCriterion.TimeRangeCriterion timeRangeCriterion =
                            criterion.getTimeRangeCriterion();
                    if (timeRangeCriterion.getBeginTime().getEpochSeconds() < 1) {
                        final String errorMsg = "QueryRequestStatusRequest.TimeRangeCriterion.beginTime seconds must be greater than 0";
                        sendQueryRequestStatusResponseReject(errorMsg, responseObserver);
                        return;
                    }
                }
                
                case CRITERION_NOT_SET -> {
                    final String errorMsg = "QueryRequestStatusRequest.criterion is not set";
                    sendQueryRequestStatusResponseReject(errorMsg, responseObserver);
                    return;
                }
            }
        }

        handler.handleQueryRequestStatus(request, responseObserver);
    }

    private static SubscribeDataResponse subscribeDataResponseExceptionalResult(
            String msg, ExceptionalResult.ExceptionalResultStatus status
    ) {
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(status)
                .setMessage(msg)
                .build();

        final SubscribeDataResponse response = SubscribeDataResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();

        return response;
    }

    private static SubscribeDataResponse subscribeDataResponseReject(String msg) {

        return subscribeDataResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT);
    }

    private static SubscribeDataResponse subscribeDataResponseError(String msg) {

        return subscribeDataResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR);
    }

    private static SubscribeDataResponse subscribeDataResponseAck(
    ) {
        final SubscribeDataResponse.AckResult result =
                SubscribeDataResponse.AckResult.newBuilder()
                        .build();

        final SubscribeDataResponse response = SubscribeDataResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setAckResult(result)
                .build();

        return response;
    }

    private static SubscribeDataResponse subscribeDataResponse(SubscribeDataResponse.SubscribeDataResult result) {

        final SubscribeDataResponse response = SubscribeDataResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setSubscribeDataResult(result)
                .build();

        return response;
    }

    private static SubscribeDataResponse subscribeDataResponse(
            DataTimestamps dataTimestamps,
            List<DataColumn> dataColumns
    ) {
        final SubscribeDataResponse.SubscribeDataResult result =
                SubscribeDataResponse.SubscribeDataResult.newBuilder()
                        .setDataTimestamps(dataTimestamps)
                        .addAllDataColumns(dataColumns)
                        .build();
        return subscribeDataResponse(result);
    }

    private static SubscribeDataResponse subscribeDataResponseSerializedColumns(
            DataTimestamps dataTimestamps,
            List<SerializedDataColumn> serializedDataColumns
    ) {
        final SubscribeDataResponse.SubscribeDataResult result =
                SubscribeDataResponse.SubscribeDataResult.newBuilder()
                        .setDataTimestamps(dataTimestamps)
                        .addAllSerializedDataColumns(serializedDataColumns)
                        .build();
        return subscribeDataResponse(result);
    }

    public static void sendSubscribeDataResponseReject(
            String msg, StreamObserver<SubscribeDataResponse> responseObserver
    ) {
        final SubscribeDataResponse response = subscribeDataResponseReject(msg);
        responseObserver.onNext(response);
    }

    public static void sendSubscribeDataResponseError(
            String msg, StreamObserver<SubscribeDataResponse> responseObserver
    ) {
        final SubscribeDataResponse response = subscribeDataResponseError(msg);
        responseObserver.onNext(response);
    }

    public static void sendSubscribeDataResponseAck(
            StreamObserver<SubscribeDataResponse> responseObserver
    ) {
        final SubscribeDataResponse response = subscribeDataResponseAck();
        responseObserver.onNext(response);
    }

    public static void sendSubscribeDataResponse(
            DataTimestamps dataTimestamps,
            List<DataColumn> dataColumns,
            StreamObserver<SubscribeDataResponse> responseObserver
    ) {
        final SubscribeDataResponse response
                = subscribeDataResponse(dataTimestamps, dataColumns);
        responseObserver.onNext(response);
    }

    public static void sendSubscribeDataResponseSerializedColumns(
            DataTimestamps dataTimestamps,
            List<SerializedDataColumn> serializedDataColumns,
            StreamObserver<SubscribeDataResponse> responseObserver
    ) {
        final SubscribeDataResponse response
                = subscribeDataResponseSerializedColumns(dataTimestamps, serializedDataColumns);
        responseObserver.onNext(response);
    }

    @Override
    public StreamObserver<SubscribeDataRequest> subscribeData(
            StreamObserver<SubscribeDataResponse> responseObserver
    ) {
        return new SubscribeDataRequestObserver(responseObserver, handler);
    }

}
