package com.ospreydcs.dp.service.annotation.service;

import com.ospreydcs.dp.grpc.v1.annotation.*;
import com.ospreydcs.dp.grpc.v1.common.ExceptionalResult;
import com.ospreydcs.dp.service.annotation.handler.AnnotationValidationUtility;
import com.ospreydcs.dp.service.annotation.handler.interfaces.AnnotationHandlerInterface;
import com.ospreydcs.dp.service.annotation.handler.model.HandlerExportDataRequest;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import com.ospreydcs.dp.service.common.protobuf.TimestampUtility;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class AnnotationServiceImpl extends DpAnnotationServiceGrpc.DpAnnotationServiceImplBase {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // instance variables
    private AnnotationHandlerInterface handler;

    public boolean init(AnnotationHandlerInterface handler) {
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

    private static SaveDataSetResponse saveDataSetResponseReject(String msg) {

        final ExceptionalResult exceptionalResult =
                ExceptionalResult.newBuilder()
                        .setExceptionalResultStatus(
                                ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT)
                        .setMessage(msg)
                        .build();

        final SaveDataSetResponse response = SaveDataSetResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();

        return response;
    }

    public static void sendSaveDataSetResponseReject(
            String errorMsg,
            StreamObserver<SaveDataSetResponse> responseObserver
    ) {
        final SaveDataSetResponse response = saveDataSetResponseReject(errorMsg);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private static SaveDataSetResponse saveDataSetResponseError(String msg) {

        final ExceptionalResult exceptionalResult =
                ExceptionalResult.newBuilder()
                        .setExceptionalResultStatus(ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR)
                        .setMessage(msg)
                        .build();

        final SaveDataSetResponse response = SaveDataSetResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();

        return response;
    }

    public static void sendSaveDataSetResponseError(
            String errorMsg, StreamObserver<SaveDataSetResponse> responseObserver
    ) {
        final SaveDataSetResponse response = saveDataSetResponseError(errorMsg);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private static SaveDataSetResponse saveDataSetResponseSuccess(String dataSetId) {

        final SaveDataSetResponse.SaveDataSetResult result =
                SaveDataSetResponse.SaveDataSetResult.newBuilder()
                        .setDataSetId(dataSetId)
                        .build();

        final SaveDataSetResponse response = SaveDataSetResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setSaveDataSetResult(result)
                .build();

        return response;
    }

    public static void sendSaveDataSetResponseSuccess(
            String dataSetId, StreamObserver<SaveDataSetResponse> responseObserver
    ) {
        final SaveDataSetResponse response = saveDataSetResponseSuccess(dataSetId);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void saveDataSet(
            SaveDataSetRequest request,
            StreamObserver<SaveDataSetResponse> responseObserver
    ) {
        logger.info("id: {} saveDataSet request received", responseObserver.hashCode());

        final DataSet dataSet = request.getDataSet();
        if (dataSet == null) {
            final String errorMsg = "SaveDataSetRequest.dataSet must be specified";
            sendSaveDataSetResponseReject(errorMsg, responseObserver);
        }

        // validate DataSet
        ResultStatus resultStatus = AnnotationValidationUtility.validateDataSet(dataSet);
        if (resultStatus.isError) {
            logger.debug("id: {} SaveDataSetRequest.dataSet validation failed: {}",
                    responseObserver.hashCode(),
                    resultStatus.msg);
            sendSaveDataSetResponseReject(
                    resultStatus.msg,
                    responseObserver);
            return;
        }

        // handle request
        handler.handleSaveDataSet(request, responseObserver);
    }

    private static QueryDataSetsResponse queryDataSetsResponseExceptionalResult(
            String msg,
            ExceptionalResult.ExceptionalResultStatus status
    ) {
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(status)
                .setMessage(msg)
                .build();

        final QueryDataSetsResponse response = QueryDataSetsResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();

        return response;
    }

    public static QueryDataSetsResponse queryDataSetsResponseReject(String msg) {
        return queryDataSetsResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT);
    }

    public static QueryDataSetsResponse queryDataSetsResponseError(String msg) {
        return queryDataSetsResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR);
    }

    public static QueryDataSetsResponse queryDataSetsResponse(
            QueryDataSetsResponse.DataSetsResult dataSetsResult
    ) {
        return QueryDataSetsResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setDataSetsResult(dataSetsResult)
                .build();
    }

    public static void sendQueryDataSetsResponseReject(
            String msg, StreamObserver<QueryDataSetsResponse> responseObserver) {

        final QueryDataSetsResponse response = queryDataSetsResponseReject(msg);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public static void sendQueryDataSetsResponseError(
            String msg, StreamObserver<QueryDataSetsResponse> responseObserver
    ) {
        final QueryDataSetsResponse response = queryDataSetsResponseError(msg);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public static void sendQueryDataSetsResponse(
            QueryDataSetsResponse.DataSetsResult dataSetsResult,
            StreamObserver<QueryDataSetsResponse> responseObserver
    ) {
        final QueryDataSetsResponse response  = queryDataSetsResponse(dataSetsResult);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void queryDataSets(
            QueryDataSetsRequest request,
            StreamObserver<QueryDataSetsResponse> responseObserver
    ) {
        logger.info("id: {} queryDataSets request received", responseObserver.hashCode());

        // check that request contains non-empty list of criteria
        final List<QueryDataSetsRequest.QueryDataSetsCriterion> criterionList = request.getCriteriaList();
        if (criterionList.size() == 0) {
            final String errorMsg = "QueryDataSetsRequest.criteria list must not be empty";
            sendQueryDataSetsResponseReject(errorMsg, responseObserver);
        }

        // validate query criteria
        for (QueryDataSetsRequest.QueryDataSetsCriterion criterion : criterionList) {

            switch (criterion.getCriterionCase()) {

                case IDCRITERION -> {
                    final QueryDataSetsRequest.QueryDataSetsCriterion.IdCriterion idCriterion
                            = criterion.getIdCriterion();
                    if (idCriterion.getId().isBlank()) {
                        final String errorMsg =
                                "QueryDataSetsRequest.criteria.IdCriterion id must be specified";
                        sendQueryDataSetsResponseReject(errorMsg, responseObserver);
                        return;
                    }
                }

                case OWNERCRITERION -> {
                    final QueryDataSetsRequest.QueryDataSetsCriterion.OwnerCriterion ownerCriterion
                            = criterion.getOwnerCriterion();
                    if (ownerCriterion.getOwnerId().isBlank()) {
                        final String errorMsg =
                                "QueryDataSetsRequest.criteria.OwnerCriterion ownerId must be specified";
                        sendQueryDataSetsResponseReject(errorMsg, responseObserver);
                        return;
                    }
                }

                case TEXTCRITERION -> {
                    final QueryDataSetsRequest.QueryDataSetsCriterion.TextCriterion textCriterion
                            = criterion.getTextCriterion();
                    if (textCriterion.getText().isBlank()) {
                        final String errorMsg =
                                "QueryDataSetsRequest.criteria.TextCriterion text must be specified";
                        sendQueryDataSetsResponseReject(errorMsg, responseObserver);
                        return;
                    }
                }

                case PVNAMECRITERION -> {
                    final QueryDataSetsRequest.QueryDataSetsCriterion.PvNameCriterion pvNameCriterion
                            = criterion.getPvNameCriterion();
                    if (pvNameCriterion.getName().isBlank()) {
                        final String errorMsg =
                                "QueryDataSetsRequest.criteria.PvNameCriterion name must be specified";
                        sendQueryDataSetsResponseReject(errorMsg, responseObserver);
                        return;
                    }
                }

                case CRITERION_NOT_SET -> {
                    final String errorMsg =
                            "QueryDataSetsRequest.criteria criterion case not set";
                    sendQueryDataSetsResponseReject(errorMsg, responseObserver);
                    return;
                }
            }
        }

        handler.handleQueryDataSets(request, responseObserver);
    }

    private static SaveAnnotationResponse saveAnnotationResponseReject(String msg) {
        final ExceptionalResult exceptionalResult =
                ExceptionalResult.newBuilder()
                        .setExceptionalResultStatus(
                                ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT)
                        .setMessage(msg)
                        .build();

        final SaveAnnotationResponse response = SaveAnnotationResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();

        return response;
    }

    public static void sendSaveAnnotationResponseReject(
            String errorMsg,
            StreamObserver<SaveAnnotationResponse> responseObserver
    ) {
        final SaveAnnotationResponse response = saveAnnotationResponseReject(errorMsg);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private static SaveAnnotationResponse saveAnnotationResponseError(String msg) {

        final ExceptionalResult exceptionalResult =
                ExceptionalResult.newBuilder()
                        .setExceptionalResultStatus(ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR)
                        .setMessage(msg)
                        .build();

        final SaveAnnotationResponse response = SaveAnnotationResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();

        return response;
    }

    public static void sendSaveAnnotationResponseError(
            String errorMsg, StreamObserver<SaveAnnotationResponse> responseObserver
    ) {
        final SaveAnnotationResponse response = saveAnnotationResponseError(errorMsg);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private static SaveAnnotationResponse saveAnnotationResponseSuccess(String annotationId) {

        final SaveAnnotationResponse.SaveAnnotationResult result =
                SaveAnnotationResponse.SaveAnnotationResult.newBuilder()
                        .setAnnotationId(annotationId)
                        .build();

        final SaveAnnotationResponse response = SaveAnnotationResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setSaveAnnotationResult(result)
                .build();

        return response;
    }

    public static void sendSaveAnnotationResponseSuccess(
            String annotationId, StreamObserver<SaveAnnotationResponse> responseObserver) {
        final SaveAnnotationResponse response = saveAnnotationResponseSuccess(annotationId);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void saveAnnotation(
            SaveAnnotationRequest request,
            StreamObserver<SaveAnnotationResponse> responseObserver
    ) {
        logger.info(
                "id: {} saveAnnotation request received with name: {}",
                responseObserver.hashCode(),
                request.getName());

        // perform validation of base annotation details
        // validate common annotation details
        final ResultStatus resultStatus =
                AnnotationValidationUtility.validateSaveAnnotationRequest(request);
        if (resultStatus.isError) {
            logger.debug(
                    "id: {} saveAnnotation validation failed: ",
                    responseObserver.hashCode(),
                    resultStatus.msg);
            sendSaveAnnotationResponseReject(
                    resultStatus.msg,
                    responseObserver);
            return;
        }

        // handle request
        handler.handleSaveAnnotation(request, responseObserver);
    }

    private static QueryAnnotationsResponse queryAnnotationsResponseExceptionalResult(
            String msg,
            ExceptionalResult.ExceptionalResultStatus status
    ) {
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(status)
                .setMessage(msg)
                .build();

        final QueryAnnotationsResponse response = QueryAnnotationsResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();

        return response;
    }

    public static QueryAnnotationsResponse queryAnnotationsResponseReject(String msg) {
        return queryAnnotationsResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT);
    }

    public static QueryAnnotationsResponse queryAnnotationsResponseError(String msg) {
        return queryAnnotationsResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR);
    }

    public static QueryAnnotationsResponse queryAnnotationsResponse(
            QueryAnnotationsResponse.AnnotationsResult annotationsResult
    ) {
        return QueryAnnotationsResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setAnnotationsResult(annotationsResult)
                .build();
    }

    public static void sendQueryAnnotationsResponseReject(
            String msg, StreamObserver<QueryAnnotationsResponse> responseObserver) {

        final QueryAnnotationsResponse response = queryAnnotationsResponseReject(msg);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public static void sendQueryAnnotationsResponseError(
            String msg, StreamObserver<QueryAnnotationsResponse> responseObserver
    ) {
        final QueryAnnotationsResponse response = queryAnnotationsResponseError(msg);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public static void sendQueryAnnotationsResponse(
            QueryAnnotationsResponse.AnnotationsResult annotationsResult,
            StreamObserver<QueryAnnotationsResponse> responseObserver
    ) {
        final QueryAnnotationsResponse response  = queryAnnotationsResponse(annotationsResult);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void queryAnnotations(
            QueryAnnotationsRequest request,
            StreamObserver<QueryAnnotationsResponse> responseObserver
    ) {
        logger.info("id: {} queryAnnotations request received", responseObserver.hashCode());

        // check that request contains non-empty list of criteria
        final List<QueryAnnotationsRequest.QueryAnnotationsCriterion> criterionList = request.getCriteriaList();
        if (criterionList.size() == 0) {
            final String errorMsg = "QueryAnnotationsRequest.criteria list must not be empty";
            sendQueryAnnotationsResponseReject(errorMsg, responseObserver);
            return;
        }

        // validate query criteria
        for (QueryAnnotationsRequest.QueryAnnotationsCriterion criterion : criterionList) {

            switch (criterion.getCriterionCase()) {

                case IDCRITERION -> {
                    final QueryAnnotationsRequest.QueryAnnotationsCriterion.IdCriterion idCriterion
                            = criterion.getIdCriterion();
                    if (idCriterion.getId().isBlank()) {
                        final String errorMsg =
                                "QueryAnnotationsRequest.criteria.IdCriterion id must be specified";
                        sendQueryAnnotationsResponseReject(errorMsg, responseObserver);
                        return;
                    }
                }

                case OWNERCRITERION -> {
                    final QueryAnnotationsRequest.QueryAnnotationsCriterion.OwnerCriterion ownerCriterion
                            = criterion.getOwnerCriterion();
                    if (ownerCriterion.getOwnerId().isBlank()) {
                        final String errorMsg =
                                "QueryAnnotationsRequest.criteria.OwnerCriterion ownerId must be specified";
                        sendQueryAnnotationsResponseReject(errorMsg, responseObserver);
                        return;
                    }
                }

                case DATASETSCRITERION -> {
                    final QueryAnnotationsRequest.QueryAnnotationsCriterion.DataSetsCriterion dataSetsCriterion
                            = criterion.getDataSetsCriterion();
                    if (dataSetsCriterion.getDataSetId().isBlank()) {
                        final String errorMsg =
                                "QueryAnnotationsRequest.criteria.DataSetCriterion dataSetId must be specified";
                        sendQueryAnnotationsResponseReject(errorMsg, responseObserver);
                        return;
                    }
                }

                case ANNOTATIONSCRITERION -> {
                    final QueryAnnotationsRequest.QueryAnnotationsCriterion.AnnotationsCriterion annotationsCriterion
                            = criterion.getAnnotationsCriterion();
                    if (annotationsCriterion.getAnnotationId().isBlank()) {
                        final String errorMsg =
                                "QueryAnnotationsRequest.criteria.AssociatedAnnotationIdCriterion id must be specified";
                        sendQueryAnnotationsResponseReject(errorMsg, responseObserver);
                        return;
                    }
                }

                case TEXTCRITERION -> {
                    final QueryAnnotationsRequest.QueryAnnotationsCriterion.TextCriterion commentCriterion
                            = criterion.getTextCriterion();
                    if (commentCriterion.getText().isBlank()) {
                        final String errorMsg =
                                "QueryAnnotationsRequest.criteria.TextCriterion text must be specified";
                        sendQueryAnnotationsResponseReject(errorMsg, responseObserver);
                        return;
                    }
                }

                case TAGSCRITERION -> {
                    final QueryAnnotationsRequest.QueryAnnotationsCriterion.TagsCriterion tagsCriterion
                            = criterion.getTagsCriterion();
                    if (tagsCriterion.getTagValue().isBlank()) {
                        final String errorMsg =
                                "QueryAnnotationsRequest.criteria.TagsCriterion tagValue must be specified";
                        sendQueryAnnotationsResponseReject(errorMsg, responseObserver);
                        return;
                    }
                }

                case ATTRIBUTESCRITERION -> {
                    final QueryAnnotationsRequest.QueryAnnotationsCriterion.AttributesCriterion attributesCriterion
                            = criterion.getAttributesCriterion();
                    if (attributesCriterion.getKey().isBlank()) {
                        final String errorMsg =
                                "QueryAnnotationsRequest.criteria.AttributesCriterion key must be specified";
                        sendQueryAnnotationsResponseReject(errorMsg, responseObserver);
                        return;
                    }
                    if (attributesCriterion.getValue().isBlank()) {
                        final String errorMsg =
                                "QueryAnnotationsRequest.criteria.AttributesCriterion value must be specified";
                        sendQueryAnnotationsResponseReject(errorMsg, responseObserver);
                        return;
                    }
                }

                case CRITERION_NOT_SET -> {
                    final String errorMsg =
                            "QueryAnnotationsRequest.criteria criterion case not set";
                    sendQueryAnnotationsResponseReject(errorMsg, responseObserver);
                    return;
                }
            }
        }

        handler.handleQueryAnnotations(request, responseObserver);
    }

    private static ExportDataResponse exportDataResponseReject(String msg) {

        final ExceptionalResult exceptionalResult =
                ExceptionalResult.newBuilder()
                        .setExceptionalResultStatus(
                                ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT)
                        .setMessage(msg)
                        .build();

        final ExportDataResponse response = ExportDataResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();

        return response;
    }

    public static void sendExportDataResponseReject(
            String errorMsg,
            StreamObserver<ExportDataResponse> responseObserver
    ) {
        final ExportDataResponse response = exportDataResponseReject(errorMsg);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private static ExportDataResponse exportDataResponseError(String msg) {

        final ExceptionalResult exceptionalResult =
                ExceptionalResult.newBuilder()
                        .setExceptionalResultStatus(ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR)
                        .setMessage(msg)
                        .build();

        final ExportDataResponse response = ExportDataResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();

        return response;
    }

    public static void sendExportDataResponseError(
            String errorMsg, StreamObserver<ExportDataResponse> responseObserver
    ) {
        final ExportDataResponse response = exportDataResponseError(errorMsg);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private static ExportDataResponse exportDataResponseSuccess(String filePath, String fileUrl) {

        final ExportDataResponse.ExportDataResult result =
                ExportDataResponse.ExportDataResult.newBuilder()
                        .setFilePath(filePath)
                        .setFileUrl(fileUrl == null ? "" : fileUrl)
                        .build();

        final ExportDataResponse response = ExportDataResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExportDataResult(result)
                .build();

        return response;
    }

    public static void sendExportDataResponseSuccess(
            String filePath, String fileUrl, StreamObserver<ExportDataResponse> responseObserver
    ) {
        final ExportDataResponse response = exportDataResponseSuccess(filePath, fileUrl);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
    
    @Override
    public void exportData(
            ExportDataRequest request,
            StreamObserver<ExportDataResponse> responseObserver
    ) {
        logger.info("id: {} exportData request received", responseObserver.hashCode());

        // validate request
        ResultStatus resultStatus = AnnotationValidationUtility.validateExportDataRequest(request);
        if (resultStatus.isError) {
            logger.debug("id: {} ExportDataRequest validation failed: {}",
                    responseObserver.hashCode(),
                    resultStatus.msg);
            sendExportDataResponseReject(
                    resultStatus.msg,
                    responseObserver);
            return;
        }

        // handle request
        HandlerExportDataRequest handlerRequest = new HandlerExportDataRequest(request, responseObserver);
        handler.handleExportData(handlerRequest);
    }

    // =========================================================
    // savePvMetadata
    // =========================================================

    private static SavePvMetadataResponse savePvMetadataResponseExceptionalResult(
            String msg, ExceptionalResult.ExceptionalResultStatus status) {
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(status)
                .setMessage(msg)
                .build();
        return SavePvMetadataResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();
    }

    public static void sendSavePvMetadataResponseReject(
            String msg, StreamObserver<SavePvMetadataResponse> responseObserver) {
        responseObserver.onNext(savePvMetadataResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT));
        responseObserver.onCompleted();
    }

    public static void sendSavePvMetadataResponseError(
            String msg, StreamObserver<SavePvMetadataResponse> responseObserver) {
        responseObserver.onNext(savePvMetadataResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR));
        responseObserver.onCompleted();
    }

    public static void sendSavePvMetadataResponseSuccess(
            String pvName, StreamObserver<SavePvMetadataResponse> responseObserver) {
        final SavePvMetadataResponse.SavePvMetadataResult result =
                SavePvMetadataResponse.SavePvMetadataResult.newBuilder()
                        .setPvName(pvName)
                        .build();
        responseObserver.onNext(SavePvMetadataResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setSavePvMetadataResult(result)
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void savePvMetadata(
            SavePvMetadataRequest request,
            StreamObserver<SavePvMetadataResponse> responseObserver
    ) {
        logger.info("id: {} savePvMetadata request received pvName: {}",
                responseObserver.hashCode(), request.getPvName());
        handler.handleSavePvMetadata(request, responseObserver);
    }

    // =========================================================
    // queryPvMetadata
    // =========================================================

    private static QueryPvMetadataResponse queryPvMetadataResponseExceptionalResult(
            String msg, ExceptionalResult.ExceptionalResultStatus status) {
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(status)
                .setMessage(msg)
                .build();
        return QueryPvMetadataResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();
    }

    public static void sendQueryPvMetadataResponseReject(
            String msg, StreamObserver<QueryPvMetadataResponse> responseObserver) {
        responseObserver.onNext(queryPvMetadataResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT));
        responseObserver.onCompleted();
    }

    public static void sendQueryPvMetadataResponseError(
            String msg, StreamObserver<QueryPvMetadataResponse> responseObserver) {
        responseObserver.onNext(queryPvMetadataResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR));
        responseObserver.onCompleted();
    }

    public static void sendQueryPvMetadataResponse(
            QueryPvMetadataResponse.PvMetadataResult result,
            StreamObserver<QueryPvMetadataResponse> responseObserver) {
        responseObserver.onNext(QueryPvMetadataResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setPvMetadataResult(result)
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void queryPvMetadata(
            QueryPvMetadataRequest request,
            StreamObserver<QueryPvMetadataResponse> responseObserver
    ) {
        logger.info("id: {} queryPvMetadata request received", responseObserver.hashCode());
        handler.handleQueryPvMetadata(request, responseObserver);
    }

    // =========================================================
    // getPvMetadata
    // =========================================================

    private static GetPvMetadataResponse getPvMetadataResponseExceptionalResult(
            String msg, ExceptionalResult.ExceptionalResultStatus status) {
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(status)
                .setMessage(msg)
                .build();
        return GetPvMetadataResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();
    }

    public static void sendGetPvMetadataResponseReject(
            String msg, StreamObserver<GetPvMetadataResponse> responseObserver) {
        responseObserver.onNext(getPvMetadataResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT));
        responseObserver.onCompleted();
    }

    public static void sendGetPvMetadataResponseError(
            String msg, StreamObserver<GetPvMetadataResponse> responseObserver) {
        responseObserver.onNext(getPvMetadataResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR));
        responseObserver.onCompleted();
    }

    public static void sendGetPvMetadataResponseSuccess(
            com.ospreydcs.dp.grpc.v1.common.PvMetadata pvMetadata,
            StreamObserver<GetPvMetadataResponse> responseObserver) {
        final GetPvMetadataResponse.GetPvMetadataResult result =
                GetPvMetadataResponse.GetPvMetadataResult.newBuilder()
                        .setPvMetadata(pvMetadata)
                        .build();
        responseObserver.onNext(GetPvMetadataResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setGetPvMetadataResult(result)
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void getPvMetadata(
            GetPvMetadataRequest request,
            StreamObserver<GetPvMetadataResponse> responseObserver
    ) {
        logger.info("id: {} getPvMetadata request received pvNameOrAlias: {}",
                responseObserver.hashCode(), request.getPvNameOrAlias());
        handler.handleGetPvMetadata(request, responseObserver);
    }

    // =========================================================
    // deletePvMetadata
    // =========================================================

    private static DeletePvMetadataResponse deletePvMetadataResponseExceptionalResult(
            String msg, ExceptionalResult.ExceptionalResultStatus status) {
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(status)
                .setMessage(msg)
                .build();
        return DeletePvMetadataResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();
    }

    public static void sendDeletePvMetadataResponseReject(
            String msg, StreamObserver<DeletePvMetadataResponse> responseObserver) {
        responseObserver.onNext(deletePvMetadataResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT));
        responseObserver.onCompleted();
    }

    public static void sendDeletePvMetadataResponseError(
            String msg, StreamObserver<DeletePvMetadataResponse> responseObserver) {
        responseObserver.onNext(deletePvMetadataResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR));
        responseObserver.onCompleted();
    }

    public static void sendDeletePvMetadataResponseSuccess(
            String pvName, StreamObserver<DeletePvMetadataResponse> responseObserver) {
        final DeletePvMetadataResponse.DeletePvMetadataResult result =
                DeletePvMetadataResponse.DeletePvMetadataResult.newBuilder()
                        .setPvName(pvName)
                        .build();
        responseObserver.onNext(DeletePvMetadataResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setDeletePvMetadataResult(result)
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void deletePvMetadata(
            DeletePvMetadataRequest request,
            StreamObserver<DeletePvMetadataResponse> responseObserver
    ) {
        logger.info("id: {} deletePvMetadata request received pvNameOrAlias: {}",
                responseObserver.hashCode(), request.getPvNameOrAlias());
        handler.handleDeletePvMetadata(request, responseObserver);
    }

    // =========================================================
    // saveConfiguration
    // =========================================================

    private static SaveConfigurationResponse saveConfigurationResponseExceptionalResult(
            String msg, ExceptionalResult.ExceptionalResultStatus status) {
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(status)
                .setMessage(msg)
                .build();
        return SaveConfigurationResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();
    }

    public static void sendSaveConfigurationResponseReject(
            String msg, StreamObserver<SaveConfigurationResponse> responseObserver) {
        responseObserver.onNext(saveConfigurationResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT));
        responseObserver.onCompleted();
    }

    public static void sendSaveConfigurationResponseError(
            String msg, StreamObserver<SaveConfigurationResponse> responseObserver) {
        responseObserver.onNext(saveConfigurationResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR));
        responseObserver.onCompleted();
    }

    public static void sendSaveConfigurationResponseSuccess(
            String configurationName, StreamObserver<SaveConfigurationResponse> responseObserver) {
        final SaveConfigurationResponse.SaveConfigurationResult result =
                SaveConfigurationResponse.SaveConfigurationResult.newBuilder()
                        .setConfigurationName(configurationName)
                        .build();
        responseObserver.onNext(SaveConfigurationResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setSaveConfigurationResult(result)
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void saveConfiguration(
            SaveConfigurationRequest request,
            StreamObserver<SaveConfigurationResponse> responseObserver
    ) {
        logger.info("id: {} saveConfiguration request received configurationName: {}",
                responseObserver.hashCode(), request.getConfigurationName());
        handler.handleSaveConfiguration(request, responseObserver);
    }

    // =========================================================
    // getConfiguration
    // =========================================================

    private static GetConfigurationResponse getConfigurationResponseExceptionalResult(
            String msg, ExceptionalResult.ExceptionalResultStatus status) {
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(status)
                .setMessage(msg)
                .build();
        return GetConfigurationResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();
    }

    public static void sendGetConfigurationResponseReject(
            String msg, StreamObserver<GetConfigurationResponse> responseObserver) {
        responseObserver.onNext(getConfigurationResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT));
        responseObserver.onCompleted();
    }

    public static void sendGetConfigurationResponseError(
            String msg, StreamObserver<GetConfigurationResponse> responseObserver) {
        responseObserver.onNext(getConfigurationResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR));
        responseObserver.onCompleted();
    }

    public static void sendGetConfigurationResponseSuccess(
            com.ospreydcs.dp.grpc.v1.common.Configuration configuration,
            StreamObserver<GetConfigurationResponse> responseObserver) {
        final GetConfigurationResponse.GetConfigurationResult result =
                GetConfigurationResponse.GetConfigurationResult.newBuilder()
                        .setConfiguration(configuration)
                        .build();
        responseObserver.onNext(GetConfigurationResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setGetConfigurationResult(result)
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void getConfiguration(
            GetConfigurationRequest request,
            StreamObserver<GetConfigurationResponse> responseObserver
    ) {
        logger.info("id: {} getConfiguration request received configurationName: {}",
                responseObserver.hashCode(), request.getConfigurationName());
        handler.handleGetConfiguration(request, responseObserver);
    }

    // =========================================================
    // queryConfigurations
    // =========================================================

    private static QueryConfigurationsResponse queryConfigurationsResponseExceptionalResult(
            String msg, ExceptionalResult.ExceptionalResultStatus status) {
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(status)
                .setMessage(msg)
                .build();
        return QueryConfigurationsResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();
    }

    public static void sendQueryConfigurationsResponseReject(
            String msg, StreamObserver<QueryConfigurationsResponse> responseObserver) {
        responseObserver.onNext(queryConfigurationsResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT));
        responseObserver.onCompleted();
    }

    public static void sendQueryConfigurationsResponseError(
            String msg, StreamObserver<QueryConfigurationsResponse> responseObserver) {
        responseObserver.onNext(queryConfigurationsResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR));
        responseObserver.onCompleted();
    }

    public static void sendQueryConfigurationsResponse(
            QueryConfigurationsResponse.QueryConfigurationsResult result,
            StreamObserver<QueryConfigurationsResponse> responseObserver) {
        responseObserver.onNext(QueryConfigurationsResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setQueryConfigurationsResult(result)
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void queryConfigurations(
            QueryConfigurationsRequest request,
            StreamObserver<QueryConfigurationsResponse> responseObserver
    ) {
        logger.info("id: {} queryConfigurations request received", responseObserver.hashCode());
        handler.handleQueryConfigurations(request, responseObserver);
    }

    // =========================================================
    // deleteConfiguration
    // =========================================================

    private static DeleteConfigurationResponse deleteConfigurationResponseExceptionalResult(
            String msg, ExceptionalResult.ExceptionalResultStatus status) {
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(status)
                .setMessage(msg)
                .build();
        return DeleteConfigurationResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();
    }

    public static void sendDeleteConfigurationResponseReject(
            String msg, StreamObserver<DeleteConfigurationResponse> responseObserver) {
        responseObserver.onNext(deleteConfigurationResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT));
        responseObserver.onCompleted();
    }

    public static void sendDeleteConfigurationResponseError(
            String msg, StreamObserver<DeleteConfigurationResponse> responseObserver) {
        responseObserver.onNext(deleteConfigurationResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR));
        responseObserver.onCompleted();
    }

    public static void sendDeleteConfigurationResponseSuccess(
            String configurationName, StreamObserver<DeleteConfigurationResponse> responseObserver) {
        final DeleteConfigurationResponse.DeleteConfigurationResult result =
                DeleteConfigurationResponse.DeleteConfigurationResult.newBuilder()
                        .setConfigurationName(configurationName)
                        .build();
        responseObserver.onNext(DeleteConfigurationResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setDeleteConfigurationResult(result)
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteConfiguration(
            DeleteConfigurationRequest request,
            StreamObserver<DeleteConfigurationResponse> responseObserver
    ) {
        logger.info("id: {} deleteConfiguration request received configurationName: {}",
                responseObserver.hashCode(), request.getConfigurationName());
        handler.handleDeleteConfiguration(request, responseObserver);
    }

    // =========================================================
    // patchConfiguration (stub — not yet implemented)
    // =========================================================

    @Override
    public void patchConfiguration(
            PatchConfigurationRequest request,
            StreamObserver<PatchConfigurationResponse> responseObserver
    ) {
        logger.info("id: {} patchConfiguration request received", responseObserver.hashCode());
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR)
                .setMessage("patchConfiguration() is not yet implemented")
                .build();
        responseObserver.onNext(PatchConfigurationResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build());
        responseObserver.onCompleted();
    }

    // =========================================================
    // bulkSaveConfiguration (stub — not yet implemented)
    // =========================================================

    @Override
    public void bulkSaveConfiguration(
            BulkSaveConfigurationRequest request,
            StreamObserver<BulkSaveConfigurationResponse> responseObserver
    ) {
        logger.info("id: {} bulkSaveConfiguration request received", responseObserver.hashCode());
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR)
                .setMessage("bulkSaveConfiguration() is not yet implemented")
                .build();
        responseObserver.onNext(BulkSaveConfigurationResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build());
        responseObserver.onCompleted();
    }

    // =========================================================
    // saveConfigurationActivation
    // =========================================================

    private static SaveConfigurationActivationResponse saveConfigurationActivationResponseExceptionalResult(
            String msg, ExceptionalResult.ExceptionalResultStatus status) {
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(status)
                .setMessage(msg)
                .build();
        return SaveConfigurationActivationResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();
    }

    public static void sendSaveConfigurationActivationResponseReject(
            String msg, StreamObserver<SaveConfigurationActivationResponse> responseObserver) {
        responseObserver.onNext(saveConfigurationActivationResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT));
        responseObserver.onCompleted();
    }

    public static void sendSaveConfigurationActivationResponseError(
            String msg, StreamObserver<SaveConfigurationActivationResponse> responseObserver) {
        responseObserver.onNext(saveConfigurationActivationResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR));
        responseObserver.onCompleted();
    }

    public static void sendSaveConfigurationActivationResponseSuccess(
            String clientActivationId, StreamObserver<SaveConfigurationActivationResponse> responseObserver) {
        final SaveConfigurationActivationResponse.SaveConfigurationActivationResult result =
                SaveConfigurationActivationResponse.SaveConfigurationActivationResult.newBuilder()
                        .setClientActivationId(clientActivationId)
                        .build();
        responseObserver.onNext(SaveConfigurationActivationResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setSaveConfigurationActivationResult(result)
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void saveConfigurationActivation(
            SaveConfigurationActivationRequest request,
            StreamObserver<SaveConfigurationActivationResponse> responseObserver
    ) {
        logger.info("id: {} saveConfigurationActivation request received configurationName: {}",
                responseObserver.hashCode(), request.getConfigurationName());
        handler.handleSaveConfigurationActivation(request, responseObserver);
    }

    // =========================================================
    // getConfigurationActivation
    // =========================================================

    private static GetConfigurationActivationResponse getConfigurationActivationResponseExceptionalResult(
            String msg, ExceptionalResult.ExceptionalResultStatus status) {
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(status)
                .setMessage(msg)
                .build();
        return GetConfigurationActivationResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();
    }

    public static void sendGetConfigurationActivationResponseReject(
            String msg, StreamObserver<GetConfigurationActivationResponse> responseObserver) {
        responseObserver.onNext(getConfigurationActivationResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT));
        responseObserver.onCompleted();
    }

    public static void sendGetConfigurationActivationResponseError(
            String msg, StreamObserver<GetConfigurationActivationResponse> responseObserver) {
        responseObserver.onNext(getConfigurationActivationResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR));
        responseObserver.onCompleted();
    }

    public static void sendGetConfigurationActivationResponseSuccess(
            com.ospreydcs.dp.grpc.v1.common.ConfigurationActivation activation,
            StreamObserver<GetConfigurationActivationResponse> responseObserver) {
        final GetConfigurationActivationResponse.GetConfigurationActivationResult result =
                GetConfigurationActivationResponse.GetConfigurationActivationResult.newBuilder()
                        .setConfigurationActivation(activation)
                        .build();
        responseObserver.onNext(GetConfigurationActivationResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setGetConfigurationActivationResult(result)
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void getConfigurationActivation(
            GetConfigurationActivationRequest request,
            StreamObserver<GetConfigurationActivationResponse> responseObserver
    ) {
        logger.info("id: {} getConfigurationActivation request received", responseObserver.hashCode());
        handler.handleGetConfigurationActivation(request, responseObserver);
    }

    // =========================================================
    // queryConfigurationActivations
    // =========================================================

    private static QueryConfigurationActivationsResponse queryConfigurationActivationsResponseExceptionalResult(
            String msg, ExceptionalResult.ExceptionalResultStatus status) {
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(status)
                .setMessage(msg)
                .build();
        return QueryConfigurationActivationsResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();
    }

    public static void sendQueryConfigurationActivationsResponseReject(
            String msg, StreamObserver<QueryConfigurationActivationsResponse> responseObserver) {
        responseObserver.onNext(queryConfigurationActivationsResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT));
        responseObserver.onCompleted();
    }

    public static void sendQueryConfigurationActivationsResponseError(
            String msg, StreamObserver<QueryConfigurationActivationsResponse> responseObserver) {
        responseObserver.onNext(queryConfigurationActivationsResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR));
        responseObserver.onCompleted();
    }

    public static void sendQueryConfigurationActivationsResponse(
            QueryConfigurationActivationsResponse.QueryConfigurationActivationsResult result,
            StreamObserver<QueryConfigurationActivationsResponse> responseObserver) {
        responseObserver.onNext(QueryConfigurationActivationsResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setQueryConfigurationActivationsResult(result)
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void queryConfigurationActivations(
            QueryConfigurationActivationsRequest request,
            StreamObserver<QueryConfigurationActivationsResponse> responseObserver
    ) {
        logger.info("id: {} queryConfigurationActivations request received", responseObserver.hashCode());
        handler.handleQueryConfigurationActivations(request, responseObserver);
    }

    // =========================================================
    // deleteConfigurationActivation
    // =========================================================

    private static DeleteConfigurationActivationResponse deleteConfigurationActivationResponseExceptionalResult(
            String msg, ExceptionalResult.ExceptionalResultStatus status) {
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(status)
                .setMessage(msg)
                .build();
        return DeleteConfigurationActivationResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();
    }

    public static void sendDeleteConfigurationActivationResponseReject(
            String msg, StreamObserver<DeleteConfigurationActivationResponse> responseObserver) {
        responseObserver.onNext(deleteConfigurationActivationResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT));
        responseObserver.onCompleted();
    }

    public static void sendDeleteConfigurationActivationResponseError(
            String msg, StreamObserver<DeleteConfigurationActivationResponse> responseObserver) {
        responseObserver.onNext(deleteConfigurationActivationResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR));
        responseObserver.onCompleted();
    }

    public static void sendDeleteConfigurationActivationResponseSuccess(
            String clientActivationId, StreamObserver<DeleteConfigurationActivationResponse> responseObserver) {
        final DeleteConfigurationActivationResponse.DeleteConfigurationActivationResult result =
                DeleteConfigurationActivationResponse.DeleteConfigurationActivationResult.newBuilder()
                        .setClientActivationId(clientActivationId)
                        .build();
        responseObserver.onNext(DeleteConfigurationActivationResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setDeleteConfigurationActivationResult(result)
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteConfigurationActivation(
            DeleteConfigurationActivationRequest request,
            StreamObserver<DeleteConfigurationActivationResponse> responseObserver
    ) {
        logger.info("id: {} deleteConfigurationActivation request received", responseObserver.hashCode());
        handler.handleDeleteConfigurationActivation(request, responseObserver);
    }

    // =========================================================
    // patchConfigurationActivation (stub — not yet implemented)
    // =========================================================

    @Override
    public void patchConfigurationActivation(
            PatchConfigurationActivationRequest request,
            StreamObserver<PatchConfigurationActivationResponse> responseObserver
    ) {
        logger.info("id: {} patchConfigurationActivation request received", responseObserver.hashCode());
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR)
                .setMessage("patchConfigurationActivation() is not yet implemented")
                .build();
        responseObserver.onNext(PatchConfigurationActivationResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build());
        responseObserver.onCompleted();
    }

    // =========================================================
    // bulkSaveConfigurationActivation (stub — not yet implemented)
    // =========================================================

    @Override
    public void bulkSaveConfigurationActivation(
            BulkSaveConfigurationActivationRequest request,
            StreamObserver<BulkSaveConfigurationActivationResponse> responseObserver
    ) {
        logger.info("id: {} bulkSaveConfigurationActivation request received", responseObserver.hashCode());
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR)
                .setMessage("bulkSaveConfigurationActivation() is not yet implemented")
                .build();
        responseObserver.onNext(BulkSaveConfigurationActivationResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build());
        responseObserver.onCompleted();
    }

    // =========================================================
    // getActiveConfigurations
    // =========================================================

    private static GetActiveConfigurationsResponse getActiveConfigurationsResponseExceptionalResult(
            String msg, ExceptionalResult.ExceptionalResultStatus status) {
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(status)
                .setMessage(msg)
                .build();
        return GetActiveConfigurationsResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build();
    }

    public static void sendGetActiveConfigurationsResponseReject(
            String msg, StreamObserver<GetActiveConfigurationsResponse> responseObserver) {
        responseObserver.onNext(getActiveConfigurationsResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT));
        responseObserver.onCompleted();
    }

    public static void sendGetActiveConfigurationsResponseError(
            String msg, StreamObserver<GetActiveConfigurationsResponse> responseObserver) {
        responseObserver.onNext(getActiveConfigurationsResponseExceptionalResult(
                msg, ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR));
        responseObserver.onCompleted();
    }

    public static void sendGetActiveConfigurationsResponse(
            GetActiveConfigurationsResponse.GetActiveConfigurationsResult result,
            StreamObserver<GetActiveConfigurationsResponse> responseObserver) {
        responseObserver.onNext(GetActiveConfigurationsResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setGetActiveConfigurationsResult(result)
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void getActiveConfigurations(
            GetActiveConfigurationsRequest request,
            StreamObserver<GetActiveConfigurationsResponse> responseObserver
    ) {
        logger.info("id: {} getActiveConfigurations request received", responseObserver.hashCode());
        handler.handleGetActiveConfigurations(request, responseObserver);
    }

    // =========================================================
    // patchPvMetadata (stub — not yet implemented)
    // =========================================================

    @Override
    public void patchPvMetadata(
            PatchPvMetadataRequest request,
            StreamObserver<PatchPvMetadataResponse> responseObserver
    ) {
        logger.info("id: {} patchPvMetadata request received", responseObserver.hashCode());
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR)
                .setMessage("patchPvMetadata is not yet implemented")
                .build();
        responseObserver.onNext(PatchPvMetadataResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build());
        responseObserver.onCompleted();
    }

    // =========================================================
    // bulkSavePvMetadata (stub — not yet implemented)
    // =========================================================

    @Override
    public void bulkSavePvMetadata(
            BulkSavePvMetadataRequest request,
            StreamObserver<BulkSavePvMetadataResponse> responseObserver
    ) {
        logger.info("id: {} bulkSavePvMetadata request received", responseObserver.hashCode());
        final ExceptionalResult exceptionalResult = ExceptionalResult.newBuilder()
                .setExceptionalResultStatus(ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_ERROR)
                .setMessage("bulkSavePvMetadata is not yet implemented")
                .build();
        responseObserver.onNext(BulkSavePvMetadataResponse.newBuilder()
                .setResponseTime(TimestampUtility.getTimestampNow())
                .setExceptionalResult(exceptionalResult)
                .build());
        responseObserver.onCompleted();
    }
}
