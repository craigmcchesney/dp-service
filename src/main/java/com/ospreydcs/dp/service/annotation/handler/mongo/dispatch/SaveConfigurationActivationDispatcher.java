package com.ospreydcs.dp.service.annotation.handler.mongo.dispatch;

import com.ospreydcs.dp.grpc.v1.annotation.SaveConfigurationActivationRequest;
import com.ospreydcs.dp.grpc.v1.annotation.SaveConfigurationActivationResponse;
import com.ospreydcs.dp.service.annotation.service.AnnotationServiceImpl;
import com.ospreydcs.dp.service.common.handler.Dispatcher;
import com.ospreydcs.dp.service.common.model.MongoSaveResult;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import io.grpc.stub.StreamObserver;

public class SaveConfigurationActivationDispatcher extends Dispatcher {

    private final StreamObserver<SaveConfigurationActivationResponse> responseObserver;
    private final SaveConfigurationActivationRequest request;

    public SaveConfigurationActivationDispatcher(
            StreamObserver<SaveConfigurationActivationResponse> responseObserver,
            SaveConfigurationActivationRequest request
    ) {
        this.responseObserver = responseObserver;
        this.request = request;
    }

    public void handleValidationError(ResultStatus resultStatus) {
        AnnotationServiceImpl.sendSaveConfigurationActivationResponseReject(resultStatus.msg, responseObserver);
    }

    public void handleError(String errorMsg) {
        AnnotationServiceImpl.sendSaveConfigurationActivationResponseError(errorMsg, responseObserver);
    }

    public void handleResult(MongoSaveResult result) {
        if (result.isError) {
            AnnotationServiceImpl.sendSaveConfigurationActivationResponseError(result.message, responseObserver);
        } else {
            AnnotationServiceImpl.sendSaveConfigurationActivationResponseSuccess(result.documentId, responseObserver);
        }
    }
}
