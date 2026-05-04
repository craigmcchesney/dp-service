package com.ospreydcs.dp.service.annotation.handler.mongo.dispatch;

import com.ospreydcs.dp.grpc.v1.annotation.SaveConfigurationRequest;
import com.ospreydcs.dp.grpc.v1.annotation.SaveConfigurationResponse;
import com.ospreydcs.dp.service.annotation.service.AnnotationServiceImpl;
import com.ospreydcs.dp.service.common.handler.Dispatcher;
import com.ospreydcs.dp.service.common.model.MongoSaveResult;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import io.grpc.stub.StreamObserver;

public class SaveConfigurationDispatcher extends Dispatcher {

    private final StreamObserver<SaveConfigurationResponse> responseObserver;
    private final SaveConfigurationRequest request;

    public SaveConfigurationDispatcher(
            StreamObserver<SaveConfigurationResponse> responseObserver,
            SaveConfigurationRequest request
    ) {
        this.responseObserver = responseObserver;
        this.request = request;
    }

    public void handleValidationError(ResultStatus resultStatus) {
        AnnotationServiceImpl.sendSaveConfigurationResponseReject(resultStatus.msg, responseObserver);
    }

    public void handleError(String errorMsg) {
        AnnotationServiceImpl.sendSaveConfigurationResponseError(errorMsg, responseObserver);
    }

    public void handleResult(MongoSaveResult result) {
        if (result.isError) {
            AnnotationServiceImpl.sendSaveConfigurationResponseError(result.message, responseObserver);
        } else {
            AnnotationServiceImpl.sendSaveConfigurationResponseSuccess(result.documentId, responseObserver);
        }
    }
}
