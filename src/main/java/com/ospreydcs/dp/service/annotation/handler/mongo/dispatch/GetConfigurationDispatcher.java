package com.ospreydcs.dp.service.annotation.handler.mongo.dispatch;

import com.ospreydcs.dp.grpc.v1.annotation.GetConfigurationRequest;
import com.ospreydcs.dp.grpc.v1.annotation.GetConfigurationResponse;
import com.ospreydcs.dp.service.annotation.service.AnnotationServiceImpl;
import com.ospreydcs.dp.service.common.bson.configuration.ConfigurationDocument;
import com.ospreydcs.dp.service.common.handler.Dispatcher;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import io.grpc.stub.StreamObserver;

public class GetConfigurationDispatcher extends Dispatcher {

    private final StreamObserver<GetConfigurationResponse> responseObserver;
    private final GetConfigurationRequest request;

    public GetConfigurationDispatcher(
            StreamObserver<GetConfigurationResponse> responseObserver,
            GetConfigurationRequest request
    ) {
        this.responseObserver = responseObserver;
        this.request = request;
    }

    public void handleValidationError(ResultStatus resultStatus) {
        AnnotationServiceImpl.sendGetConfigurationResponseReject(resultStatus.msg, responseObserver);
    }

    public void handleError(String errorMsg) {
        AnnotationServiceImpl.sendGetConfigurationResponseError(errorMsg, responseObserver);
    }

    public void handleResult(ConfigurationDocument document) {
        if (document == null) {
            final String msg = "no Configuration record found for: " + request.getConfigurationName();
            AnnotationServiceImpl.sendGetConfigurationResponseReject(msg, responseObserver);
        } else {
            AnnotationServiceImpl.sendGetConfigurationResponseSuccess(document.toConfiguration(), responseObserver);
        }
    }
}
