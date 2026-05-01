package com.ospreydcs.dp.service.annotation.handler.mongo.dispatch;

import com.ospreydcs.dp.grpc.v1.annotation.GetConfigurationActivationRequest;
import com.ospreydcs.dp.grpc.v1.annotation.GetConfigurationActivationResponse;
import com.ospreydcs.dp.service.annotation.service.AnnotationServiceImpl;
import com.ospreydcs.dp.service.common.bson.configuration.ConfigurationActivationDocument;
import com.ospreydcs.dp.service.common.handler.Dispatcher;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import io.grpc.stub.StreamObserver;

public class GetConfigurationActivationDispatcher extends Dispatcher {

    private final StreamObserver<GetConfigurationActivationResponse> responseObserver;
    private final GetConfigurationActivationRequest request;

    public GetConfigurationActivationDispatcher(
            StreamObserver<GetConfigurationActivationResponse> responseObserver,
            GetConfigurationActivationRequest request
    ) {
        this.responseObserver = responseObserver;
        this.request = request;
    }

    public void handleValidationError(ResultStatus resultStatus) {
        AnnotationServiceImpl.sendGetConfigurationActivationResponseReject(resultStatus.msg, responseObserver);
    }

    public void handleError(String errorMsg) {
        AnnotationServiceImpl.sendGetConfigurationActivationResponseError(errorMsg, responseObserver);
    }

    public void handleResult(ConfigurationActivationDocument document) {
        if (document == null) {
            final String keyDescription = switch (request.getKeyCase()) {
                case CLIENTACTIVATIONID -> "clientActivationId: " + request.getClientActivationId();
                case COMPOSITEKEY -> "configurationName: " + request.getCompositeKey().getConfigurationName();
                default -> "unknown key";
            };
            final String msg = "no ConfigurationActivation record found for: " + keyDescription;
            AnnotationServiceImpl.sendGetConfigurationActivationResponseReject(msg, responseObserver);
        } else {
            AnnotationServiceImpl.sendGetConfigurationActivationResponseSuccess(
                    document.toConfigurationActivation(), responseObserver);
        }
    }
}
