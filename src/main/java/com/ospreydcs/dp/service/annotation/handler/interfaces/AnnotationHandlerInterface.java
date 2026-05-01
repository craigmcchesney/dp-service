package com.ospreydcs.dp.service.annotation.handler.interfaces;

import com.ospreydcs.dp.grpc.v1.annotation.*;
import com.ospreydcs.dp.service.annotation.handler.model.HandlerExportDataRequest;
import io.grpc.stub.StreamObserver;

public interface AnnotationHandlerInterface {

    boolean init();
    boolean fini();
    boolean start();
    boolean stop();

    void handleSaveDataSet(
            SaveDataSetRequest request,
            StreamObserver<SaveDataSetResponse> responseObserver);

    void handleQueryDataSets(QueryDataSetsRequest request, StreamObserver<QueryDataSetsResponse> responseObserver);

    void handleSaveAnnotation(
            SaveAnnotationRequest request,
            StreamObserver<SaveAnnotationResponse> responseObserver);

    void handleQueryAnnotations(
            QueryAnnotationsRequest request, StreamObserver<QueryAnnotationsResponse> responseObserver);

    void handleExportData(HandlerExportDataRequest handlerRequest);

    void handleSavePvMetadata(
            SavePvMetadataRequest request,
            StreamObserver<SavePvMetadataResponse> responseObserver);

    void handleQueryPvMetadata(
            QueryPvMetadataRequest request,
            StreamObserver<QueryPvMetadataResponse> responseObserver);

    void handleGetPvMetadata(
            GetPvMetadataRequest request,
            StreamObserver<GetPvMetadataResponse> responseObserver);

    void handleDeletePvMetadata(
            DeletePvMetadataRequest request,
            StreamObserver<DeletePvMetadataResponse> responseObserver);

    void handleSaveConfiguration(
            SaveConfigurationRequest request,
            StreamObserver<SaveConfigurationResponse> responseObserver);

    void handleGetConfiguration(
            GetConfigurationRequest request,
            StreamObserver<GetConfigurationResponse> responseObserver);

    void handleQueryConfigurations(
            QueryConfigurationsRequest request,
            StreamObserver<QueryConfigurationsResponse> responseObserver);

    void handleDeleteConfiguration(
            DeleteConfigurationRequest request,
            StreamObserver<DeleteConfigurationResponse> responseObserver);

    void handleSaveConfigurationActivation(
            SaveConfigurationActivationRequest request,
            StreamObserver<SaveConfigurationActivationResponse> responseObserver);

    void handleGetConfigurationActivation(
            GetConfigurationActivationRequest request,
            StreamObserver<GetConfigurationActivationResponse> responseObserver);

    void handleQueryConfigurationActivations(
            QueryConfigurationActivationsRequest request,
            StreamObserver<QueryConfigurationActivationsResponse> responseObserver);

    void handleDeleteConfigurationActivation(
            DeleteConfigurationActivationRequest request,
            StreamObserver<DeleteConfigurationActivationResponse> responseObserver);

    void handleGetActiveConfigurations(
            GetActiveConfigurationsRequest request,
            StreamObserver<GetActiveConfigurationsResponse> responseObserver);
}
