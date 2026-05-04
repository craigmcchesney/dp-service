package com.ospreydcs.dp.service.annotation;

import ch.systemsx.cisd.hdf5.IHDF5Reader;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.ospreydcs.dp.grpc.v1.annotation.*;
import com.ospreydcs.dp.grpc.v1.common.CalculationsSpec;
import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DoubleColumn;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.service.common.bson.column.DataColumnDocument;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.bson.calculations.CalculationsDataFrameDocument;
import com.ospreydcs.dp.service.common.bson.calculations.CalculationsDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataBlockDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import com.ospreydcs.dp.service.common.protobuf.AttributesUtility;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.ospreydcs.dp.service.annotation.handler.mongo.export.DataExportHdf5File.*;
import static org.junit.Assert.*;

/**
 * Base class for unit and integration tests covering the Annotation Service APIs.  Provides utilities for those tests,
 * including 1) params objects for creating protobuf API requests, 2) methods for building protobuf API requests from
 * the params, 3) observers for the API response streams, and 4) utilities for verifying the API results.
 */
public class AnnotationTestBase {

    public record AnnotationDataBlock(
            long beginSeconds,
            long beginNanos,
            long endSeconds,
            long endNanos,
            List<String> pvNames) {
    }

    public record AnnotationDataSet(
            String id,
            String name,
            String ownerId,
            String description,
            List<AnnotationDataBlock> dataBlocks) {
    }

    public record SaveDataSetParams(AnnotationDataSet dataSet) {
    }

    public static class SaveDataSetResponseObserver implements StreamObserver<SaveDataSetResponse> {

        // instance variables
        private final CountDownLatch finishLatch = new CountDownLatch(1);
        private final AtomicBoolean isError = new AtomicBoolean(false);
        private final List<String> errorMessageList = Collections.synchronizedList(new ArrayList<>());
        private final List<String> dataSetIdList = Collections.synchronizedList(new ArrayList<>());

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

        public String getDataSetId() {
            if (!dataSetIdList.isEmpty()) {
                return dataSetIdList.get(0);
            } else {
                return null;
            }
        }

        @Override
        public void onNext(SaveDataSetResponse response) {

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

                assertTrue(response.hasSaveDataSetResult());
                final SaveDataSetResponse.SaveDataSetResult result = response.getSaveDataSetResult();
                assertNotNull(result);

                // flag error if already received a response
                if (!dataSetIdList.isEmpty()) {
                    final String errorMsg = "onNext received more than one response";
                    System.err.println(errorMsg);
                    isError.set(true);
                    errorMessageList.add(errorMsg);

                } else {
                    dataSetIdList.add(result.getDataSetId());
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
                final String errorMsg = "onError error: " + status;
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

    public static class QueryDataSetsParams {

        public String idCriterion = null;
        public String ownerCriterion = null;
        public String textCriterion = null;
        public String pvNameCriterion = null;

        public void setIdCriterion(String idCriterion) {
            this.idCriterion = idCriterion;
        }

        public void setOwnerCriterion(String ownerCriterion) {
            this.ownerCriterion = ownerCriterion;
        }

        public void setTextCriterion(String commentCriterion) {
            this.textCriterion = commentCriterion;
        }

        public void setPvNameCriterion(String pvNameCriterion) {
            this.pvNameCriterion = pvNameCriterion;
        }
    }

    public static class QueryDataSetsResponseObserver implements StreamObserver<QueryDataSetsResponse> {

        // instance variables
        private final CountDownLatch finishLatch = new CountDownLatch(1);
        private final AtomicBoolean isError = new AtomicBoolean(false);
        private final List<String> errorMessageList = Collections.synchronizedList(new ArrayList<>());
        private final List<DataSet> dataSetsList =
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

        public boolean isError() { return isError.get(); }

        public String getErrorMessage() {
            if (!errorMessageList.isEmpty()) {
                return errorMessageList.get(0);
            } else {
                return "";
            }
        }

        public List<DataSet> getDataSetsList() {
            return dataSetsList;
        }

        @Override
        public void onNext(QueryDataSetsResponse response) {

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

                assertTrue(response.hasDataSetsResult());
                List<DataSet> responseDataSetsList =
                        response.getDataSetsResult().getDataSetsList();

                // flag error if already received a response
                if (!dataSetsList.isEmpty()) {
                    final String errorMsg = "onNext received more than one response";
                    System.err.println(errorMsg);
                    isError.set(true);
                    errorMessageList.add(errorMsg);

                } else {
                    dataSetsList.addAll(responseDataSetsList);
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
                final String errorMsg = "onError error: " + status;
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

    public static class SaveAnnotationRequestParams {

        public final String id;
        public final String ownerId;
        public final List<String> dataSetIds;
        public final String name;
        public final List<String> annotationIds;
        public final String comment;
        public final List<String> tags;
        public final Map<String, String> attributeMap;
        public final Calculations calculations;

        public SaveAnnotationRequestParams(String ownerId, String name, List<String> dataSetIds) {
            this.id = null;
            this.ownerId = ownerId;
            this.dataSetIds = dataSetIds;
            this.name = name;
            this.annotationIds = null;
            this.comment = null;
            this.tags = null;
            this.attributeMap = null;
            this.calculations = null;
        }

        public SaveAnnotationRequestParams(
                String id,
                String ownerId,
                String name,
                List<String> dataSetIds,
                List<String> annotationIds,
                String comment,
                List<String> tags,
                Map<String, String> attributeMap,
                Calculations calculations
        ) {
            this.id = id;
            this.ownerId = ownerId;
            this.dataSetIds = dataSetIds;
            this.name = name;
            this.annotationIds = annotationIds;
            this.comment = comment;
            this.tags = tags;
            this.attributeMap = attributeMap;
            this.calculations = calculations;
        }
    }

    public static class SaveAnnotationResponseObserver implements StreamObserver<SaveAnnotationResponse> {

        // instance variables
        private final CountDownLatch finishLatch = new CountDownLatch(1);
        private final AtomicBoolean isError = new AtomicBoolean(false);
        private final List<String> errorMessageList = Collections.synchronizedList(new ArrayList<>());
        private final List<String> annotationIdList = Collections.synchronizedList(new ArrayList<>());

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

        public String getAnnotationId() {
            if (!annotationIdList.isEmpty()) {
                return annotationIdList.get(0);
            } else {
                return null;
            }
        }

        @Override
        public void onNext(SaveAnnotationResponse response) {

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

                assertTrue(response.hasSaveAnnotationResult());
                final SaveAnnotationResponse.SaveAnnotationResult result = response.getSaveAnnotationResult();
                assertNotNull(result);

                // flag error if already received a response
                if (!annotationIdList.isEmpty()) {
                    final String errorMsg = "onNext received more than one response";
                    System.err.println(errorMsg);
                    isError.set(true);
                    errorMessageList.add(errorMsg);

                } else {
                    annotationIdList.add(result.getAnnotationId());
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
                final String errorMsg = "onError error: " + status;
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

    public static class QueryAnnotationsParams {

        public String idCriterion = null;
        public String ownerCriterion = null;
        public String datasetsCriterion = null;
        public String annotationsCriterion = null;
        public String textCriterion = null;
        public String tagsCriterion = null;
        public String attributesCriterionKey = null;
        public String attributesCriterionValue = null;
        public String eventCriterion = null;

        public void setIdCriterion(String idCriterion) {
            this.idCriterion = idCriterion;
        }

        public void setOwnerCriterion(String ownerCriterion) {
            this.ownerCriterion = ownerCriterion;
        }

        public void setDatasetsCriterion(String datasetsCriterion) {
            this.datasetsCriterion = datasetsCriterion;
        }

        public void setAnnotationsCriterion(String annotationsCriterion) {
            this.annotationsCriterion = annotationsCriterion;
        }

        public void setTextCriterion(String commentCriterion) {
            this.textCriterion = commentCriterion;
        }

        public void setTagsCriterion(String tagsCriterion) {
            this.tagsCriterion = tagsCriterion;
        }

        public void setAttributesCriterion(String attributeCriterionKey, String attributeCriterionValue) {
            this.attributesCriterionKey = attributeCriterionKey;
            this.attributesCriterionValue = attributeCriterionValue;
        }

    }

    public static class QueryAnnotationsResponseObserver implements StreamObserver<QueryAnnotationsResponse> {

        // instance variables
        private final CountDownLatch finishLatch = new CountDownLatch(1);
        private final AtomicBoolean isError = new AtomicBoolean(false);
        private final List<String> errorMessageList = Collections.synchronizedList(new ArrayList<>());
        private final List<QueryAnnotationsResponse.AnnotationsResult.Annotation> annotationsList =
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

        public boolean isError() { return isError.get(); }

        public String getErrorMessage() {
            if (!errorMessageList.isEmpty()) {
                return errorMessageList.get(0);
            } else {
                return "";
            }
        }

        public List<QueryAnnotationsResponse.AnnotationsResult.Annotation> getAnnotationsList() {
            return annotationsList;
        }

        @Override
        public void onNext(QueryAnnotationsResponse response) {

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

                assertTrue(response.hasAnnotationsResult());
                List<QueryAnnotationsResponse.AnnotationsResult.Annotation> responseAnnotationList =
                        response.getAnnotationsResult().getAnnotationsList();

                // flag error if already received a response
                if (!annotationsList.isEmpty()) {
                    final String errorMsg = "onNext received more than one response";
                    System.err.println(errorMsg);
                    isError.set(true);
                    errorMessageList.add(errorMsg);

                } else {
                    annotationsList.addAll(responseAnnotationList);
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
                final String errorMsg = "onError error: " + status;
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

    public static class ExportDataResponseObserver implements StreamObserver<ExportDataResponse> {

        // instance variables
        private final CountDownLatch finishLatch = new CountDownLatch(1);
        private final AtomicBoolean isError = new AtomicBoolean(false);
        private final List<String> errorMessageList = Collections.synchronizedList(new ArrayList<>());
        private final List<ExportDataResponse.ExportDataResult> resultList =
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

        public boolean isError() { return isError.get(); }

        public String getErrorMessage() {
            if (!errorMessageList.isEmpty()) {
                return errorMessageList.get(0);
            } else {
                return "";
            }
        }

        public ExportDataResponse.ExportDataResult getResult() {
            if (!resultList.isEmpty()) {
                return resultList.get(0);
            } else {
                return null;
            }
        }

        @Override
        public void onNext(ExportDataResponse response) {

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

                assertTrue(response.hasExportDataResult());
                final ExportDataResponse.ExportDataResult result = response.getExportDataResult();
                assertNotNull(result);

                // flag error if already received a response
                if (!resultList.isEmpty()) {
                    final String errorMsg = "onNext received more than one response";
                    System.err.println(errorMsg);
                    isError.set(true);
                    errorMessageList.add(errorMsg);

                } else {
                    resultList.add(result);
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
                final String errorMsg = "onError error: " + status;
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

    public static SaveDataSetRequest buildSaveDataSetRequest(SaveDataSetParams params) {

        DataSet.Builder dataSetBuilder
                = DataSet.newBuilder();

        for (AnnotationDataBlock block : params.dataSet.dataBlocks) {

            Timestamp.Builder beginTimeBuilder = Timestamp.newBuilder();
            beginTimeBuilder.setEpochSeconds(block.beginSeconds);
            beginTimeBuilder.setNanoseconds(block.beginNanos);

            Timestamp.Builder endTimeBuilder = Timestamp.newBuilder();
            endTimeBuilder.setEpochSeconds(block.endSeconds);
            endTimeBuilder.setNanoseconds(block.endNanos);

            DataBlock.Builder dataBlockBuilder
                    = DataBlock.newBuilder();
            dataBlockBuilder.setBeginTime(beginTimeBuilder);
            dataBlockBuilder.setEndTime(endTimeBuilder);
            dataBlockBuilder.addAllPvNames(block.pvNames);
            dataBlockBuilder.build();

            dataSetBuilder.addDataBlocks(dataBlockBuilder);
        }

        if (params.dataSet.id != null) {
            dataSetBuilder.setId(params.dataSet.id);
        }

        dataSetBuilder.setName(params.dataSet.name);
        dataSetBuilder.setDescription(params.dataSet.description);
        dataSetBuilder.setOwnerId(params.dataSet.ownerId);

        dataSetBuilder.build();

        SaveDataSetRequest.Builder requestBuilder = SaveDataSetRequest.newBuilder();
        requestBuilder.setDataSet(dataSetBuilder);

        return requestBuilder.build();
    }

    public static QueryDataSetsRequest buildQueryDataSetsRequest(
            QueryDataSetsParams params
    ) {
        QueryDataSetsRequest.Builder requestBuilder = QueryDataSetsRequest.newBuilder();

        // add id criteria
        if (params.idCriterion != null) {
            QueryDataSetsRequest.QueryDataSetsCriterion.IdCriterion idCriterion =
                    QueryDataSetsRequest.QueryDataSetsCriterion.IdCriterion.newBuilder()
                            .setId(params.idCriterion)
                            .build();
            QueryDataSetsRequest.QueryDataSetsCriterion idQueryDataSetsCriterion =
                    QueryDataSetsRequest.QueryDataSetsCriterion.newBuilder()
                            .setIdCriterion(idCriterion)
                            .build();
            requestBuilder.addCriteria(idQueryDataSetsCriterion);
        }

        // add owner criteria
        if (params.ownerCriterion != null) {
            QueryDataSetsRequest.QueryDataSetsCriterion.OwnerCriterion ownerCriterion =
                    QueryDataSetsRequest.QueryDataSetsCriterion.OwnerCriterion.newBuilder()
                            .setOwnerId(params.ownerCriterion)
                            .build();
            QueryDataSetsRequest.QueryDataSetsCriterion ownerQueryDataSetsCriterion =
                    QueryDataSetsRequest.QueryDataSetsCriterion.newBuilder()
                            .setOwnerCriterion(ownerCriterion)
                            .build();
            requestBuilder.addCriteria(ownerQueryDataSetsCriterion);
        }

        // add description criteria
        if (params.textCriterion != null) {
            QueryDataSetsRequest.QueryDataSetsCriterion.TextCriterion textCriterion =
                    QueryDataSetsRequest.QueryDataSetsCriterion.TextCriterion.newBuilder()
                            .setText(params.textCriterion)
                            .build();
            QueryDataSetsRequest.QueryDataSetsCriterion descriptionQueryDataSetsCriterion =
                    QueryDataSetsRequest.QueryDataSetsCriterion.newBuilder()
                            .setTextCriterion(textCriterion)
                            .build();
            requestBuilder.addCriteria(descriptionQueryDataSetsCriterion);
        }

        // add pvName criteria
        if (params.pvNameCriterion != null) {
            QueryDataSetsRequest.QueryDataSetsCriterion.PvNameCriterion pvNameCriterion =
                    QueryDataSetsRequest.QueryDataSetsCriterion.PvNameCriterion.newBuilder()
                            .setName(params.pvNameCriterion)
                            .build();
            QueryDataSetsRequest.QueryDataSetsCriterion pvNameQueryDataSetsCriterion =
                    QueryDataSetsRequest.QueryDataSetsCriterion.newBuilder()
                            .setPvNameCriterion(pvNameCriterion)
                            .build();
            requestBuilder.addCriteria(pvNameQueryDataSetsCriterion);
        }

        return requestBuilder.build();
    }

    public static SaveAnnotationRequest buildSaveAnnotationRequest(SaveAnnotationRequestParams params) {

        SaveAnnotationRequest.Builder requestBuilder = SaveAnnotationRequest.newBuilder();

        if (params.id != null) {
            requestBuilder.setId(params.id);
        }

        // handle required annotation fields
        requestBuilder.setOwnerId(params.ownerId);
        requestBuilder.addAllDataSetIds(params.dataSetIds);
        requestBuilder.setName(params.name);

        // handle optional annotation fields
        if (params.annotationIds != null) {
            requestBuilder.addAllAnnotationIds(params.annotationIds);
        }
        if (params.comment != null) {
            requestBuilder.setComment(params.comment);
        }
        if (params.tags != null) {
            requestBuilder.addAllTags(params.tags);
        }
        if (params.attributeMap != null) {
            requestBuilder.addAllAttributes(AttributesUtility.attributeListFromMap(params.attributeMap));
        }
        if (params.calculations != null) {
            requestBuilder.setCalculations(params.calculations);
        }

        return requestBuilder.build();
    }

    public static QueryAnnotationsRequest buildQueryAnnotationsRequest(
            final QueryAnnotationsParams params
    ) {
        QueryAnnotationsRequest.Builder requestBuilder = QueryAnnotationsRequest.newBuilder();

        // handle IdCriterion
        if (params.idCriterion != null) {
            QueryAnnotationsRequest.QueryAnnotationsCriterion.IdCriterion idCriterion =
                    QueryAnnotationsRequest.QueryAnnotationsCriterion.IdCriterion.newBuilder()
                            .setId(params.idCriterion)
                            .build();
            QueryAnnotationsRequest.QueryAnnotationsCriterion idQueryAnnotationsCriterion =
                    QueryAnnotationsRequest.QueryAnnotationsCriterion.newBuilder()
                            .setIdCriterion(idCriterion)
                            .build();
            requestBuilder.addCriteria(idQueryAnnotationsCriterion);
        }

        // handle OwnerCriterion
        if (params.ownerCriterion != null) {
            QueryAnnotationsRequest.QueryAnnotationsCriterion.OwnerCriterion ownerCriterion =
                    QueryAnnotationsRequest.QueryAnnotationsCriterion.OwnerCriterion.newBuilder()
                            .setOwnerId(params.ownerCriterion)
                            .build();
            QueryAnnotationsRequest.QueryAnnotationsCriterion ownerQueryAnnotationsCriterion =
                    QueryAnnotationsRequest.QueryAnnotationsCriterion.newBuilder()
                            .setOwnerCriterion(ownerCriterion)
                            .build();
            requestBuilder.addCriteria(ownerQueryAnnotationsCriterion);
        }

        // handle DataSetsCriterion
        if (params.datasetsCriterion != null) {
            QueryAnnotationsRequest.QueryAnnotationsCriterion.DataSetsCriterion dataSetsCriterion =
                    QueryAnnotationsRequest.QueryAnnotationsCriterion.DataSetsCriterion.newBuilder()
                            .setDataSetId(params.datasetsCriterion)
                            .build();
            QueryAnnotationsRequest.QueryAnnotationsCriterion datasetIdQueryAnnotationsCriterion =
                    QueryAnnotationsRequest.QueryAnnotationsCriterion.newBuilder()
                            .setDataSetsCriterion(dataSetsCriterion)
                            .build();
            requestBuilder.addCriteria(datasetIdQueryAnnotationsCriterion);
        }

        // handle AnnotationsCriterion
        if (params.annotationsCriterion != null) {
            QueryAnnotationsRequest.QueryAnnotationsCriterion.AnnotationsCriterion annotationsCriterion =
                    QueryAnnotationsRequest.QueryAnnotationsCriterion.AnnotationsCriterion.newBuilder()
                            .setAnnotationId(params.annotationsCriterion)
                            .build();
            QueryAnnotationsRequest.QueryAnnotationsCriterion associatedAnnotationQueryAnnotationsCriterion =
                    QueryAnnotationsRequest.QueryAnnotationsCriterion.newBuilder()
                            .setAnnotationsCriterion(annotationsCriterion)
                            .build();
            requestBuilder.addCriteria(associatedAnnotationQueryAnnotationsCriterion);
        }

        // handle TextCriterion
        if (params.textCriterion != null) {
            QueryAnnotationsRequest.QueryAnnotationsCriterion.TextCriterion textCriterion =
                    QueryAnnotationsRequest.QueryAnnotationsCriterion.TextCriterion.newBuilder()
                            .setText(params.textCriterion)
                            .build();
            QueryAnnotationsRequest.QueryAnnotationsCriterion commentQueryAnnotationsCriterion =
                    QueryAnnotationsRequest.QueryAnnotationsCriterion.newBuilder()
                            .setTextCriterion(textCriterion)
                            .build();
            requestBuilder.addCriteria(commentQueryAnnotationsCriterion);
        }

        // handle TagsCriterion
        if (params.tagsCriterion != null) {
            QueryAnnotationsRequest.QueryAnnotationsCriterion.TagsCriterion tagsCriterion =
                    QueryAnnotationsRequest.QueryAnnotationsCriterion.TagsCriterion.newBuilder()
                            .setTagValue(params.tagsCriterion)
                            .build();
            QueryAnnotationsRequest.QueryAnnotationsCriterion tagsQueryAnnotationsCriterion =
                    QueryAnnotationsRequest.QueryAnnotationsCriterion.newBuilder()
                            .setTagsCriterion(tagsCriterion)
                            .build();
            requestBuilder.addCriteria(tagsQueryAnnotationsCriterion);
        }

        // handle AttributesCriterion
        if (params.attributesCriterionKey != null) {
            assertNotNull(params.attributesCriterionValue);
            QueryAnnotationsRequest.QueryAnnotationsCriterion.AttributesCriterion attributesCriterion =
                    QueryAnnotationsRequest.QueryAnnotationsCriterion.AttributesCriterion.newBuilder()
                            .setKey(params.attributesCriterionKey)
                            .setValue(params.attributesCriterionValue)
                            .build();
            QueryAnnotationsRequest.QueryAnnotationsCriterion attributesQueryAnnotationsCriterion =
                    QueryAnnotationsRequest.QueryAnnotationsCriterion.newBuilder()
                            .setAttributesCriterion(attributesCriterion)
                            .build();
            requestBuilder.addCriteria(attributesQueryAnnotationsCriterion);
        }

        return requestBuilder.build();
    }

    public static ExportDataRequest buildExportDataRequest(
            String dataSetId,
            CalculationsSpec calculationsSpec,
            ExportDataRequest.ExportOutputFormat outputFormat
    ) {
        ExportDataRequest.Builder requestBuilder = ExportDataRequest.newBuilder();

        // set datasetId if specified
        if (dataSetId != null) {
            requestBuilder.setDataSetId(dataSetId);
        }

        // create calculationsSpec if calculationsId is specified
        if (calculationsSpec != null) {
            requestBuilder.setCalculationsSpec(calculationsSpec);
        }

        // set output format
        requestBuilder.setOutputFormat(outputFormat);

        return requestBuilder.build();
    }

    public static void verifyDatasetHdf5Content(IHDF5Reader reader, DataSetDocument dataset) {

        // verify dataset paths
        final String datasetGroup = PATH_SEPARATOR
                + GROUP_DATASET;
        assertTrue(reader.object().isGroup(datasetGroup));

        final String dataBlocksGroup = PATH_SEPARATOR
                + GROUP_DATASET
                + PATH_SEPARATOR
                + GROUP_DATA_BLOCKS;
        assertTrue(reader.object().isGroup(dataBlocksGroup));

        // verify dataset contents
        int dataBlockIndex = 0;
        for (DataBlockDocument dataBlock : dataset.getDataBlocks()) {
            final String dataBlockIndexGroup = dataBlocksGroup
                    + PATH_SEPARATOR
                    + dataBlockIndex;
            assertTrue(reader.object().isGroup(dataBlockIndexGroup));
            final String dataBlockPathBase = dataBlockIndexGroup + PATH_SEPARATOR;
            final String pvNameListPath = dataBlockPathBase + DATASET_BLOCK_PV_NAME_LIST;
            assertArrayEquals(dataBlock.getPvNames().toArray(new String[0]), reader.readStringArray(pvNameListPath));
            final String beginTimeSecondsPath = dataBlockPathBase + DATASET_BLOCK_BEGIN_SECONDS;
            assertEquals(dataBlock.getBeginTime().getSeconds(), reader.readLong(beginTimeSecondsPath));
            final String beginTimeNanosPath = dataBlockPathBase + DATASET_BLOCK_BEGIN_NANOS;
            assertEquals(dataBlock.getBeginTime().getNanos(), reader.readLong(beginTimeNanosPath));
            final String endTimeSecondsPath = dataBlockPathBase + DATASET_BLOCK_END_SECONDS;
            assertEquals(dataBlock.getEndTime().getSeconds(), reader.readLong(endTimeSecondsPath));
            final String endTimeNanosPath = dataBlockPathBase + DATASET_BLOCK_END_NANOS;
            assertEquals(dataBlock.getEndTime().getNanos(), reader.readLong(endTimeNanosPath));
            dataBlockIndex = dataBlockIndex + 1;
        }
    }

    public static void verifyBucketDocumentHdf5Content(IHDF5Reader reader, BucketDocument bucketDocument) {

        final String firstSecondsString =
                String.format("%012d", bucketDocument.getDataTimestamps().getFirstTime().getSeconds());
        final String firstNanosString =
                String.format("%012d", bucketDocument.getDataTimestamps().getFirstTime().getNanos());

        // check paths for pv index
        final String pvsPath = PATH_SEPARATOR + GROUP_PVS;
        final String pvPath = pvsPath + PATH_SEPARATOR + bucketDocument.getPvName();
        assertTrue(reader.object().isGroup(pvPath));
        final String pvBucketPath = pvPath
                + PATH_SEPARATOR
                + GROUP_TIMES
                + PATH_SEPARATOR
                + firstSecondsString
                + PATH_SEPARATOR
                + firstNanosString;
        assertTrue(reader.object().isGroup(pvBucketPath));

        // verify dataset contents accessed via pv index
        verifyBucketDocumentHdf5ContentViaPath(reader, pvBucketPath, bucketDocument);

        // check paths for time index
        final String timesPath = PATH_SEPARATOR + GROUP_TIMES;
        final String timeBucketPath = timesPath
                + PATH_SEPARATOR
                + firstSecondsString
                + PATH_SEPARATOR
                + firstNanosString
                + PATH_SEPARATOR
                + GROUP_PVS
                + PATH_SEPARATOR
                + bucketDocument.getPvName();
        assertTrue(reader.object().isGroup(timeBucketPath));

        // verify dataset contents accessed via time index
        verifyBucketDocumentHdf5ContentViaPath(reader, timeBucketPath, bucketDocument);
    }

    public static void verifyBucketDocumentHdf5ContentViaPath(
            IHDF5Reader reader,
            String pvBucketPath,
            BucketDocument bucketDocument
    ) {
        // verify dataset contents for first seconds/nanos/time
        final String firstSecondsPath = pvBucketPath + PATH_SEPARATOR + DATASET_FIRST_SECONDS;
        assertEquals(
                bucketDocument.getDataTimestamps().getFirstTime().getSeconds(),
                reader.readLong(firstSecondsPath));
        final String firstNanosPath = pvBucketPath + PATH_SEPARATOR + DATASET_FIRST_NANOS;
        assertEquals(
                bucketDocument.getDataTimestamps().getFirstTime().getNanos(),
                reader.readLong(firstNanosPath));
        final String firstTimePath = pvBucketPath + PATH_SEPARATOR + DATASET_FIRST_TIME;
        assertEquals(
                bucketDocument.getDataTimestamps().getFirstTime().getDateTime(),
                reader.time().readDate(firstTimePath));

        // verify dataset contents for first seconds/nanos/time
        final String lastSecondsPath = pvBucketPath + PATH_SEPARATOR + DATASET_LAST_SECONDS;
        assertEquals(
                bucketDocument.getDataTimestamps().getLastTime().getSeconds(),
                reader.readLong(lastSecondsPath));
        final String lastNanosPath = pvBucketPath + PATH_SEPARATOR + DATASET_LAST_NANOS;
        assertEquals(
                bucketDocument.getDataTimestamps().getLastTime().getNanos(),
                reader.readLong(lastNanosPath));
        final String lastTimePath = pvBucketPath + PATH_SEPARATOR + DATASET_LAST_TIME;
        assertEquals(
                bucketDocument.getDataTimestamps().getLastTime().getDateTime(),
                reader.time().readDate(lastTimePath));

        // sample period and count
        final String sampleCountPath = pvBucketPath + PATH_SEPARATOR + DATASET_SAMPLE_COUNT;
        assertEquals(
                bucketDocument.getDataTimestamps().getSampleCount(),
                reader.readInt(sampleCountPath));
        final String samplePeriodPath = pvBucketPath + PATH_SEPARATOR + DATASET_SAMPLE_PERIOD;
        assertEquals(
                bucketDocument.getDataTimestamps().getSamplePeriod(),
                reader.readLong(samplePeriodPath));

        // data column content as byte array
        final String columnDataPath = pvBucketPath + PATH_SEPARATOR + DATA_COLUMN_BYTES;
        Message documentProtobufColumn = bucketDocument.getDataColumn().toProtobufColumn();
        final byte[] fileBytes = reader.readAsByteArray(columnDataPath);
        assertArrayEquals(documentProtobufColumn.toByteArray(), fileBytes);

        // data column encoding
        final String columnEncodingPath = pvBucketPath + PATH_SEPARATOR + DATA_COLUMN_ENCODING;
        final String fileEncodingValue = reader.readString(columnEncodingPath);
        assertEquals(
                ENCODING_PROTO + ":" + documentProtobufColumn.getClass().getSimpleName(),
                fileEncodingValue);

        // test deserialization of encoded column
        try {
            Message fileProtobufColumn = null;
            switch (reader.readString(columnEncodingPath)) {
                case (ENCODING_PROTO + ":" + "DataColumn") -> {
                    fileProtobufColumn = DataColumn.parseFrom(fileBytes);
                }
                case (ENCODING_PROTO + ":" + "DoubleColumn") -> {
                    fileProtobufColumn = DoubleColumn.parseFrom(fileBytes);
                }
            }
            assertEquals(documentProtobufColumn, fileProtobufColumn);
        } catch (InvalidProtocolBufferException e) {
            fail("error parsing protobuf column: " + e.getMessage());
        }

        // dataTimestampsBytes
        final String dataTimestampsPath = pvBucketPath + PATH_SEPARATOR + DATA_TIMESTAMPS_BYTES;
        assertArrayEquals(
                bucketDocument.getDataTimestamps().getBytes(),
                reader.readAsByteArray(dataTimestampsPath));

        // tags
        final String tagsPath = pvBucketPath + PATH_SEPARATOR + DATASET_TAGS;
        if (bucketDocument.getTags() != null) {
            assertTrue(reader.object().exists(tagsPath));
            assertArrayEquals(
                    bucketDocument.getTags().toArray(new String[0]),
                    reader.readStringArray(tagsPath));
        } else {
            assertFalse(reader.object().exists(tagsPath));
        }

        // attributeMap - one array for keys and one for values
        final String attributeMapKeysPath = pvBucketPath + PATH_SEPARATOR + DATASET_ATTRIBUTE_MAP_KEYS;
        if (bucketDocument.getAttributes() != null) {
            assertTrue(reader.object().exists(attributeMapKeysPath));
            assertArrayEquals(
                    bucketDocument.getAttributes().keySet().toArray(new String[0]),
                    reader.readStringArray(attributeMapKeysPath));
            final String attributeMapValuesPath = pvBucketPath + PATH_SEPARATOR + DATASET_ATTRIBUTE_MAP_VALUES;
            assertArrayEquals(
                    bucketDocument.getAttributes().values().toArray(new String[0]),
                    reader.readStringArray(attributeMapValuesPath));
        } else {
            assertFalse(reader.object().exists(attributeMapKeysPath));
        }

        // providerId
        final String providerIdPath = pvBucketPath + PATH_SEPARATOR + DATASET_PROVIDER_ID;
        assertEquals(bucketDocument.getProviderId(), reader.readString(providerIdPath));

    }

    public static void verifyCalculationsDocumentHdf5Content(
            IHDF5Reader reader,
            CalculationsDocument calculationsDocument,
            Map<String, CalculationsSpec.ColumnNameList> frameColumnNamesMap
    ) {
        // verify group for calculations id
        final String calculationsIdGroup = GROUP_CALCULATIONS + PATH_SEPARATOR + calculationsDocument.getId().toString();
        assertTrue(reader.object().isGroup(calculationsIdGroup));

        // verify frame group
        final String framesGroup = calculationsIdGroup + PATH_SEPARATOR + GROUP_FRAMES;
        assertTrue(reader.object().isGroup(framesGroup));

        // verify contents for each frame in CalculationsDocument
        int frameIndex = 0;
        for (CalculationsDataFrameDocument calculationsDataFrameDocument : calculationsDocument.getDataFrames()) {

            if ((frameColumnNamesMap != null)
                    && ( ! frameColumnNamesMap.containsKey(calculationsDataFrameDocument.getName()))) {
                // skip frame if not specified in map
                continue;
            }

            // verify frame index group
            final String frameIndexGroup = framesGroup + PATH_SEPARATOR + frameIndex;
            assertTrue(reader.object().isGroup(frameIndexGroup));

            // verify frame name
            final String frameNamePath = frameIndexGroup + PATH_SEPARATOR + GROUP_NAME;
            final String frameName = reader.readString(frameNamePath);
            assertEquals(calculationsDataFrameDocument.getName(), frameName);

            // verify frame dataTimestampsBytes
            final String frameDataTimestampsBytesPath = frameIndexGroup + PATH_SEPARATOR + DATA_TIMESTAMPS_BYTES;
            assertArrayEquals(
                    calculationsDataFrameDocument.getDataTimestamps().getBytes(),
                    reader.readAsByteArray(frameDataTimestampsBytesPath));

            // verify columns group
            final String columnsGroup = frameIndexGroup + PATH_SEPARATOR + GROUP_COLUMNS;
            assertTrue(reader.object().isGroup(columnsGroup));

            // verify contents for each frame column
            int columnIndex = 0;
            for (DataColumnDocument calculationsDataColumnDocument : calculationsDataFrameDocument.getDataColumns()) {

                if ((frameColumnNamesMap != null)
                        && ( ! frameColumnNamesMap.get(frameName).getColumnNamesList().contains(
                                calculationsDataColumnDocument.getName()))) {
                    // skip column if not specified in map for frame;
                    continue;
                }

                // verify column index group
                final String columnIndexGroup = columnsGroup + PATH_SEPARATOR + columnIndex;
                assertTrue(reader.object().isGroup(columnIndexGroup));

                // verify column name
                final String columnNamePath = columnIndexGroup + PATH_SEPARATOR + GROUP_NAME;
                final String columnName = reader.readString(columnNamePath);
                assertEquals(calculationsDataColumnDocument.getName(), columnName);

                // verify dataColumnBytes
                final String dataColumnBytesPath = columnIndexGroup + PATH_SEPARATOR + DATA_COLUMN_BYTES;
                assertArrayEquals(
                        calculationsDataColumnDocument.toByteArray(),
                        reader.readAsByteArray(dataColumnBytesPath));

                columnIndex = columnIndex + 1;
            }

            if (frameColumnNamesMap != null) {
                assertEquals(columnIndex, frameColumnNamesMap.get(frameName).getColumnNamesList().size());
            }

            frameIndex = frameIndex + 1;
        }

        // check number of frames matches map size, if map is provided
        if (frameColumnNamesMap != null) {
            assertEquals(frameIndex, frameColumnNamesMap.size());
        }
    }

    // =========================================================================
    // PvMetadata response observers and request builders
    // =========================================================================

    public static class SavePvMetadataResponseObserver implements StreamObserver<SavePvMetadataResponse> {

        private final CountDownLatch finishLatch = new CountDownLatch(1);
        private final AtomicBoolean isError = new AtomicBoolean(false);
        private final List<String> errorMessageList = Collections.synchronizedList(new ArrayList<>());
        private final List<String> pvNameList = Collections.synchronizedList(new ArrayList<>());

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
            return errorMessageList.isEmpty() ? "" : errorMessageList.get(0);
        }

        public String getPvName() {
            return pvNameList.isEmpty() ? null : pvNameList.get(0);
        }

        @Override
        public void onNext(SavePvMetadataResponse response) {
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
                assertTrue(response.hasSavePvMetadataResult());
                final SavePvMetadataResponse.SavePvMetadataResult result = response.getSavePvMetadataResult();
                assertNotNull(result);
                if (!pvNameList.isEmpty()) {
                    final String errorMsg = "onNext received more than one response";
                    System.err.println(errorMsg);
                    isError.set(true);
                    errorMessageList.add(errorMsg);
                } else {
                    pvNameList.add(result.getPvName());
                    finishLatch.countDown();
                }
            }).start();
        }

        @Override
        public void onError(Throwable t) {
            new Thread(() -> {
                final Status status = Status.fromThrowable(t);
                final String errorMsg = "onError error: " + status;
                System.err.println(errorMsg);
                isError.set(true);
                errorMessageList.add(errorMsg);
                finishLatch.countDown();
            }).start();
        }

        @Override
        public void onCompleted() {}
    }

    public static class QueryPvMetadataResponseObserver implements StreamObserver<QueryPvMetadataResponse> {

        private final CountDownLatch finishLatch = new CountDownLatch(1);
        private final AtomicBoolean isError = new AtomicBoolean(false);
        private final List<String> errorMessageList = Collections.synchronizedList(new ArrayList<>());
        private final List<com.ospreydcs.dp.grpc.v1.common.PvMetadata> pvMetadataList =
                Collections.synchronizedList(new ArrayList<>());
        private String nextPageToken = "";

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
            return errorMessageList.isEmpty() ? "" : errorMessageList.get(0);
        }

        public List<com.ospreydcs.dp.grpc.v1.common.PvMetadata> getPvMetadataList() {
            return pvMetadataList;
        }

        public String getNextPageToken() { return nextPageToken; }

        @Override
        public void onNext(QueryPvMetadataResponse response) {
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
                assertTrue(response.hasPvMetadataResult());
                final QueryPvMetadataResponse.PvMetadataResult result = response.getPvMetadataResult();
                assertNotNull(result);
                pvMetadataList.addAll(result.getPvMetadataList());
                nextPageToken = result.getNextPageToken();
                finishLatch.countDown();
            }).start();
        }

        @Override
        public void onError(Throwable t) {
            new Thread(() -> {
                final Status status = Status.fromThrowable(t);
                final String errorMsg = "onError error: " + status;
                System.err.println(errorMsg);
                isError.set(true);
                errorMessageList.add(errorMsg);
                finishLatch.countDown();
            }).start();
        }

        @Override
        public void onCompleted() {}
    }

    public static class GetPvMetadataResponseObserver implements StreamObserver<GetPvMetadataResponse> {

        private final CountDownLatch finishLatch = new CountDownLatch(1);
        private final AtomicBoolean isError = new AtomicBoolean(false);
        private final List<String> errorMessageList = Collections.synchronizedList(new ArrayList<>());
        private final List<com.ospreydcs.dp.grpc.v1.common.PvMetadata> pvMetadataList =
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

        public boolean isError() { return isError.get(); }

        public String getErrorMessage() {
            return errorMessageList.isEmpty() ? "" : errorMessageList.get(0);
        }

        public com.ospreydcs.dp.grpc.v1.common.PvMetadata getPvMetadata() {
            return pvMetadataList.isEmpty() ? null : pvMetadataList.get(0);
        }

        @Override
        public void onNext(GetPvMetadataResponse response) {
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
                assertTrue(response.hasGetPvMetadataResult());
                final GetPvMetadataResponse.GetPvMetadataResult result = response.getGetPvMetadataResult();
                assertNotNull(result);
                pvMetadataList.add(result.getPvMetadata());
                finishLatch.countDown();
            }).start();
        }

        @Override
        public void onError(Throwable t) {
            new Thread(() -> {
                final Status status = Status.fromThrowable(t);
                final String errorMsg = "onError error: " + status;
                System.err.println(errorMsg);
                isError.set(true);
                errorMessageList.add(errorMsg);
                finishLatch.countDown();
            }).start();
        }

        @Override
        public void onCompleted() {}
    }

    public static class DeletePvMetadataResponseObserver implements StreamObserver<DeletePvMetadataResponse> {

        private final CountDownLatch finishLatch = new CountDownLatch(1);
        private final AtomicBoolean isError = new AtomicBoolean(false);
        private final List<String> errorMessageList = Collections.synchronizedList(new ArrayList<>());
        private final List<String> pvNameList = Collections.synchronizedList(new ArrayList<>());

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
            return errorMessageList.isEmpty() ? "" : errorMessageList.get(0);
        }

        public String getDeletedPvName() {
            return pvNameList.isEmpty() ? null : pvNameList.get(0);
        }

        @Override
        public void onNext(DeletePvMetadataResponse response) {
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
                assertTrue(response.hasDeletePvMetadataResult());
                final DeletePvMetadataResponse.DeletePvMetadataResult result = response.getDeletePvMetadataResult();
                assertNotNull(result);
                pvNameList.add(result.getPvName());
                finishLatch.countDown();
            }).start();
        }

        @Override
        public void onError(Throwable t) {
            new Thread(() -> {
                final Status status = Status.fromThrowable(t);
                final String errorMsg = "onError error: " + status;
                System.err.println(errorMsg);
                isError.set(true);
                errorMessageList.add(errorMsg);
                finishLatch.countDown();
            }).start();
        }

        @Override
        public void onCompleted() {}
    }

    public static class PatchPvMetadataResponseObserver
            implements StreamObserver<com.ospreydcs.dp.grpc.v1.annotation.PatchPvMetadataResponse> {

        private final CountDownLatch finishLatch = new CountDownLatch(1);
        private final AtomicBoolean isError = new AtomicBoolean(false);
        private final List<String> errorMessageList = Collections.synchronizedList(new ArrayList<>());

        public void await() {
            try {
                finishLatch.await(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                isError.set(true);
                errorMessageList.add("InterruptedException waiting for finishLatch");
            }
        }

        public boolean isError() { return isError.get(); }

        public String getErrorMessage() {
            return errorMessageList.isEmpty() ? "" : errorMessageList.get(0);
        }

        @Override
        public void onNext(com.ospreydcs.dp.grpc.v1.annotation.PatchPvMetadataResponse response) {
            if (response.hasExceptionalResult()) {
                isError.set(true);
                errorMessageList.add(response.getExceptionalResult().getMessage());
            }
            finishLatch.countDown();
        }

        @Override
        public void onError(Throwable t) {
            isError.set(true);
            errorMessageList.add("onError: " + Status.fromThrowable(t));
            finishLatch.countDown();
        }

        @Override
        public void onCompleted() {}
    }

    public static class BulkSavePvMetadataResponseObserver
            implements StreamObserver<com.ospreydcs.dp.grpc.v1.annotation.BulkSavePvMetadataResponse> {

        private final CountDownLatch finishLatch = new CountDownLatch(1);
        private final AtomicBoolean isError = new AtomicBoolean(false);
        private final List<String> errorMessageList = Collections.synchronizedList(new ArrayList<>());

        public void await() {
            try {
                finishLatch.await(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                isError.set(true);
                errorMessageList.add("InterruptedException waiting for finishLatch");
            }
        }

        public boolean isError() { return isError.get(); }

        public String getErrorMessage() {
            return errorMessageList.isEmpty() ? "" : errorMessageList.get(0);
        }

        @Override
        public void onNext(com.ospreydcs.dp.grpc.v1.annotation.BulkSavePvMetadataResponse response) {
            if (response.hasExceptionalResult()) {
                isError.set(true);
                errorMessageList.add(response.getExceptionalResult().getMessage());
            }
            finishLatch.countDown();
        }

        @Override
        public void onError(Throwable t) {
            isError.set(true);
            errorMessageList.add("onError: " + Status.fromThrowable(t));
            finishLatch.countDown();
        }

        @Override
        public void onCompleted() {}
    }

    public record SavePvMetadataParams(
            String pvName,
            List<String> aliases,
            List<String> tags,
            List<com.ospreydcs.dp.grpc.v1.common.Attribute> attributes,
            String description,
            String modifiedBy
    ) {}

    public static SavePvMetadataRequest buildSavePvMetadataRequest(SavePvMetadataParams params) {
        final SavePvMetadataRequest.Builder builder = SavePvMetadataRequest.newBuilder();
        if (params.pvName() != null) {
            builder.setPvName(params.pvName());
        }
        if (params.aliases() != null) {
            builder.addAllAliases(params.aliases());
        }
        if (params.tags() != null) {
            builder.addAllTags(params.tags());
        }
        if (params.attributes() != null) {
            builder.addAllAttributes(params.attributes());
        }
        if (params.description() != null) {
            builder.setDescription(params.description());
        }
        if (params.modifiedBy() != null) {
            builder.setModifiedBy(params.modifiedBy());
        }
        return builder.build();
    }

    public static QueryPvMetadataRequest buildQueryPvMetadataRequest(
            List<QueryPvMetadataRequest.QueryPvMetadataCriterion> criteria,
            int limit,
            String pageToken
    ) {
        final QueryPvMetadataRequest.Builder builder = QueryPvMetadataRequest.newBuilder();
        builder.addAllCriteria(criteria);
        if (limit > 0) {
            builder.setLimit(limit);
        }
        if (pageToken != null && !pageToken.isBlank()) {
            builder.setPageToken(pageToken);
        }
        return builder.build();
    }

    public static GetPvMetadataRequest buildGetPvMetadataRequest(String pvNameOrAlias) {
        return GetPvMetadataRequest.newBuilder()
                .setPvNameOrAlias(pvNameOrAlias)
                .build();
    }

    public static DeletePvMetadataRequest buildDeletePvMetadataRequest(String pvNameOrAlias) {
        return DeletePvMetadataRequest.newBuilder()
                .setPvNameOrAlias(pvNameOrAlias)
                .build();
    }

    // =========================================================================
    // Configuration response observers, params, and request builders
    // =========================================================================

    public static class SaveConfigurationResponseObserver implements StreamObserver<SaveConfigurationResponse> {

        private final CountDownLatch finishLatch = new CountDownLatch(1);
        private final AtomicBoolean isError = new AtomicBoolean(false);
        private final List<String> errorMessageList = Collections.synchronizedList(new ArrayList<>());
        private final List<String> configNameList = Collections.synchronizedList(new ArrayList<>());

        public void await() {
            try { finishLatch.await(1, TimeUnit.MINUTES); }
            catch (InterruptedException e) { isError.set(true); errorMessageList.add("await interrupted"); }
        }
        public boolean isError() { return isError.get(); }
        public String getErrorMessage() { return errorMessageList.isEmpty() ? "" : errorMessageList.get(0); }
        public String getConfigurationName() { return configNameList.isEmpty() ? null : configNameList.get(0); }

        @Override
        public void onNext(SaveConfigurationResponse response) {
            new Thread(() -> {
                if (response.hasExceptionalResult()) {
                    isError.set(true);
                    errorMessageList.add(response.getExceptionalResult().getMessage());
                    finishLatch.countDown();
                    return;
                }
                assertTrue(response.hasSaveConfigurationResult());
                configNameList.add(response.getSaveConfigurationResult().getConfigurationName());
                finishLatch.countDown();
            }).start();
        }
        @Override public void onError(Throwable t) {
            new Thread(() -> {
                isError.set(true); errorMessageList.add("onError: " + Status.fromThrowable(t)); finishLatch.countDown();
            }).start();
        }
        @Override public void onCompleted() {}
    }

    public static class GetConfigurationResponseObserver implements StreamObserver<GetConfigurationResponse> {

        private final CountDownLatch finishLatch = new CountDownLatch(1);
        private final AtomicBoolean isError = new AtomicBoolean(false);
        private final List<String> errorMessageList = Collections.synchronizedList(new ArrayList<>());
        private final List<com.ospreydcs.dp.grpc.v1.common.Configuration> configList =
                Collections.synchronizedList(new ArrayList<>());

        public void await() {
            try { finishLatch.await(1, TimeUnit.MINUTES); }
            catch (InterruptedException e) { isError.set(true); errorMessageList.add("await interrupted"); }
        }
        public boolean isError() { return isError.get(); }
        public String getErrorMessage() { return errorMessageList.isEmpty() ? "" : errorMessageList.get(0); }
        public com.ospreydcs.dp.grpc.v1.common.Configuration getConfiguration() {
            return configList.isEmpty() ? null : configList.get(0);
        }

        @Override
        public void onNext(GetConfigurationResponse response) {
            new Thread(() -> {
                if (response.hasExceptionalResult()) {
                    isError.set(true);
                    errorMessageList.add(response.getExceptionalResult().getMessage());
                    finishLatch.countDown();
                    return;
                }
                assertTrue(response.hasGetConfigurationResult());
                configList.add(response.getGetConfigurationResult().getConfiguration());
                finishLatch.countDown();
            }).start();
        }
        @Override public void onError(Throwable t) {
            new Thread(() -> {
                isError.set(true); errorMessageList.add("onError: " + Status.fromThrowable(t)); finishLatch.countDown();
            }).start();
        }
        @Override public void onCompleted() {}
    }

    public static class QueryConfigurationsResponseObserver implements StreamObserver<QueryConfigurationsResponse> {

        private final CountDownLatch finishLatch = new CountDownLatch(1);
        private final AtomicBoolean isError = new AtomicBoolean(false);
        private final List<String> errorMessageList = Collections.synchronizedList(new ArrayList<>());
        private final List<com.ospreydcs.dp.grpc.v1.common.Configuration> configList =
                Collections.synchronizedList(new ArrayList<>());
        private String nextPageToken = "";

        public void await() {
            try { finishLatch.await(1, TimeUnit.MINUTES); }
            catch (InterruptedException e) { isError.set(true); errorMessageList.add("await interrupted"); }
        }
        public boolean isError() { return isError.get(); }
        public String getErrorMessage() { return errorMessageList.isEmpty() ? "" : errorMessageList.get(0); }
        public List<com.ospreydcs.dp.grpc.v1.common.Configuration> getConfigurationList() { return configList; }
        public String getNextPageToken() { return nextPageToken; }

        @Override
        public void onNext(QueryConfigurationsResponse response) {
            new Thread(() -> {
                if (response.hasExceptionalResult()) {
                    isError.set(true);
                    errorMessageList.add(response.getExceptionalResult().getMessage());
                    finishLatch.countDown();
                    return;
                }
                assertTrue(response.hasQueryConfigurationsResult());
                configList.addAll(response.getQueryConfigurationsResult().getConfigurationsList());
                nextPageToken = response.getQueryConfigurationsResult().getNextPageToken();
                finishLatch.countDown();
            }).start();
        }
        @Override public void onError(Throwable t) {
            new Thread(() -> {
                isError.set(true); errorMessageList.add("onError: " + Status.fromThrowable(t)); finishLatch.countDown();
            }).start();
        }
        @Override public void onCompleted() {}
    }

    public static class DeleteConfigurationResponseObserver implements StreamObserver<DeleteConfigurationResponse> {

        private final CountDownLatch finishLatch = new CountDownLatch(1);
        private final AtomicBoolean isError = new AtomicBoolean(false);
        private final List<String> errorMessageList = Collections.synchronizedList(new ArrayList<>());
        private final List<String> configNameList = Collections.synchronizedList(new ArrayList<>());

        public void await() {
            try { finishLatch.await(1, TimeUnit.MINUTES); }
            catch (InterruptedException e) { isError.set(true); errorMessageList.add("await interrupted"); }
        }
        public boolean isError() { return isError.get(); }
        public String getErrorMessage() { return errorMessageList.isEmpty() ? "" : errorMessageList.get(0); }
        public String getConfigurationName() { return configNameList.isEmpty() ? null : configNameList.get(0); }

        @Override
        public void onNext(DeleteConfigurationResponse response) {
            new Thread(() -> {
                if (response.hasExceptionalResult()) {
                    isError.set(true);
                    errorMessageList.add(response.getExceptionalResult().getMessage());
                    finishLatch.countDown();
                    return;
                }
                assertTrue(response.hasDeleteConfigurationResult());
                configNameList.add(response.getDeleteConfigurationResult().getConfigurationName());
                finishLatch.countDown();
            }).start();
        }
        @Override public void onError(Throwable t) {
            new Thread(() -> {
                isError.set(true); errorMessageList.add("onError: " + Status.fromThrowable(t)); finishLatch.countDown();
            }).start();
        }
        @Override public void onCompleted() {}
    }

    public static class PatchConfigurationResponseObserver implements StreamObserver<PatchConfigurationResponse> {
        private final CountDownLatch finishLatch = new CountDownLatch(1);
        private final AtomicBoolean isError = new AtomicBoolean(false);
        private final List<String> errorMessageList = Collections.synchronizedList(new ArrayList<>());
        public void await() {
            try { finishLatch.await(1, TimeUnit.MINUTES); }
            catch (InterruptedException e) { isError.set(true); errorMessageList.add("await interrupted"); }
        }
        public boolean isError() { return isError.get(); }
        public String getErrorMessage() { return errorMessageList.isEmpty() ? "" : errorMessageList.get(0); }
        @Override public void onNext(PatchConfigurationResponse response) {
            if (response.hasExceptionalResult()) { isError.set(true); errorMessageList.add(response.getExceptionalResult().getMessage()); }
            finishLatch.countDown();
        }
        @Override public void onError(Throwable t) { isError.set(true); errorMessageList.add("onError: " + Status.fromThrowable(t)); finishLatch.countDown(); }
        @Override public void onCompleted() {}
    }

    public static class BulkSaveConfigurationResponseObserver implements StreamObserver<BulkSaveConfigurationResponse> {
        private final CountDownLatch finishLatch = new CountDownLatch(1);
        private final AtomicBoolean isError = new AtomicBoolean(false);
        private final List<String> errorMessageList = Collections.synchronizedList(new ArrayList<>());
        public void await() {
            try { finishLatch.await(1, TimeUnit.MINUTES); }
            catch (InterruptedException e) { isError.set(true); errorMessageList.add("await interrupted"); }
        }
        public boolean isError() { return isError.get(); }
        public String getErrorMessage() { return errorMessageList.isEmpty() ? "" : errorMessageList.get(0); }
        @Override public void onNext(BulkSaveConfigurationResponse response) {
            if (response.hasExceptionalResult()) { isError.set(true); errorMessageList.add(response.getExceptionalResult().getMessage()); }
            finishLatch.countDown();
        }
        @Override public void onError(Throwable t) { isError.set(true); errorMessageList.add("onError: " + Status.fromThrowable(t)); finishLatch.countDown(); }
        @Override public void onCompleted() {}
    }

    public record SaveConfigurationParams(
            String configurationName,
            String category,
            String description,
            String parentConfigurationName,
            List<String> tags,
            List<com.ospreydcs.dp.grpc.v1.common.Attribute> attributes,
            String modifiedBy
    ) {}

    public static SaveConfigurationRequest buildSaveConfigurationRequest(SaveConfigurationParams params) {
        final SaveConfigurationRequest.Builder builder = SaveConfigurationRequest.newBuilder();
        if (params.configurationName() != null) builder.setConfigurationName(params.configurationName());
        if (params.category() != null) builder.setCategory(params.category());
        if (params.description() != null) builder.setDescription(params.description());
        if (params.parentConfigurationName() != null) builder.setParentConfigurationName(params.parentConfigurationName());
        if (params.tags() != null) builder.addAllTags(params.tags());
        if (params.attributes() != null) builder.addAllAttributes(params.attributes());
        if (params.modifiedBy() != null) builder.setModifiedBy(params.modifiedBy());
        return builder.build();
    }

    public static GetConfigurationRequest buildGetConfigurationRequest(String configurationName) {
        return GetConfigurationRequest.newBuilder().setConfigurationName(configurationName).build();
    }

    public static QueryConfigurationsRequest buildQueryConfigurationsRequest(
            List<QueryConfigurationsRequest.QueryConfigurationsCriterion> criteria, int limit, String pageToken) {
        final QueryConfigurationsRequest.Builder builder = QueryConfigurationsRequest.newBuilder();
        builder.addAllCriteria(criteria);
        if (limit > 0) builder.setLimit(limit);
        if (pageToken != null && !pageToken.isBlank()) builder.setPageToken(pageToken);
        return builder.build();
    }

    public static DeleteConfigurationRequest buildDeleteConfigurationRequest(String configurationName) {
        return DeleteConfigurationRequest.newBuilder().setConfigurationName(configurationName).build();
    }

    // =========================================================================
    // Configuration Activation response observers, params, and request builders
    // =========================================================================

    public static class SaveConfigurationActivationResponseObserver
            implements StreamObserver<SaveConfigurationActivationResponse> {

        private final CountDownLatch finishLatch = new CountDownLatch(1);
        private final AtomicBoolean isError = new AtomicBoolean(false);
        private final List<String> errorMessageList = Collections.synchronizedList(new ArrayList<>());
        private final List<String> idList = Collections.synchronizedList(new ArrayList<>());

        public void await() {
            try { finishLatch.await(1, TimeUnit.MINUTES); }
            catch (InterruptedException e) { isError.set(true); errorMessageList.add("await interrupted"); }
        }
        public boolean isError() { return isError.get(); }
        public String getErrorMessage() { return errorMessageList.isEmpty() ? "" : errorMessageList.get(0); }
        public String getClientActivationId() { return idList.isEmpty() ? null : idList.get(0); }

        @Override
        public void onNext(SaveConfigurationActivationResponse response) {
            new Thread(() -> {
                if (response.hasExceptionalResult()) {
                    isError.set(true);
                    errorMessageList.add(response.getExceptionalResult().getMessage());
                    finishLatch.countDown();
                    return;
                }
                assertTrue(response.hasSaveConfigurationActivationResult());
                idList.add(response.getSaveConfigurationActivationResult().getClientActivationId());
                finishLatch.countDown();
            }).start();
        }
        @Override public void onError(Throwable t) {
            new Thread(() -> {
                isError.set(true); errorMessageList.add("onError: " + Status.fromThrowable(t)); finishLatch.countDown();
            }).start();
        }
        @Override public void onCompleted() {}
    }

    public static class GetConfigurationActivationResponseObserver
            implements StreamObserver<GetConfigurationActivationResponse> {

        private final CountDownLatch finishLatch = new CountDownLatch(1);
        private final AtomicBoolean isError = new AtomicBoolean(false);
        private final List<String> errorMessageList = Collections.synchronizedList(new ArrayList<>());
        private final List<com.ospreydcs.dp.grpc.v1.common.ConfigurationActivation> activationList =
                Collections.synchronizedList(new ArrayList<>());

        public void await() {
            try { finishLatch.await(1, TimeUnit.MINUTES); }
            catch (InterruptedException e) { isError.set(true); errorMessageList.add("await interrupted"); }
        }
        public boolean isError() { return isError.get(); }
        public String getErrorMessage() { return errorMessageList.isEmpty() ? "" : errorMessageList.get(0); }
        public com.ospreydcs.dp.grpc.v1.common.ConfigurationActivation getConfigurationActivation() {
            return activationList.isEmpty() ? null : activationList.get(0);
        }

        @Override
        public void onNext(GetConfigurationActivationResponse response) {
            new Thread(() -> {
                if (response.hasExceptionalResult()) {
                    isError.set(true);
                    errorMessageList.add(response.getExceptionalResult().getMessage());
                    finishLatch.countDown();
                    return;
                }
                assertTrue(response.hasGetConfigurationActivationResult());
                activationList.add(response.getGetConfigurationActivationResult().getConfigurationActivation());
                finishLatch.countDown();
            }).start();
        }
        @Override public void onError(Throwable t) {
            new Thread(() -> {
                isError.set(true); errorMessageList.add("onError: " + Status.fromThrowable(t)); finishLatch.countDown();
            }).start();
        }
        @Override public void onCompleted() {}
    }

    public static class QueryConfigurationActivationsResponseObserver
            implements StreamObserver<QueryConfigurationActivationsResponse> {

        private final CountDownLatch finishLatch = new CountDownLatch(1);
        private final AtomicBoolean isError = new AtomicBoolean(false);
        private final List<String> errorMessageList = Collections.synchronizedList(new ArrayList<>());
        private final List<com.ospreydcs.dp.grpc.v1.common.ConfigurationActivation> activationList =
                Collections.synchronizedList(new ArrayList<>());
        private String nextPageToken = "";

        public void await() {
            try { finishLatch.await(1, TimeUnit.MINUTES); }
            catch (InterruptedException e) { isError.set(true); errorMessageList.add("await interrupted"); }
        }
        public boolean isError() { return isError.get(); }
        public String getErrorMessage() { return errorMessageList.isEmpty() ? "" : errorMessageList.get(0); }
        public List<com.ospreydcs.dp.grpc.v1.common.ConfigurationActivation> getActivationList() { return activationList; }
        public String getNextPageToken() { return nextPageToken; }

        @Override
        public void onNext(QueryConfigurationActivationsResponse response) {
            new Thread(() -> {
                if (response.hasExceptionalResult()) {
                    isError.set(true);
                    errorMessageList.add(response.getExceptionalResult().getMessage());
                    finishLatch.countDown();
                    return;
                }
                assertTrue(response.hasQueryConfigurationActivationsResult());
                activationList.addAll(
                        response.getQueryConfigurationActivationsResult().getConfigurationActivationsList());
                nextPageToken = response.getQueryConfigurationActivationsResult().getNextPageToken();
                finishLatch.countDown();
            }).start();
        }
        @Override public void onError(Throwable t) {
            new Thread(() -> {
                isError.set(true); errorMessageList.add("onError: " + Status.fromThrowable(t)); finishLatch.countDown();
            }).start();
        }
        @Override public void onCompleted() {}
    }

    public static class DeleteConfigurationActivationResponseObserver
            implements StreamObserver<DeleteConfigurationActivationResponse> {

        private final CountDownLatch finishLatch = new CountDownLatch(1);
        private final AtomicBoolean isError = new AtomicBoolean(false);
        private final List<String> errorMessageList = Collections.synchronizedList(new ArrayList<>());
        private final List<String> idList = Collections.synchronizedList(new ArrayList<>());

        public void await() {
            try { finishLatch.await(1, TimeUnit.MINUTES); }
            catch (InterruptedException e) { isError.set(true); errorMessageList.add("await interrupted"); }
        }
        public boolean isError() { return isError.get(); }
        public String getErrorMessage() { return errorMessageList.isEmpty() ? "" : errorMessageList.get(0); }
        public String getClientActivationId() { return idList.isEmpty() ? null : idList.get(0); }

        @Override
        public void onNext(DeleteConfigurationActivationResponse response) {
            new Thread(() -> {
                if (response.hasExceptionalResult()) {
                    isError.set(true);
                    errorMessageList.add(response.getExceptionalResult().getMessage());
                    finishLatch.countDown();
                    return;
                }
                assertTrue(response.hasDeleteConfigurationActivationResult());
                idList.add(response.getDeleteConfigurationActivationResult().getClientActivationId());
                finishLatch.countDown();
            }).start();
        }
        @Override public void onError(Throwable t) {
            new Thread(() -> {
                isError.set(true); errorMessageList.add("onError: " + Status.fromThrowable(t)); finishLatch.countDown();
            }).start();
        }
        @Override public void onCompleted() {}
    }

    public static class GetActiveConfigurationsResponseObserver
            implements StreamObserver<GetActiveConfigurationsResponse> {

        private final CountDownLatch finishLatch = new CountDownLatch(1);
        private final AtomicBoolean isError = new AtomicBoolean(false);
        private final List<String> errorMessageList = Collections.synchronizedList(new ArrayList<>());
        private final List<com.ospreydcs.dp.grpc.v1.common.ConfigurationActivation> activationList =
                Collections.synchronizedList(new ArrayList<>());

        public void await() {
            try { finishLatch.await(1, TimeUnit.MINUTES); }
            catch (InterruptedException e) { isError.set(true); errorMessageList.add("await interrupted"); }
        }
        public boolean isError() { return isError.get(); }
        public String getErrorMessage() { return errorMessageList.isEmpty() ? "" : errorMessageList.get(0); }
        public List<com.ospreydcs.dp.grpc.v1.common.ConfigurationActivation> getActivationList() { return activationList; }

        @Override
        public void onNext(GetActiveConfigurationsResponse response) {
            new Thread(() -> {
                if (response.hasExceptionalResult()) {
                    isError.set(true);
                    errorMessageList.add(response.getExceptionalResult().getMessage());
                    finishLatch.countDown();
                    return;
                }
                assertTrue(response.hasGetActiveConfigurationsResult());
                activationList.addAll(
                        response.getGetActiveConfigurationsResult().getConfigurationActivationsList());
                finishLatch.countDown();
            }).start();
        }
        @Override public void onError(Throwable t) {
            new Thread(() -> {
                isError.set(true); errorMessageList.add("onError: " + Status.fromThrowable(t)); finishLatch.countDown();
            }).start();
        }
        @Override public void onCompleted() {}
    }

    public static class PatchConfigurationActivationResponseObserver
            implements StreamObserver<PatchConfigurationActivationResponse> {
        private final CountDownLatch finishLatch = new CountDownLatch(1);
        private final AtomicBoolean isError = new AtomicBoolean(false);
        private final List<String> errorMessageList = Collections.synchronizedList(new ArrayList<>());
        public void await() {
            try { finishLatch.await(1, TimeUnit.MINUTES); }
            catch (InterruptedException e) { isError.set(true); errorMessageList.add("await interrupted"); }
        }
        public boolean isError() { return isError.get(); }
        public String getErrorMessage() { return errorMessageList.isEmpty() ? "" : errorMessageList.get(0); }
        @Override public void onNext(PatchConfigurationActivationResponse response) {
            if (response.hasExceptionalResult()) { isError.set(true); errorMessageList.add(response.getExceptionalResult().getMessage()); }
            finishLatch.countDown();
        }
        @Override public void onError(Throwable t) { isError.set(true); errorMessageList.add("onError: " + Status.fromThrowable(t)); finishLatch.countDown(); }
        @Override public void onCompleted() {}
    }

    public static class BulkSaveConfigurationActivationResponseObserver
            implements StreamObserver<BulkSaveConfigurationActivationResponse> {
        private final CountDownLatch finishLatch = new CountDownLatch(1);
        private final AtomicBoolean isError = new AtomicBoolean(false);
        private final List<String> errorMessageList = Collections.synchronizedList(new ArrayList<>());
        public void await() {
            try { finishLatch.await(1, TimeUnit.MINUTES); }
            catch (InterruptedException e) { isError.set(true); errorMessageList.add("await interrupted"); }
        }
        public boolean isError() { return isError.get(); }
        public String getErrorMessage() { return errorMessageList.isEmpty() ? "" : errorMessageList.get(0); }
        @Override public void onNext(BulkSaveConfigurationActivationResponse response) {
            if (response.hasExceptionalResult()) { isError.set(true); errorMessageList.add(response.getExceptionalResult().getMessage()); }
            finishLatch.countDown();
        }
        @Override public void onError(Throwable t) { isError.set(true); errorMessageList.add("onError: " + Status.fromThrowable(t)); finishLatch.countDown(); }
        @Override public void onCompleted() {}
    }

    public record SaveConfigurationActivationParams(
            String clientActivationId,
            String configurationName,
            Timestamp startTime,
            Timestamp endTime,
            String description,
            List<String> tags,
            List<com.ospreydcs.dp.grpc.v1.common.Attribute> attributes,
            String modifiedBy
    ) {}

    public static SaveConfigurationActivationRequest buildSaveConfigurationActivationRequest(
            SaveConfigurationActivationParams params) {
        final SaveConfigurationActivationRequest.Builder builder =
                SaveConfigurationActivationRequest.newBuilder();
        if (params.clientActivationId() != null) builder.setClientActivationId(params.clientActivationId());
        if (params.configurationName() != null) builder.setConfigurationName(params.configurationName());
        if (params.startTime() != null) builder.setStartTime(params.startTime());
        if (params.endTime() != null) builder.setEndTime(params.endTime());
        if (params.description() != null) builder.setDescription(params.description());
        if (params.tags() != null) builder.addAllTags(params.tags());
        if (params.attributes() != null) builder.addAllAttributes(params.attributes());
        if (params.modifiedBy() != null) builder.setModifiedBy(params.modifiedBy());
        return builder.build();
    }

    public static GetConfigurationActivationRequest buildGetConfigurationActivationByIdRequest(
            String clientActivationId) {
        return GetConfigurationActivationRequest.newBuilder()
                .setClientActivationId(clientActivationId).build();
    }

    public static GetConfigurationActivationRequest buildGetConfigurationActivationByCompositeKeyRequest(
            String configurationName, Timestamp startTime) {
        return GetConfigurationActivationRequest.newBuilder()
                .setCompositeKey(
                        GetConfigurationActivationRequest.CompositeKey.newBuilder()
                                .setConfigurationName(configurationName)
                                .setStartTime(startTime)
                                .build())
                .build();
    }

    public static QueryConfigurationActivationsRequest buildQueryConfigurationActivationsRequest(
            List<QueryConfigurationActivationsRequest.QueryConfigurationActivationsCriterion> criteria,
            int limit, String pageToken) {
        final QueryConfigurationActivationsRequest.Builder builder =
                QueryConfigurationActivationsRequest.newBuilder();
        builder.addAllCriteria(criteria);
        if (limit > 0) builder.setLimit(limit);
        if (pageToken != null && !pageToken.isBlank()) builder.setPageToken(pageToken);
        return builder.build();
    }

    public static DeleteConfigurationActivationRequest buildDeleteConfigurationActivationByIdRequest(
            String clientActivationId) {
        return DeleteConfigurationActivationRequest.newBuilder()
                .setClientActivationId(clientActivationId).build();
    }

    public static DeleteConfigurationActivationRequest buildDeleteConfigurationActivationByCompositeKeyRequest(
            String configurationName, Timestamp startTime) {
        return DeleteConfigurationActivationRequest.newBuilder()
                .setCompositeKey(
                        DeleteConfigurationActivationRequest.CompositeKey.newBuilder()
                                .setConfigurationName(configurationName)
                                .setStartTime(startTime)
                                .build())
                .build();
    }

    public static GetActiveConfigurationsRequest buildGetActiveConfigurationsRequest(Timestamp timestamp) {
        return GetActiveConfigurationsRequest.newBuilder().setTimestamp(timestamp).build();
    }

}
