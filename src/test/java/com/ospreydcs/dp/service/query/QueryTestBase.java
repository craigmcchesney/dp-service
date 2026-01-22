package com.ospreydcs.dp.service.query;

import com.ospreydcs.dp.grpc.v1.common.DataBucket;
import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.query.*;
import com.ospreydcs.dp.grpc.v1.query.ProviderMetadata;
import com.ospreydcs.dp.service.query.benchmark.QueryBenchmarkBase;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class QueryTestBase {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    public record QueryDataRequestParams(
            List<String> columnNames,
            Long beginTimeSeconds,
            Long beginTimeNanos,
            Long endTimeSeconds,
            Long endTimeNanos,
            boolean useSerializedDataColumns
    ) {
    }

    public record QueryTableRequestParams(
            QueryTableRequest.TableResultFormat tableResultFormat,
            List<String> pvNameList,
            String pvNamePattern,
            Long beginTimeSeconds,
            Long beginTimeNanos,
            Long endTimeSeconds,
            Long endTimeNanos
    ) {
    }

    public static class QueryProvidersRequestParams {

        public String idCriterion = null;
        public String textCriterion = null;
        public String tagsCriterion = null;
        public String attributesCriterionKey = null;
        public String attributesCriterionValue = null;

        public void setIdCriterion(String idCriterion) {
            this.idCriterion = idCriterion;
        }

        public void setTextCriterion(String textCriterion) {
            this.textCriterion = textCriterion;
        }

        public void setTagsCriterion(String tagsCriterion) {
            this.tagsCriterion = tagsCriterion;
        }

        public void setAttributesCriterion(String attributeCriterionKey, String attributeCriterionValue) {
            this.attributesCriterionKey = attributeCriterionKey;
            this.attributesCriterionValue = attributeCriterionValue;
        }
    }

    public static QueryDataRequest buildQueryDataRequest(QueryDataRequestParams params) {
        
        // build API query request from params
        QueryDataRequest.Builder requestBuilder = QueryDataRequest.newBuilder();

        QueryDataRequest.QuerySpec.Builder querySpecBuilder = QueryDataRequest.QuerySpec.newBuilder();
        
        if (params.columnNames != null && !params.columnNames.isEmpty()) {
            querySpecBuilder.addAllPvNames(params.columnNames);
        }
        
        if (params.beginTimeSeconds != null) {
            final Timestamp.Builder beginTimeBuilder = Timestamp.newBuilder();
            beginTimeBuilder.setEpochSeconds(params.beginTimeSeconds);
            if (params.beginTimeNanos != null) beginTimeBuilder.setNanoseconds(params.beginTimeNanos);
            beginTimeBuilder.build();
            querySpecBuilder.setBeginTime(beginTimeBuilder);
        }
        
        if (params.endTimeSeconds != null) {
            final Timestamp.Builder endTimeBuilder = Timestamp.newBuilder();
            endTimeBuilder.setEpochSeconds(params.endTimeSeconds);
            if (params.endTimeNanos != null) endTimeBuilder.setNanoseconds(params.endTimeNanos);
            endTimeBuilder.build();
            querySpecBuilder.setEndTime(endTimeBuilder);
        }

        // specify whether to use SerializedDataColumns or regular DataColumns
//        querySpecBuilder.setUseSerializedDataColumns(params.useSerializedDataColumns);

        querySpecBuilder.build();
        requestBuilder.setQuerySpec(querySpecBuilder);

        return requestBuilder.build();
    }

    public static QueryTableRequest buildQueryTableRequest(QueryTableRequestParams params) {

        QueryTableRequest.Builder requestBuilder = QueryTableRequest.newBuilder();

        // set format
        if (params.tableResultFormat != null) {
            requestBuilder.setFormat(params.tableResultFormat);
        }

        // set pvNameList or PvNamePattern
        if (params.pvNameList != null && !params.pvNameList.isEmpty()) {
            PvNameList pvNameList = PvNameList.newBuilder()
                    .addAllPvNames(params.pvNameList)
                    .build();
            requestBuilder.setPvNameList(pvNameList);
        } else if (params.pvNamePattern != null && !params.pvNamePattern.isBlank()) {
            PvNamePattern pvNamePattern = PvNamePattern.newBuilder()
                    .setPattern(params.pvNamePattern)
                    .build();
            requestBuilder.setPvNamePattern(pvNamePattern);
        }

        // set begin time
        if (params.beginTimeSeconds != null) {
            final Timestamp.Builder beginTimeBuilder = Timestamp.newBuilder();
            beginTimeBuilder.setEpochSeconds(params.beginTimeSeconds);
            if (params.beginTimeNanos != null) beginTimeBuilder.setNanoseconds(params.beginTimeNanos);
            beginTimeBuilder.build();
            requestBuilder.setBeginTime(beginTimeBuilder);
        }

        // set end time
        if (params.endTimeSeconds != null) {
            final Timestamp.Builder endTimeBuilder = Timestamp.newBuilder();
            endTimeBuilder.setEpochSeconds(params.endTimeSeconds);
            if (params.endTimeNanos != null) endTimeBuilder.setNanoseconds(params.endTimeNanos);
            endTimeBuilder.build();
            requestBuilder.setEndTime(endTimeBuilder);
        }

        return requestBuilder.build();
    }

    public static QueryPvMetadataRequest buildQueryPvMetadataRequest(String columnNamePattern) {

        QueryPvMetadataRequest.Builder requestBuilder = QueryPvMetadataRequest.newBuilder();

        PvNamePattern.Builder pvNamePatternBuilder = PvNamePattern.newBuilder();
        pvNamePatternBuilder.setPattern(columnNamePattern);
        pvNamePatternBuilder.build();

        requestBuilder.setPvNamePattern(pvNamePatternBuilder);
        return requestBuilder.build();
    }

    public static QueryPvMetadataRequest buildQueryPvMetadataRequest(List<String> pvNames) {

        QueryPvMetadataRequest.Builder requestBuilder = QueryPvMetadataRequest.newBuilder();

        PvNameList.Builder pvNameListBuilder = PvNameList.newBuilder();
        pvNameListBuilder.addAllPvNames(pvNames);
        pvNameListBuilder.build();

        requestBuilder.setPvNameList(pvNameListBuilder);
        return requestBuilder.build();
    }

    public static QueryProviderMetadataRequest buildQueryProviderMetadataRequest(String providerId) {

        QueryProviderMetadataRequest.Builder requestBuilder = QueryProviderMetadataRequest.newBuilder();
        requestBuilder.setProviderId(providerId);
        return requestBuilder.build();
    }


    public static class QueryTableResponseObserver implements StreamObserver<QueryTableResponse> {

        private final CountDownLatch finishLatch = new CountDownLatch(1);
        private final AtomicBoolean isError = new AtomicBoolean(false);
        private final List<String> errorMessageList = Collections.synchronizedList(new ArrayList<>());
        private final List<QueryTableResponse> responseList = Collections.synchronizedList(new ArrayList<>());

        public void await() {
            try {
                finishLatch.await(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                System.err.println("InterruptedException waiting for finishLatch");
                isError.set(true);
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

        public QueryTableResponse getQueryResponse() {
            return responseList.get(0);
        }

        @Override
        public void onNext(QueryTableResponse response) {
            // handle response in separate thread to better simulate out of process grpc,
            // otherwise response is handled in same thread as service handler that sent it
            new Thread(() -> {

                if (response.hasExceptionalResult()) {
                    final String errorMsg = "onNext received exceptional response: "
                            + response.getExceptionalResult().getMessage();
                    System.err.println(errorMsg);
                    isError.set(true);
                    errorMessageList.add(errorMsg);
                }

                responseList.add(response);
                finishLatch.countDown();
                if (responseList.size() > 1) {
                    System.err.println("QueryTableResponseObserver onNext received more than one response");
                    isError.set(true);
                }

            }).start();

        }

        @Override
        public void onError(Throwable t) {
            // handle response in separate thread to better simulate out of process grpc,
            // otherwise response is handled in same thread as service handler that sent it
            new Thread(() -> {
                Status status = Status.fromThrowable(t);
                System.err.println("QueryTableResponseObserver error: " + status);
                isError.set(true);
            }).start();
        }

        @Override
        public void onCompleted() {
        }
    }

    public static class QueryDataResponseStreamObserver implements StreamObserver<QueryDataResponse> {

        private static enum ObserverType {
            UNARY,
            STREAM,
            BIDI_STREAM
        }

        private final CountDownLatch finishLatch = new CountDownLatch(1);
        private final AtomicBoolean isError = new AtomicBoolean(false);
        private final List<String> errorMessageList = Collections.synchronizedList(new ArrayList<>());
        private final List<DataBucket> dataBucketList = Collections.synchronizedList(new ArrayList<>());
        private StreamObserver<QueryDataRequest> requestObserver = null;
        private final ObserverType observerType;
        private final int numBucketsExpected;
        private final AtomicInteger numBucketsReceived = new AtomicInteger(0);

        public QueryDataResponseStreamObserver(ObserverType observerType) {
            this.observerType = observerType;
            this.numBucketsExpected = 0;
        }

        public QueryDataResponseStreamObserver(ObserverType observerType, int numBucketsExpected) {
            this.observerType = observerType;
            this.numBucketsExpected = numBucketsExpected;
        }

        public static QueryDataResponseStreamObserver newQueryDataStreamObserver() {
            return new QueryDataResponseStreamObserver(ObserverType.STREAM);
        }

        public static QueryDataResponseStreamObserver newQueryDataUnaryObserver() {
            return new QueryDataResponseStreamObserver(ObserverType.UNARY);
        }

        public static QueryDataResponseStreamObserver newQueryDataBidiStreamObserver(int numBucketsExpected) {
            return new QueryDataResponseStreamObserver(ObserverType.BIDI_STREAM, numBucketsExpected);
        }

        public void setRequestObserver(StreamObserver<QueryDataRequest> requestObserver) {
            this.requestObserver = requestObserver;
        }

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

        public List<DataBucket> getDataBucketList() {
            return dataBucketList;
        }

        @Override
        public void onNext(QueryDataResponse response) {
            
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

                assertTrue(response.hasQueryData());
                final List<DataBucket> responseBucketList =
                        response.getQueryData().getDataBucketsList();
                final int bucketsReceived = numBucketsReceived.addAndGet(responseBucketList.size());
                for (DataBucket bucket : responseBucketList) {
                    dataBucketList.add(bucket);
                }

                if (observerType == ObserverType.UNARY) {
                    // we only expect a single response if unary
                    finishLatch.countDown();
                    return;
                }

                if (observerType == ObserverType.BIDI_STREAM) {

                    if (bucketsReceived >= numBucketsExpected) {
                        // bidi stream received expected number of result buckets
                        finishLatch.countDown();
                        return;
                    } else {
                        logger.trace("requesting next query result response");
                        QueryDataRequest nextRequest = QueryBenchmarkBase.buildNextQueryDataRequest();
                        requestObserver.onNext(nextRequest);
                    }
                }

            }).start();
        }

        @Override
        public void onError(Throwable t) {
            // handle response in separate thread to better simulate out of process grpc,
            // otherwise response is handled in same thread as service handler that sent it
            new Thread(() -> {
                Status status = Status.fromThrowable(t);
                final String errorMsg = "onError: " + status;
                System.err.println(errorMsg);
                isError.set(true);
                errorMessageList.add(errorMsg);
                finishLatch.countDown();
            }).start();
        }

        @Override
        public void onCompleted() {
            // handle response in separate thread to better simulate out of process grpc,
            // otherwise response is handled in same thread as service handler that sent it
            new Thread(() -> {
                if (observerType == ObserverType.STREAM) {
                    finishLatch.countDown();
                }
            }).start();
        }
    }

    public static class QueryPvMetadataResponseObserver implements StreamObserver<QueryPvMetadataResponse> {

        // instance variables
        private final CountDownLatch finishLatch = new CountDownLatch(1);
        private final AtomicBoolean isError = new AtomicBoolean(false);
        private final List<String> errorMessageList = Collections.synchronizedList(new ArrayList<>());
        private final List<QueryPvMetadataResponse.MetadataResult.PvInfo> pvInfoList =
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

        public List<QueryPvMetadataResponse.MetadataResult.PvInfo> getPvInfoList() {
            return pvInfoList;
        }

        @Override
        public void onNext(QueryPvMetadataResponse response) {

            // handle response in separate thread to better simulate out of process grpc,
            // otherwise response is handled in same thread as service handler that sent it
            new Thread(() -> {

                if (response.hasExceptionalResult()) {
                    final String errorMsg = "QueryResponseColumnInfoObserver onNext received exception response: "
                            + response.getExceptionalResult().getMessage();
                    System.err.println(errorMsg);
                    isError.set(true);
                    errorMessageList.add(errorMsg);
                    finishLatch.countDown();
                    return;
                }

                assertTrue(response.hasMetadataResult());
                final QueryPvMetadataResponse.MetadataResult metadataResult = response.getMetadataResult();
                assertNotNull(metadataResult);
                // assertTrue(metadataResult.getPvInfosCount() > 0); - MAYBE ALLOW AN EMPTY RESULT FOR NEGATIVE TEST CASE?

                // flag error if already received a response
                if (!pvInfoList.isEmpty()) {
                    final String errorMsg = "QueryResponseColumnInfoObserver onNext received more than one response";
                    System.err.println(errorMsg);
                    isError.set(true);
                    errorMessageList.add(errorMsg);

                } else {
                    for (QueryPvMetadataResponse.MetadataResult.PvInfo pvInfo :
                            response.getMetadataResult().getPvInfosList()) {
                        pvInfoList.add(pvInfo);
                    }
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
                final String errorMsg = "QueryResponseColumnInfoObserver error: " + status;
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

    public static class QueryProvidersResponseObserver implements StreamObserver<QueryProvidersResponse> {

        // instance variables
        private final CountDownLatch finishLatch = new CountDownLatch(1);
        private final AtomicBoolean isError = new AtomicBoolean(false);
        private final List<String> errorMessageList = Collections.synchronizedList(new ArrayList<>());
        private final List<QueryProvidersResponse.ProvidersResult.ProviderInfo> providerInfoList =
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

        public List<QueryProvidersResponse.ProvidersResult.ProviderInfo> getProviderInfoList() {
            return providerInfoList;
        }

        @Override
        public void onNext(QueryProvidersResponse response) {

            // handle response in separate thread to better simulate out of process grpc,
            // otherwise response is handled in same thread as service handler that sent it
            new Thread(() -> {

                if (response.hasExceptionalResult()) {
                    final String errorMsg = "onNext received ExceptionalResult: "
                            + response.getExceptionalResult().getMessage();
                    System.err.println(errorMsg);
                    isError.set(true);
                    errorMessageList.add(errorMsg);
                    finishLatch.countDown();
                    return;
                }

                assertTrue(response.hasProvidersResult());
                final QueryProvidersResponse.ProvidersResult providersResult = response.getProvidersResult();
                assertNotNull(providersResult);

                // flag error if already received a response
                if (!providerInfoList.isEmpty()) {
                    final String errorMsg = "onNext received more than one response";
                    System.err.println(errorMsg);
                    isError.set(true);
                    errorMessageList.add(errorMsg);

                } else {
                    providerInfoList.addAll(response.getProvidersResult().getProviderInfosList());
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

    public static class QueryProviderMetadataResponseObserver implements StreamObserver<QueryProviderMetadataResponse> {

        // instance variables
        private final CountDownLatch finishLatch = new CountDownLatch(1);
        private final AtomicBoolean isError = new AtomicBoolean(false);
        private final List<String> errorMessageList = Collections.synchronizedList(new ArrayList<>());
        private final List<ProviderMetadata> providerMetadataList =
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

        public List<ProviderMetadata> getProviderMetadataList() {
            return providerMetadataList;
        }

        @Override
        public void onNext(QueryProviderMetadataResponse response) {

            // handle response in separate thread to better simulate out of process grpc,
            // otherwise response is handled in same thread as service handler that sent it
            new Thread(() -> {

                if (response.hasExceptionalResult()) {
                    final String errorMsg = "onNext received ExceptionalResult: "
                            + response.getExceptionalResult().getMessage();
                    System.err.println(errorMsg);
                    isError.set(true);
                    errorMessageList.add(errorMsg);
                    finishLatch.countDown();
                    return;
                }

                assertTrue(response.hasMetadataResult());
                final QueryProviderMetadataResponse.MetadataResult metadataResult = response.getMetadataResult();
                assertNotNull(metadataResult);

                // flag error if already received a response
                if (!providerMetadataList.isEmpty()) {
                    final String errorMsg = "onNext received more than one response";
                    System.err.println(errorMsg);
                    isError.set(true);
                    errorMessageList.add(errorMsg);

                } else {
                    providerMetadataList.addAll(response.getMetadataResult().getProviderMetadatasList());
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

    public static QueryProvidersRequest buildQueryProvidersRequest(QueryProvidersRequestParams params) {

        QueryProvidersRequest.Builder requestBuilder = QueryProvidersRequest.newBuilder();

        if (params.idCriterion != null) {
            QueryProvidersRequest.Criterion.IdCriterion criterion =
                    QueryProvidersRequest.Criterion.IdCriterion.newBuilder()
                            .setId(params.idCriterion)
                            .build();
            QueryProvidersRequest.Criterion criteria = QueryProvidersRequest.Criterion.newBuilder()
                    .setIdCriterion(criterion)
                    .build();
            requestBuilder.addCriteria(criteria);
        }

        if (params.textCriterion != null) {
            QueryProvidersRequest.Criterion.TextCriterion criterion =
                    QueryProvidersRequest.Criterion.TextCriterion.newBuilder()
                            .setText(params.textCriterion)
                            .build();
            QueryProvidersRequest.Criterion criteria = QueryProvidersRequest.Criterion.newBuilder()
                    .setTextCriterion(criterion)
                    .build();
            requestBuilder.addCriteria(criteria);
        }

        if (params.tagsCriterion != null) {
            QueryProvidersRequest.Criterion.TagsCriterion criterion =
                    QueryProvidersRequest.Criterion.TagsCriterion.newBuilder()
                            .setTagValue(params.tagsCriterion)
                            .build();
            QueryProvidersRequest.Criterion criteria = QueryProvidersRequest.Criterion.newBuilder()
                    .setTagsCriterion(criterion)
                    .build();
            requestBuilder.addCriteria(criteria);
        }

        if (params.attributesCriterionKey != null && params.attributesCriterionValue != null) {
            QueryProvidersRequest.Criterion.AttributesCriterion criterion =
                    QueryProvidersRequest.Criterion.AttributesCriterion.newBuilder()
                            .setKey(params.attributesCriterionKey)
                            .setValue(params.attributesCriterionValue)
                            .build();
            QueryProvidersRequest.Criterion criteria = QueryProvidersRequest.Criterion.newBuilder()
                    .setAttributesCriterion(criterion)
                    .build();
            requestBuilder.addCriteria(criteria);
        }

        return requestBuilder.build();
    }

    public static void verifyDataBucket(
            DataBucket responseBucket,
            DataColumn requestColumn,
            long startSeconds,
            long startNanos,
            long samplePeriod,
            int numSamples
    ) {
        assertEquals(
                startSeconds,
                responseBucket.getDataTimestamps().getSamplingClock().getStartTime().getEpochSeconds());
        assertEquals(
                startNanos,
                responseBucket.getDataTimestamps().getSamplingClock().getStartTime().getNanoseconds());
        assertEquals(
                samplePeriod,
                responseBucket.getDataTimestamps().getSamplingClock().getPeriodNanos());
        assertEquals(
                numSamples,
                responseBucket.getDataTimestamps().getSamplingClock().getCount());

        // this compares each DataValue including ValueStatus, confirmed in debugger
        assertEquals(
                requestColumn,
                responseBucket.getDataColumn());
    }

}
