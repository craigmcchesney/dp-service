package com.ospreydcs.dp.service.integration.v2api;

import com.ospreydcs.dp.grpc.v1.common.DataBucket;
import com.ospreydcs.dp.grpc.v1.common.DoubleColumn;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;
import com.ospreydcs.dp.grpc.v1.ingestion.SubscribeDataResponse;
import com.ospreydcs.dp.service.ingest.IngestionTestBase;
import com.ospreydcs.dp.service.ingest.utility.SubscribeDataUtility;
import com.ospreydcs.dp.service.integration.GrpcIntegrationTestBase;
import com.ospreydcs.dp.service.query.QueryTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DoubleColumnIT extends GrpcIntegrationTestBase {

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() {
        super.tearDown();
    }

    /**
     * Covers use in the APIs of the DoubleColumn protobuf message.  Registers a provider, which is required before
     * using the ingestion APIs.  Uses the data ingestion API to send an IngestDataRequest whose IngestionDataFrame
     * contains a DoubleColumn data structure.  Uses the time-series data query API to retrieve the bucket containing
     * the DataColumn sent in the ingestion request.  Confirms that the DoubleColumn retrieved via the query API matches
     * the column sent in the ingestion request, using DataColumn.equals() which compares column name and data values
     * in the two columns.
     */
    @Test
    public void doubleColumnTest() {

        String providerId;
        {
            // register ingestion provider
            final String providerName = String.valueOf(1);
            providerId = ingestionServiceWrapper.registerProvider(providerName, null);
        }

        List<String> columnNames;
        long firstSeconds;
        long firstNanos;
        IngestionTestBase.IngestionRequestParams ingestionRequestParams;
        DoubleColumn requestDoubleColumn;
        {
            // positive unary ingestion test for DoubleColumn
            // assemble IngestionRequest
            final String requestId = "request-8";
            final String pvName = "pv_08";
            columnNames = Arrays.asList(pvName);
            firstSeconds = Instant.now().getEpochSecond();
            firstNanos = 0L;
            final long sampleIntervalNanos = 1_000_000L;
            final int numSamples = 2;

            // specify explicit DoubleColumn data
            final List<DoubleColumn> doubleColumns = new ArrayList<>();
            DoubleColumn.Builder doubleColumnBuilder = DoubleColumn.newBuilder();
            doubleColumnBuilder.setName(pvName);
            doubleColumnBuilder.addValues(12.34);
            doubleColumnBuilder.addValues(34.56);
            requestDoubleColumn = doubleColumnBuilder.build();
            doubleColumns.add(requestDoubleColumn);

            // create request parameters
            ingestionRequestParams =
                    new IngestionTestBase.IngestionRequestParams(
                            providerId,
                            requestId,
                            null,
                            null,
                            firstSeconds,
                            firstNanos,
                            sampleIntervalNanos,
                            numSamples,
                            columnNames,
                            null,
                            null,
                            null,
                            false,
                            null
                    );
            ingestionRequestParams.setDoubleColumnList(doubleColumns); // add list of DoubleColumns to request parameters

            final IngestDataRequest request =
                    IngestionTestBase.buildIngestionRequest(ingestionRequestParams);

            ingestionServiceWrapper.sendAndVerifyIngestData(ingestionRequestParams, request, 0);
        }

        // positive queryData() test case
        {
            // select 5 seconds of data for each pv
            final long beginSeconds = firstSeconds;
            final long beginNanos = firstNanos;
            final long endSeconds = beginSeconds + 1L;
            final long endNanos = 0L;

            // 2 pvs, 5 seconds, 1 bucket per second per pv
            final int numBucketsExpected = 1;
            final boolean expectReject = false;
            final String expectedRejectMessage = "";

            final QueryTestBase.QueryDataRequestParams params =
                    new QueryTestBase.QueryDataRequestParams(
                            columnNames,
                            beginSeconds,
                            beginNanos,
                            endSeconds,
                            endNanos,
                            false
                    );

            final List<DataBucket> queryResultBuckets = queryServiceWrapper.queryData(
                    params,
                    expectReject,
                    expectedRejectMessage
            );

            assertEquals(numBucketsExpected, queryResultBuckets.size());
            for (DataBucket queryResultBucket : queryResultBuckets) {
                assertEquals(requestDoubleColumn, queryResultBucket.getDoubleColumn());
            }
        }

        // create a data subscription, verification succeeds because data have been ingested for the subscription PV
        SubscribeDataUtility.SubscribeDataCall subscribeDataCall;
        {
            final int expectedResponseCount = 1;
            final boolean expectReject = false;
            final String expectedRejectMessage = "";
            subscribeDataCall =
                    ingestionServiceWrapper.initiateSubscribeDataRequest(
                            columnNames, expectedResponseCount, expectReject, expectedRejectMessage);
        }

        // ingest data that will be published to subscription
        DoubleColumn subscriptionColumn;
        {
            // positive unary ingestion test for DoubleColumn
            // assemble IngestionRequest
            final String requestId = "request-9";
            final String pvName = "pv_08";
            columnNames = Arrays.asList(pvName);
            final long sampleIntervalNanos = 1_000_000L;
            final int numSamples = 2;

            // specify explicit DoubleColumn data
            final List<DoubleColumn> doubleColumns = new ArrayList<>();
            DoubleColumn.Builder doubleColumnBuilder = DoubleColumn.newBuilder();
            doubleColumnBuilder.setName(pvName);
            doubleColumnBuilder.addValues(98.76);
            doubleColumnBuilder.addValues(54.32);
            subscriptionColumn = doubleColumnBuilder.build();
            doubleColumns.add(subscriptionColumn);

            // create request parameters
            final IngestionTestBase.IngestionRequestParams subscriptionRequestParams =
                    new IngestionTestBase.IngestionRequestParams(
                            providerId,
                            requestId,
                            null,
                            null,
                            firstSeconds+1,
                            firstNanos,
                            sampleIntervalNanos,
                            numSamples,
                            columnNames,
                            null,
                            null,
                            null,
                            false,
                            null
                    );
            subscriptionRequestParams.setDoubleColumnList(doubleColumns); // add list of DoubleColumns to request parameters

            final IngestDataRequest request =
                    IngestionTestBase.buildIngestionRequest(subscriptionRequestParams);

            ingestionServiceWrapper.sendAndVerifyIngestData(subscriptionRequestParams, request, 0);
        }

        // check that expected subscription response is received
        {
            final IngestionTestBase.SubscribeDataResponseObserver responseObserver =
                    (IngestionTestBase.SubscribeDataResponseObserver) subscribeDataCall.responseObserver();
            
            // wait for completion of API method response stream and confirm not in error state
            responseObserver.awaitResponseLatch();
            assertFalse(responseObserver.isError());

            // get subscription responses for verification of expected contents
            final List<SubscribeDataResponse> responseList = responseObserver.getResponseList();
            assertEquals(1, responseList.size());
            final SubscribeDataResponse subscriptionResponse = responseList.get(0);
            assertTrue(subscriptionResponse.hasSubscribeDataResult());
            assertTrue(subscriptionResponse.getSubscribeDataResult().hasDataFrame());
            assertTrue(
                    subscriptionResponse.getSubscribeDataResult().getDataFrame().getDoubleColumnsList().contains(subscriptionColumn));
        }

    }

}
