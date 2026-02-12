package com.ospreydcs.dp.service.integration.v2api;

import com.ospreydcs.dp.grpc.v1.common.DataBucket;
import com.ospreydcs.dp.grpc.v1.common.DoubleColumn;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;
import com.ospreydcs.dp.service.ingest.IngestionTestBase;
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
            String requestId = "request-8";
            String pvName = "pv_08";
            columnNames = Arrays.asList(pvName);
            firstSeconds = Instant.now().getEpochSecond();
            firstNanos = 0L;
            long sampleIntervalNanos = 1_000_000L;
            int numSamples = 2;

            // specify explicit DoubleColumn data
            List<DoubleColumn> doubleColumns = new ArrayList<>();
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
                            IngestionTestBase.IngestionDataType.ARRAY_DOUBLE,
                            null,
                            null,
                            false,
                            null
                    );
            ingestionRequestParams.setDoubleColumnList(doubleColumns); // add list of DoubleColumns to request parameters

            IngestDataRequest request =
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

            List<DataBucket> queryResultBuckets = queryServiceWrapper.queryData(
                    params,
                    expectReject,
                    expectedRejectMessage
            );

            assertEquals(numBucketsExpected, queryResultBuckets.size());
            for (DataBucket queryResultBucket : queryResultBuckets) {
                assertEquals(requestDoubleColumn, queryResultBucket.getDoubleColumn());
            }
        }

    }

}
