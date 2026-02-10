package com.ospreydcs.dp.service.integration.query;

import com.ospreydcs.dp.grpc.v1.common.DataBucket;
import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;
import com.ospreydcs.dp.service.ingest.IngestionTestBase;
import com.ospreydcs.dp.service.integration.GrpcIntegrationTestBase;
import com.ospreydcs.dp.service.query.QueryTestBase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/*
 * Provides coverage for ingestion and query of DataValues containing ValueStatus information.
 */
@RunWith(JUnit4.class)
public class QueryDataValueStatusIT extends GrpcIntegrationTestBase {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() {
        super.tearDown();
    }

    @Test
    public void valueStatusTest() {

        // register provider
        final String providerName = String.valueOf(1);
        final String providerId = ingestionServiceWrapper.registerProvider(providerName, null);

        // create containers
        final List<IngestionTestBase.IngestionRequestParams> paramsList = new ArrayList<>();
        final List<IngestDataRequest> requestList = new ArrayList<>();

        final long startSeconds = Instant.now().getEpochSecond();
        final long samplePeriod = 1_000_000L;
        final int sampleCount = 1;

        // create 1st request
        {
            final String requestId = "request-1";
            final List<String> columnNames = Arrays.asList("PV_01");
            final List<List<Object>> values = Arrays.asList(Arrays.asList(1.01));

            // manually create ValueStatus information
            DataValue.ValueStatus.StatusCode valueStatusCode = DataValue.ValueStatus.StatusCode.DEVICE_STATUS;
            DataValue.ValueStatus.Severity valueStatusSeverity = DataValue.ValueStatus.Severity.MINOR_ALARM;
            DataValue.ValueStatus valueStatus = DataValue.ValueStatus.newBuilder()
                    .setMessage("PV_01 status")
                    .setStatusCode(valueStatusCode)
                    .setSeverity(valueStatusSeverity)
                    .build();
            final List<List<DataValue.ValueStatus>> valuesStatus = Arrays.asList(Arrays.asList(valueStatus));

            final Instant instantNow = Instant.now();
            final IngestionTestBase.IngestionRequestParams params =
                    new IngestionTestBase.IngestionRequestParams(
                            providerId,
                            requestId,
                            null,
                            null,
                            startSeconds,
                            0L,
                            samplePeriod,
                            sampleCount,
                            columnNames,
                            IngestionTestBase.IngestionDataType.DOUBLE,
                            values,
                            valuesStatus, false, null, null);
            final IngestDataRequest request = IngestionTestBase.buildIngestionRequest(params);
            paramsList.add(params);
            requestList.add(request);
        }

        // create 2nd request
        {
            final String requestId = "request-2";
            final List<String> columnNames = Arrays.asList("PV_02");
            final List<List<Object>> values = Arrays.asList(Arrays.asList(2.02));

            // manually create ValueStatus information
            DataValue.ValueStatus.StatusCode valueStatusCode = DataValue.ValueStatus.StatusCode.DRIVER_STATUS;
            DataValue.ValueStatus.Severity valueStatusSeverity = DataValue.ValueStatus.Severity.MAJOR_ALARM;
            DataValue.ValueStatus valueStatus = DataValue.ValueStatus.newBuilder()
                    .setMessage("PV_02 status")
                    .setStatusCode(valueStatusCode)
                    .setSeverity(valueStatusSeverity)
                    .build();
            final List<List<DataValue.ValueStatus>> valuesStatus = Arrays.asList(Arrays.asList(valueStatus));

            final Instant instantNow = Instant.now();
            final IngestionTestBase.IngestionRequestParams params =
                    new IngestionTestBase.IngestionRequestParams(
                            providerId,
                            requestId,
                            null,
                            null,
                            startSeconds,
                            0L,
                            samplePeriod,
                            sampleCount,
                            columnNames,
                            IngestionTestBase.IngestionDataType.DOUBLE,
                            values,
                            valuesStatus, false, null, null);
            final IngestDataRequest request = IngestionTestBase.buildIngestionRequest(params);
            paramsList.add(params);
            requestList.add(request);
        }

        // send request and examine response
        // this compares request and bucket DataColumns including DataValues and ValueStatus
        ingestionServiceWrapper.sendAndVerifyIngestDataStream(
                paramsList, requestList, 0, false, "");

        // perform query for PV_01 and verify results
        {
            final List<String> queryPvNames = Arrays.asList("PV_01");
            final DataColumn requestColumn = requestList.get(0).getIngestionDataFrame().getDataColumns(0);
            final QueryTestBase.QueryDataRequestParams params =
                    new QueryTestBase.QueryDataRequestParams(
                            queryPvNames,
                            startSeconds,
                            0L,
                            startSeconds + 1,
                            0L,
                            false);
            final List<DataBucket> queryBuckets =
                    queryServiceWrapper.queryDataStream(params, false, "");
            assertEquals(queryPvNames.size(), queryBuckets.size());
            final DataBucket responseBucket = queryBuckets.get(0);

            // this compares the request and response DataColumns including each DataValue and ValueStatus
            QueryTestBase.verifyDataBucket(
                    responseBucket, requestColumn, startSeconds, 0, samplePeriod, sampleCount);
        }

        // perform query for PV_02 and verify results
        {
            final List<String> queryPvNames = Arrays.asList("PV_02");
            final DataColumn requestColumn = requestList.get(1).getIngestionDataFrame().getDataColumns(0);
            final QueryTestBase.QueryDataRequestParams params =
                    new QueryTestBase.QueryDataRequestParams(
                            queryPvNames,
                            startSeconds,
                            0L,
                            startSeconds + 1,
                            0L,
                            false);
            final List<DataBucket> queryBuckets =
                    queryServiceWrapper.queryDataStream(params, false, "");
            assertEquals(queryPvNames.size(), queryBuckets.size());
            final DataBucket responseBucket = queryBuckets.get(0);

            // this compares the request and response DataColumns including each DataValue and ValueStatus
            QueryTestBase.verifyDataBucket(
                    responseBucket, requestColumn, startSeconds, 0, samplePeriod, sampleCount);
        }
    }

}
