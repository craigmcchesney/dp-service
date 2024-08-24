package com.ospreydcs.dp.service.integration.query;

import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryDataResponse;
import com.ospreydcs.dp.service.ingest.IngestionTestBase;
import com.ospreydcs.dp.service.integration.GrpcIntegrationTestBase;
import com.ospreydcs.dp.service.query.QueryTestBase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
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
public class ValueStatusTest extends GrpcIntegrationTestBase {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    @BeforeClass
    public static void setUp() throws Exception {
        GrpcIntegrationTestBase.setUp();
    }

    @AfterClass
    public static void tearDown() {
        GrpcIntegrationTestBase.tearDown();
    }

    @Test
    public void valueStatusTest() {

        // register provider
        final String providerName = String.valueOf(1);
        final String providerId = registerProvider(providerName, null);

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
                            null,
                            null,
                            startSeconds,
                            0L,
                            samplePeriod,
                            sampleCount,
                            columnNames,
                            IngestionTestBase.IngestionDataType.DOUBLE,
                            values,
                            valuesStatus);
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
                            null,
                            null,
                            startSeconds,
                            0L,
                            samplePeriod,
                            sampleCount,
                            columnNames,
                            IngestionTestBase.IngestionDataType.DOUBLE,
                            values,
                            valuesStatus);
            final IngestDataRequest request = IngestionTestBase.buildIngestionRequest(params);
            paramsList.add(params);
            requestList.add(request);
        }

        // send request and examine response
        // this compares request and bucket DataColumns including DataValues and ValueStatus
        sendAndVerifyIngestDataStream(paramsList, requestList, false, "");

        // perform query for PV_01 and verify results
        {
            final List<String> queryPvNames = Arrays.asList("PV_01");
            final DataColumn requestColumn = requestList.get(0).getIngestionDataFrame().getDataColumns(0);
            final List<QueryDataResponse.QueryData.DataBucket> queryBuckets = queryDataStream(
                    queryPvNames, startSeconds, 0, startSeconds + 1, 0);
            assertEquals(queryPvNames.size(), queryBuckets.size());
            final QueryDataResponse.QueryData.DataBucket responseBucket = queryBuckets.get(0);

            // this compares the request and response DataColumns including each DataValue and ValueStatus
            QueryTestBase.verifyDataBucket(
                    responseBucket, requestColumn, startSeconds, 0, samplePeriod, sampleCount);
        }

        // perform query for PV_02 and verify results
        {
            final List<String> queryPvNames = Arrays.asList("PV_02");
            final DataColumn requestColumn = requestList.get(1).getIngestionDataFrame().getDataColumns(0);
            final List<QueryDataResponse.QueryData.DataBucket> queryBuckets = queryDataStream(
                    queryPvNames, startSeconds, 0, startSeconds + 1, 0);
            assertEquals(queryPvNames.size(), queryBuckets.size());
            final QueryDataResponse.QueryData.DataBucket responseBucket = queryBuckets.get(0);

            // this compares the request and response DataColumns including each DataValue and ValueStatus
            QueryTestBase.verifyDataBucket(
                    responseBucket, requestColumn, startSeconds, 0, samplePeriod, sampleCount);
        }
    }

}
