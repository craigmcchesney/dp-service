package com.ospreydcs.dp.service.integration.ingest;

import com.ospreydcs.dp.grpc.v1.common.ExceptionalResult;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataResponse;
import com.ospreydcs.dp.service.ingest.IngestionTestBase;
import com.ospreydcs.dp.service.integration.GrpcIntegrationTestBase;
import org.junit.*;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IngestDataUnaryIT extends GrpcIntegrationTestBase {

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() {
        super.tearDown();
    }

    @Test
    public void unaryTest() {

        String providerId;
        {
            // register ingestion provider
            final String providerName = String.valueOf(1);
            providerId = ingestionServiceWrapper.registerProvider(providerName, null);
        }

        {
            // negative test case, rejection due to empty PV name

            // create request
            final String requestId = "request-1";
            final List<String> columnNames = Arrays.asList(""); // empty PV name should cause rejection
            final List<List<Object>> values = Arrays.asList(Arrays.asList(12.34));
            final Instant instantNow = Instant.now();
            final IngestionTestBase.IngestionRequestParams params =
                    new IngestionTestBase.IngestionRequestParams(
                            providerId,
                            requestId,
                            null,
                            null,
                            instantNow.getEpochSecond(),
                            0L,
                            1_000_000L,
                            1,
                            columnNames,
                            IngestionTestBase.IngestionDataType.DOUBLE,
                            values,
                            null, false, null);
            final IngestDataRequest request = IngestionTestBase.buildIngestionRequest(params);

            // send request and examine response
            final IngestDataResponse response = ingestionServiceWrapper.sendIngestData(request);
            assertTrue(response.getProviderId() == providerId);
            assertTrue(response.getClientRequestId().equals(requestId));
            assertTrue(response.hasExceptionalResult());
            assertEquals(
                    ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT,
                    response.getExceptionalResult().getExceptionalResultStatus());
            assertTrue(response.getResponseTime().getEpochSeconds() > 0);
            assertEquals(
                    "ingestionDataFrame.dataColumns[0].name must be specified",
                    response.getExceptionalResult().getMessage());
        }

        {
            // positive unary ingestion test for simple request

            // create request
            final String requestId = "request-2";
            final List<String> columnNames = Arrays.asList("PV_01");
            final List<List<Object>> values = Arrays.asList(Arrays.asList(12.34));
            final Instant instantNow = Instant.now();
            final IngestionTestBase.IngestionRequestParams params =
                    new IngestionTestBase.IngestionRequestParams(
                            providerId,
                            requestId,
                            null,
                            null,
                            instantNow.getEpochSecond(),
                            0L,
                            1_000_000L,
                            1,
                            columnNames,
                            IngestionTestBase.IngestionDataType.DOUBLE,
                            values,
                            null, false, null);
            final IngestDataRequest request = IngestionTestBase.buildIngestionRequest(params);

            // send request and examine response
            ingestionServiceWrapper.sendAndVerifyIngestData(params, request, 0);
        }
    }

}
