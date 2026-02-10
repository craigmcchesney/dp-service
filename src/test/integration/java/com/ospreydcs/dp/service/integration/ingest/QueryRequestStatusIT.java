package com.ospreydcs.dp.service.integration.ingest;

import com.ospreydcs.dp.grpc.v1.common.ExceptionalResult;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataResponse;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestionRequestStatus;
import com.ospreydcs.dp.service.ingest.IngestionTestBase;
import com.ospreydcs.dp.service.integration.GrpcIntegrationTestBase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.*;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.Thread.sleep;
import static org.junit.Assert.*;

/*
 * Provides coverage for the request status query API.
 */
public class QueryRequestStatusIT extends GrpcIntegrationTestBase {

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
    public void requestStatusTest() {

        final long startSeconds = Instant.now().getEpochSecond();

        final Map<String, String> providerIdMap = new HashMap<>();

        {
            // ingest data for five different providers, scenario with some successful requests, some with rejects, and
            // some with errors due to duplicate database ids

            for (int providerIndex=1 ; providerIndex <= 5 ; ++providerIndex) {

                // register ingestion provider
                final String providerName = String.valueOf(providerIndex);
                final String providerId = ingestionServiceWrapper.registerProvider(providerName, null);
                providerIdMap.put(providerName, providerId);

                // use same seconds value for both requests to get duplicate id

                // send simple successful ingestion requests
                int requestIndex = 0;
                for (requestIndex = 1 ; requestIndex <= 5 ; ++requestIndex) {
                    final String requestId = "request-" + providerName + "-" + requestIndex;
                    final String pvName = "pv-" + providerName + "-" + requestIndex;
                    final List<String> columnNames = Arrays.asList(pvName);
                    final List<List<Object>> values = Arrays.asList(Arrays.asList(12.34));
                    final IngestionTestBase.IngestionRequestParams params =
                            new IngestionTestBase.IngestionRequestParams(
                                    providerId,
                                    requestId,
                                    null,
                                    null,
                                    startSeconds,
                                    0L,
                                    1_000_000L,
                                    1,
                                    columnNames,
                                    IngestionTestBase.IngestionDataType.DOUBLE,
                                    values,
                                    null, false, null, null);
                    final IngestDataRequest request = IngestionTestBase.buildIngestionRequest(params);
                    ingestionServiceWrapper.sendAndVerifyIngestData(params, request, 0);
                }

                // send request that will be rejected because of empty string in columnNames list
                {
                    final String requestId = "request-" + providerName + "-" + requestIndex;
                    // final String pvName = "pv-" + providerId + "-" + requestIndex;
                    final List<String> columnNames = Arrays.asList("");  // add emtpy column name string
                    final List<List<Object>> values = Arrays.asList(Arrays.asList(12.34));
                    final IngestionTestBase.IngestionRequestParams params =
                            new IngestionTestBase.IngestionRequestParams(
                                    providerId,
                                    requestId,
                                    null,
                                    null,
                                    startSeconds,
                                    0L,
                                    1_000_000L,
                                    1,
                                    columnNames,
                                    IngestionTestBase.IngestionDataType.DOUBLE,
                                    values,
                                    null, false, null, null);
                    final IngestDataRequest request = IngestionTestBase.buildIngestionRequest(params);

                    // send but don't verify ingestion, manually inspect reject response
                    final IngestDataResponse response = ingestionServiceWrapper.sendIngestData(request); // don't verify ingestion since it will be rejected
                    assertTrue(response.getProviderId() == providerId);
                    assertTrue(response.getClientRequestId().equals(requestId));
                    assertTrue(response.hasExceptionalResult());
                    final ExceptionalResult exceptionalResult = response.getExceptionalResult();
                    assertEquals(
                            ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT,
                            exceptionalResult.getExceptionalResultStatus());
                    assertEquals(
                            "ingestionDataFrame.dataColumns[0].name must be specified",
                            exceptionalResult.getMessage());
                }

                // send request that will cause error due to duplicate database id
                requestIndex = requestIndex + 1;
                {
                    final String requestId = "request-" + providerName + "-" + requestIndex;
                    final String pvName = "pv-" + providerName + "-1"; // send data for pv that was already sent
                    final List<String> columnNames = Arrays.asList(pvName);
                    final List<List<Object>> values = Arrays.asList(Arrays.asList(12.34));
                    final IngestionTestBase.IngestionRequestParams params =
                            new IngestionTestBase.IngestionRequestParams(
                                    providerId,
                                    requestId,
                                    null,
                                    null,
                                    startSeconds,
                                    0L,
                                    1_000_000L,
                                    1,
                                    columnNames,
                                    IngestionTestBase.IngestionDataType.DOUBLE,
                                    values,
                                    null, false, null, null);
                    final IngestDataRequest request = IngestionTestBase.buildIngestionRequest(params);

                    // send but don't verify ingestion since it will fail, manually inspect ack response
                    final IngestDataResponse response = ingestionServiceWrapper.sendIngestData(request); // don't verify ingestion since it will fail
                    assertTrue(response.getProviderId() == providerId);
                    assertTrue(response.getClientRequestId().equals(requestId));
                    assertTrue(response.hasAckResult()); // request should be acked but error during processing
                    final IngestDataResponse.AckResult ackResult = response.getAckResult();
                    assertEquals(1, ackResult.getNumColumns());
                    assertEquals(1, ackResult.getNumRows());

//                    // wait until the request status document is found, otherwise we'll have a race condition on the query
//                    final RequestStatusDocument statusDocument =
//                            mongoClient.findRequestStatus(params.providerId, params.requestId);
//                    assertNotNull(statusDocument);
                }

            }
        }

        // send request status query with empty query result
        {
            final String providerId = "uknown providerId";
            final String providerName = null;
            final String requestId = "unknown requestId";
            final List<IngestionRequestStatus> status = null;
            final Long beginSeconds = null;
            final Long beginNanos = null;
            final Long endSeconds = null;
            final Long endNanos = null;

            final IngestionTestBase.QueryRequestStatusParams params = new IngestionTestBase.QueryRequestStatusParams(
                    providerId,
                    providerName,
                    requestId,
                    status,
                    beginSeconds,
                    beginNanos,
                    endSeconds,
                    endNanos
            );

            final IngestionTestBase.QueryRequestStatusExpectedResponseMap expectedResponseMap =
                    new IngestionTestBase.QueryRequestStatusExpectedResponseMap();

            final boolean expectReject = false;
            final String expectedRejectMessage = "";
            ingestionServiceWrapper.sendAndVerifyQueryRequestStatus(params, expectedResponseMap, expectReject, expectedRejectMessage);
        }

        {
            // send request status query by providerId, requestId, matches a single status document

            final String providerId = providerIdMap.get(String.valueOf(3));
            final String providerName = String.valueOf(3);
            final String requestId = "request-3-3";
            final List<IngestionRequestStatus> status = null;
            final Long beginSeconds = null;
            final Long beginNanos = null;
            final Long endSeconds = null;
            final Long endNanos = null;

            final IngestionTestBase.QueryRequestStatusParams params = new IngestionTestBase.QueryRequestStatusParams(
                    providerId,
                    null,
                    requestId,
                    status,
                    beginSeconds,
                    beginNanos,
                    endSeconds,
                    endNanos
            );

            // Create map of expected responses for use in verification
            final IngestionTestBase.QueryRequestStatusExpectedResponseMap expectedResponseMap =
                    new IngestionTestBase.QueryRequestStatusExpectedResponseMap();
            final IngestionRequestStatus expectedStatus = IngestionRequestStatus.INGESTION_REQUEST_STATUS_SUCCESS;
            final String expectedMessage = "";
            final String expectedBucketId = "pv-3-3-" + startSeconds + "-0";
            final List<String> expectedIdsCreated = Arrays.asList(expectedBucketId);
            final IngestionTestBase.QueryRequestStatusExpectedResponse expectedResponse =
                    new IngestionTestBase.QueryRequestStatusExpectedResponse(
                            providerId,
                            providerName,
                            requestId,
                            expectedStatus,
                            expectedMessage,
                            expectedIdsCreated);
            expectedResponseMap.addExpectedResponse(expectedResponse);

            ingestionServiceWrapper.sendAndVerifyQueryRequestStatus(params, expectedResponseMap, false, "");
        }

        {
            // send request status query by provider name, status and time range, for rejected and error status

            final String providerId = providerIdMap.get(String.valueOf(2));
            final String providerName = String.valueOf(2);
            final String requestId = null;
            final List<IngestionRequestStatus> status =
                    Arrays.asList(
                            IngestionRequestStatus.INGESTION_REQUEST_STATUS_REJECTED,
                            IngestionRequestStatus.INGESTION_REQUEST_STATUS_ERROR);
            final Long beginSeconds = startSeconds-60; // start before the time we sent ingestion requests
            final Long beginNanos = 0L;
            final Long endSeconds = null;
            final Long endNanos = null;

            final IngestionTestBase.QueryRequestStatusParams params = new IngestionTestBase.QueryRequestStatusParams(
                    null, // send query with ProviderNameCriterion instead of ProviderIdCriterion
                    providerName,
                    requestId,
                    status,
                    beginSeconds,
                    beginNanos,
                    endSeconds,
                    endNanos
            );

            // Create map of expected request statuses in response for use in verification
            final IngestionTestBase.QueryRequestStatusExpectedResponseMap expectedResponseMap =
                    new IngestionTestBase.QueryRequestStatusExpectedResponseMap();

            // add entry for expected rejected status
            {
                final String expectedRequestId = "request-2-6";
                final IngestionRequestStatus expectedStatus = IngestionRequestStatus.INGESTION_REQUEST_STATUS_REJECTED;
                final String expectedMessage = "name must be specified for all data columns";
                final List<String> expectedIdsCreated = Arrays.asList();
                final IngestionTestBase.QueryRequestStatusExpectedResponse expectedResponse =
                        new IngestionTestBase.QueryRequestStatusExpectedResponse(
                                providerId,
                                providerName,
                                expectedRequestId,
                                expectedStatus,
                                expectedMessage,
                                expectedIdsCreated);
                expectedResponseMap.addExpectedResponse(expectedResponse);
            }

            // add entry for expected error status
            {
                final String expectedRequestId = "request-2-7";
                final IngestionRequestStatus expectedStatus = IngestionRequestStatus.INGESTION_REQUEST_STATUS_ERROR;
                final String expectedMessage = "MongoException in insertMany: Bulk write operation error";
                final List<String> expectedIdsCreated = Arrays.asList();
                final IngestionTestBase.QueryRequestStatusExpectedResponse expectedResponse =
                        new IngestionTestBase.QueryRequestStatusExpectedResponse(
                                providerId,
                                providerName,
                                expectedRequestId,
                                expectedStatus,
                                expectedMessage,
                                expectedIdsCreated);
                expectedResponseMap.addExpectedResponse(expectedResponse);
            }

            ingestionServiceWrapper.sendAndVerifyQueryRequestStatus(params, expectedResponseMap, false, "");
        }
    }

}
