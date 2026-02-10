package com.ospreydcs.dp.service.ingest.service;

import com.ospreydcs.dp.grpc.v1.common.ExceptionalResult;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataResponse;
import com.ospreydcs.dp.service.common.protobuf.TimestampUtility;
import com.ospreydcs.dp.service.ingest.IngestionTestBase;
import org.junit.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IngestionServiceImplTest extends IngestionTestBase {

    private static final IngestionServiceImpl serviceImpl = new IngestionServiceImpl();

    /**
     * Provides test coverage for MongoDbserviceImpl.dateFromTimestamp().
     */
    @Test
    public void testDateFromTimestamp() {

        long epochSeconds = 1691438936L;
        long nanos = 999000000L;

        // create a grpc timestamp, and the convert to java date
        final Timestamp.Builder timestampBuilder = Timestamp.newBuilder();
        timestampBuilder.setEpochSeconds(epochSeconds);
        timestampBuilder.setNanoseconds(nanos);
        Timestamp timestamp = timestampBuilder.build();
        Date dateFromTimestamp = TimestampUtility.dateFromTimestamp(timestamp);

        // create a java instant, and use to create java date
        Instant instant = Instant.ofEpochSecond(epochSeconds, nanos);
        Date dateFromInstant = Date.from(instant);

        // check that the two dates are equal
        assertTrue("dateFromTimestamp date mismatch with date from instant", dateFromTimestamp.equals(dateFromInstant));
    }

    /**
     * Provides test coverage for MongoDbserviceImpl.ingestionResponseSuccess().
     */
    @Test
    public void testIngestionResponseAck() {

        // create IngestionRequest
        String providerId = String.valueOf(1);
        String requestId = "request-1";
        List<String> columnNames = Arrays.asList("pv_01");
        List<List<Object>> values = Arrays.asList(Arrays.asList(12.34, 42.00));
        Instant instantNow = Instant.now();
        int numSamples = 2;
        IngestionRequestParams params =
                new IngestionRequestParams(
                        providerId,
                        requestId,
                        null,
                        null,
                        instantNow.getEpochSecond(),
                        0L,
                        1_000_000L,
                        numSamples,
                        columnNames,
                        IngestionDataType.DOUBLE,
                        values, null, false, null, null);
        IngestDataRequest request = buildIngestionRequest(params);

        // test ingestionResponseAck
        IngestDataResponse response = serviceImpl.ingestionResponseAck(request);
        assertTrue(response.getProviderId() == providerId);
        assertTrue(response.getClientRequestId().equals(requestId));
        assertTrue(response.hasAckResult());
        assertTrue(response.getResponseTime().getEpochSeconds() > 0);
        assertTrue(response.getAckResult().getNumRows() == numSamples);
        assertTrue(response.getAckResult().getNumColumns() == columnNames.size());
    }

    @Test
    public void testIngestionResponseReject() {

        // create IngestionRequest
        String providerId = String.valueOf(1);
        String requestId = "request-1";
        List<String> columnNames = Arrays.asList("pv_01");
        List<List<Object>> values = Arrays.asList(Arrays.asList(12.34));
        Instant instantNow = Instant.now();
        int numSamples = 2;
        IngestionRequestParams params =
                new IngestionRequestParams(
                        providerId,
                        requestId,
                        null,
                        null,
                        instantNow.getEpochSecond(),
                        0L,
                        1_000_000L,
                        numSamples,
                        columnNames,
                        IngestionDataType.DOUBLE,
                        values, null, false, null, null);
        IngestDataRequest request = buildIngestionRequest(params);

        // test ingestionResponseRejectInvalid
        String msg = "test";
        IngestDataResponse response = serviceImpl.ingestionResponseReject(request, msg);
        assertTrue(response.getProviderId() == providerId);
        assertTrue(response.getClientRequestId().equals(requestId));
        assertTrue(response.hasExceptionalResult());
        assertEquals(
                ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT,
                response.getExceptionalResult().getExceptionalResultStatus());
        assertTrue(response.getResponseTime().getEpochSeconds() > 0);
        assertTrue(response.hasExceptionalResult());
        assertEquals(
                ExceptionalResult.ExceptionalResultStatus.RESULT_STATUS_REJECT,
                response.getExceptionalResult().getExceptionalResultStatus());
        assertTrue(response.getExceptionalResult().getMessage().equals(msg));
    }

}
