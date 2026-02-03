package com.ospreydcs.dp.service.ingest.handler;

import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import com.ospreydcs.dp.service.ingest.IngestionTestBase;
import org.junit.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IngestionValidationUtilityTest extends IngestionTestBase {

    @Test
    public void testValidateRequestUnspecifiedProvider() {
        String requestId = "request-1";
        List<String> columnNames = Arrays.asList("pv_01");
        List<List<Object>> values = Arrays.asList(Arrays.asList(12.34));
        Instant instantNow = Instant.now();
        IngestionTestBase.IngestionRequestParams params =
                new IngestionTestBase.IngestionRequestParams(
                        null,
                        requestId,
                        null,
                        null,
                        null,
                        null,
                        instantNow.getEpochSecond(),
                        0L,
                        1_000_000L,
                        1,
                        columnNames,
                        IngestionDataType.DOUBLE,
                        values, null, false);
        IngestDataRequest request = buildIngestionRequest(params);
        ResultStatus result = IngestionValidationUtility.validateIngestionRequest(request);
        assertTrue(result.isError);
        assertTrue(result.msg.equals("providerId must be specified"));
    }

    @Test
    public void testValidateRequestUnspecifiedRequestId() {
        String providerId = String.valueOf(1);
        List<String> columnNames = Arrays.asList("pv_01");
        List<List<Object>> values = Arrays.asList(Arrays.asList(12.34));
        Instant instantNow = Instant.now();
        IngestionTestBase.IngestionRequestParams params =
                new IngestionTestBase.IngestionRequestParams(
                        providerId,
                        null,
                        null,
                        null,
                        null,
                        null,
                        instantNow.getEpochSecond(),
                        0L,
                        1_000_000L,
                        1,
                        columnNames,
                        IngestionDataType.DOUBLE,
                        values, null, false);
        IngestDataRequest request = buildIngestionRequest(params);
        ResultStatus result = IngestionValidationUtility.validateIngestionRequest(request);
        assertTrue(result.isError);
        assertTrue(result.msg.equals("clientRequestId must be specified"));
    }

    @Test
    public void testValidateRequestInvalidTimeIterator() {
        String providerId = String.valueOf(1);
        String requestId = "request-1";
        List<String> columnNames = Arrays.asList("pv_01");
        List<List<Object>> values = Arrays.asList(Arrays.asList(12.34));
        Instant instantNow = Instant.now();
        IngestionTestBase.IngestionRequestParams params =
                new IngestionTestBase.IngestionRequestParams(
                        providerId,
                        requestId,
                        null,
                        null,
                        null,
                        null,
                        instantNow.getEpochSecond(),
                        0L,
                        1_000_000L,
                        0,
                        columnNames,
                        IngestionDataType.DOUBLE,
                        values, null, false);
        IngestDataRequest request = buildIngestionRequest(params);
        ResultStatus result = IngestionValidationUtility.validateIngestionRequest(request);
        assertTrue(result.isError);
        assertEquals(
                result.msg,
                "ingestionDataFrame.dataTimestamps.samplingClock.count must be > 0, got: 0");
    }

    /**
     * Provides test coverage of validation check for empty columns list.
     */
    @Test
    public void testValidateRequestEmptyColumnsList() {
        String providerId = String.valueOf(1);
        String requestId = "request-1";
        Instant instantNow = Instant.now();
        IngestionTestBase.IngestionRequestParams params =
                new IngestionTestBase.IngestionRequestParams(
                        providerId,
                        requestId,
                        null,
                        null,
                        null,
                        null,
                        instantNow.getEpochSecond(),
                        0L,
                        1_000_000L,
                        2,
                        null,
                        IngestionDataType.DOUBLE,
                        null, null, false);
        IngestDataRequest request = buildIngestionRequest(params);
        ResultStatus result = IngestionValidationUtility.validateIngestionRequest(request);
        assertTrue(result.isError);
        assertTrue(result.msg.equals("ingestionDataFrame must contain at least one column"));
    }

    /**
     * Provides test coverage of validation check that each column contains the same number of values as
     * the timestamps list.
     */
    @Test
    public void testValidateRequestColumnSizeMismatch() {
        String providerId = String.valueOf(1);
        String requestId = "request-1";
        List<String> columnNames = Arrays.asList("pv_01");
        List<List<Object>> values = Arrays.asList(Arrays.asList(12.34));
        Instant instantNow = Instant.now();
        IngestionTestBase.IngestionRequestParams params =
                new IngestionTestBase.IngestionRequestParams(
                        providerId,
                        requestId,
                        null,
                        null,
                        null,
                        null,
                        instantNow.getEpochSecond(),
                        0L,
                        1_000_000L,
                        2,
                        columnNames,
                        IngestionDataType.DOUBLE,
                        values, null, false);
        IngestDataRequest request = buildIngestionRequest(params);
        ResultStatus result = IngestionValidationUtility.validateIngestionRequest(request);
        assertTrue(result.isError);
        assertTrue(result.msg.contains("dataValues.length mismatch: expected 2, got: 1"));
    }

    /**
     * Provides test coverage of validation check that a name is provided for each column.
     */
    @Test
    public void testValidateRequestColumnNameMissing() {
        String providerId = String.valueOf(1);
        String requestId = "request-1";
        List<String> columnNames = Arrays.asList("");
        List<List<Object>> values = Arrays.asList(Arrays.asList(12.34, 42.00));
        Instant instantNow = Instant.now();
        IngestionTestBase.IngestionRequestParams params =
                new IngestionTestBase.IngestionRequestParams(
                        providerId,
                        requestId,
                        null,
                        null,
                        null,
                        null,
                        instantNow.getEpochSecond(),
                        0L,
                        1_000_000L,
                        2,
                        columnNames,
                        IngestionDataType.DOUBLE,
                        values, null, false);
        IngestDataRequest request = buildIngestionRequest(params);
        ResultStatus result = IngestionValidationUtility.validateIngestionRequest(request);
        assertTrue(result.isError);
        assertTrue(result.msg.equals("ingestionDataFrame.dataColumns[0].name must be specified"));
    }

}
