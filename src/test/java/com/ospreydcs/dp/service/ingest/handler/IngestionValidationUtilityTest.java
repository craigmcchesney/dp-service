package com.ospreydcs.dp.service.ingest.handler;

import com.ospreydcs.dp.grpc.v1.common.*;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest.IngestionDataFrame;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import com.ospreydcs.dp.service.ingest.IngestionTestBase;
import com.ospreydcs.dp.service.common.protobuf.TimestampUtility;
import com.google.protobuf.ByteString;
import org.junit.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

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

    // ===== New Column Type Validation Tests =====

    /**
     * Test DoubleColumn validation - missing name
     */
    @Test
    public void testValidateDoubleColumnMissingName() {
        Timestamp startTime = TimestampUtility.getTimestampNow();
        SamplingClock clock = SamplingClock.newBuilder()
                .setStartTime(startTime)
                .setPeriodNanos(1000000L)
                .setCount(2)
                .build();
        DataTimestamps timestamps = DataTimestamps.newBuilder()
                .setSamplingClock(clock)
                .build();
        
        DoubleColumn column = DoubleColumn.newBuilder()
                .setName("") // Empty name
                .addAllValues(Arrays.asList(1.23, 4.56))
                .build();
        
        IngestionDataFrame frame = IngestionDataFrame.newBuilder()
                .setDataTimestamps(timestamps)
                .addDoubleColumns(column)
                .build();
        
        IngestDataRequest request = IngestDataRequest.newBuilder()
                .setProviderId("provider-1")
                .setClientRequestId("request-1")
                .setIngestionDataFrame(frame)
                .build();
        
        ResultStatus result = IngestionValidationUtility.validateIngestionRequest(request);
        assertTrue(result.isError);
        assertEquals("ingestionDataFrame.doubleColumns[0].name must be specified", result.msg);
    }

    /**
     * Test DoubleColumn validation - size mismatch
     */
    @Test
    public void testValidateDoubleColumnSizeMismatch() {
        Timestamp startTime = TimestampUtility.getTimestampNow();
        SamplingClock clock = SamplingClock.newBuilder()
                .setStartTime(startTime)
                .setPeriodNanos(1000000L)
                .setCount(3) // Expect 3 values
                .build();
        DataTimestamps timestamps = DataTimestamps.newBuilder()
                .setSamplingClock(clock)
                .build();
        
        DoubleColumn column = DoubleColumn.newBuilder()
                .setName("sensor_01")
                .addAllValues(Arrays.asList(1.23, 4.56)) // Only 2 values
                .build();
        
        IngestionDataFrame frame = IngestionDataFrame.newBuilder()
                .setDataTimestamps(timestamps)
                .addDoubleColumns(column)
                .build();
        
        IngestDataRequest request = IngestDataRequest.newBuilder()
                .setProviderId("provider-1")
                .setClientRequestId("request-1")
                .setIngestionDataFrame(frame)
                .build();
        
        ResultStatus result = IngestionValidationUtility.validateIngestionRequest(request);
        assertTrue(result.isError);
        assertTrue(result.msg.contains("values.length mismatch: expected 3, got: 2 for PV: sensor_01"));
    }

    /**
     * Test StringColumn validation - string length exceeds maximum
     */
    @Test
    public void testValidateStringColumnTooLong() {
        Timestamp startTime = TimestampUtility.getTimestampNow();
        SamplingClock clock = SamplingClock.newBuilder()
                .setStartTime(startTime)
                .setPeriodNanos(1000000L)
                .setCount(1)
                .build();
        DataTimestamps timestamps = DataTimestamps.newBuilder()
                .setSamplingClock(clock)
                .build();
        
        // Create string longer than 256 characters
        StringBuilder longString = new StringBuilder();
        for (int i = 0; i < 260; i++) {
            longString.append("X");
        }
        
        StringColumn column = StringColumn.newBuilder()
                .setName("text_sensor")
                .addValues(longString.toString())
                .build();
        
        IngestionDataFrame frame = IngestionDataFrame.newBuilder()
                .setDataTimestamps(timestamps)
                .addStringColumns(column)
                .build();
        
        IngestDataRequest request = IngestDataRequest.newBuilder()
                .setProviderId("provider-1")
                .setClientRequestId("request-1")
                .setIngestionDataFrame(frame)
                .build();
        
        ResultStatus result = IngestionValidationUtility.validateIngestionRequest(request);
        assertTrue(result.isError);
        assertTrue(result.msg.contains("string length exceeds maximum"));
        assertTrue(result.msg.contains("length=260, max=256"));
    }

    /**
     * Test EnumColumn validation - missing enumId
     */
    @Test
    public void testValidateEnumColumnMissingEnumId() {
        Timestamp startTime = TimestampUtility.getTimestampNow();
        SamplingClock clock = SamplingClock.newBuilder()
                .setStartTime(startTime)
                .setPeriodNanos(1000000L)
                .setCount(1)
                .build();
        DataTimestamps timestamps = DataTimestamps.newBuilder()
                .setSamplingClock(clock)
                .build();
        
        EnumColumn column = EnumColumn.newBuilder()
                .setName("status_sensor")
                .setEnumId("") // Empty enumId
                .addValues(0)
                .build();
        
        IngestionDataFrame frame = IngestionDataFrame.newBuilder()
                .setDataTimestamps(timestamps)
                .addEnumColumns(column)
                .build();
        
        IngestDataRequest request = IngestDataRequest.newBuilder()
                .setProviderId("provider-1")
                .setClientRequestId("request-1")
                .setIngestionDataFrame(frame)
                .build();
        
        ResultStatus result = IngestionValidationUtility.validateIngestionRequest(request);
        assertTrue(result.isError);
        assertEquals("ingestionDataFrame.enumColumns[0].enumId must be specified for PV: status_sensor", result.msg);
    }

    /**
     * Test DoubleArrayColumn validation - invalid dimensions
     */
    @Test
    public void testValidateDoubleArrayColumnInvalidDimensions() {
        Timestamp startTime = TimestampUtility.getTimestampNow();
        SamplingClock clock = SamplingClock.newBuilder()
                .setStartTime(startTime)
                .setPeriodNanos(1000000L)
                .setCount(1)
                .build();
        DataTimestamps timestamps = DataTimestamps.newBuilder()
                .setSamplingClock(clock)
                .build();
        
        // Create dimensions with 4 dimensions (invalid - max is 3)
        ArrayDimensions dimensions = ArrayDimensions.newBuilder()
                .addAllDims(Arrays.asList(2, 3, 4, 5))
                .build();
        
        DoubleArrayColumn column = DoubleArrayColumn.newBuilder()
                .setName("array_sensor")
                .setDimensions(dimensions)
                .addAllValues(Arrays.asList(1.0, 2.0)) // Wrong number of values
                .build();
        
        IngestionDataFrame frame = IngestionDataFrame.newBuilder()
                .setDataTimestamps(timestamps)
                .addDoubleArrayColumns(column)
                .build();
        
        IngestDataRequest request = IngestDataRequest.newBuilder()
                .setProviderId("provider-1")
                .setClientRequestId("request-1")
                .setIngestionDataFrame(frame)
                .build();
        
        ResultStatus result = IngestionValidationUtility.validateIngestionRequest(request);
        assertTrue(result.isError);
        assertTrue(result.msg.contains("dimensions.dims.size must be in {1, 2, 3}, got: 4"));
    }

    /**
     * Test DoubleArrayColumn validation - zero dimension value
     */
    @Test
    public void testValidateDoubleArrayColumnZeroDimension() {
        Timestamp startTime = TimestampUtility.getTimestampNow();
        SamplingClock clock = SamplingClock.newBuilder()
                .setStartTime(startTime)
                .setPeriodNanos(1000000L)
                .setCount(1)
                .build();
        DataTimestamps timestamps = DataTimestamps.newBuilder()
                .setSamplingClock(clock)
                .build();
        
        ArrayDimensions dimensions = ArrayDimensions.newBuilder()
                .addAllDims(Arrays.asList(2, 0)) // Invalid dimension = 0
                .build();
        
        DoubleArrayColumn column = DoubleArrayColumn.newBuilder()
                .setName("array_sensor")
                .setDimensions(dimensions)
                .addAllValues(Arrays.asList(1.0, 2.0))
                .build();
        
        IngestionDataFrame frame = IngestionDataFrame.newBuilder()
                .setDataTimestamps(timestamps)
                .addDoubleArrayColumns(column)
                .build();
        
        IngestDataRequest request = IngestDataRequest.newBuilder()
                .setProviderId("provider-1")
                .setClientRequestId("request-1")
                .setIngestionDataFrame(frame)
                .build();
        
        ResultStatus result = IngestionValidationUtility.validateIngestionRequest(request);
        assertTrue(result.isError);
        assertTrue(result.msg.contains("dimensions.dims[1] must be > 0, got: 0"));
    }

    /**
     * Test DoubleArrayColumn validation - element count size mismatch
     */
    @Test
    public void testValidateDoubleArrayColumnSizeMismatch() {
        Timestamp startTime = TimestampUtility.getTimestampNow();
        SamplingClock clock = SamplingClock.newBuilder()
                .setStartTime(startTime)
                .setPeriodNanos(1000000L)
                .setCount(2) // 2 samples
                .build();
        DataTimestamps timestamps = DataTimestamps.newBuilder()
                .setSamplingClock(clock)
                .build();
        
        ArrayDimensions dimensions = ArrayDimensions.newBuilder()
                .addAllDims(Arrays.asList(3, 2)) // 6 elements per sample
                .build();
        
        DoubleArrayColumn column = DoubleArrayColumn.newBuilder()
                .setName("matrix_sensor")
                .setDimensions(dimensions)
                // Need 2 samples * 6 elements = 12 values, but providing only 10
                .addAllValues(Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0))
                .build();
        
        IngestionDataFrame frame = IngestionDataFrame.newBuilder()
                .setDataTimestamps(timestamps)
                .addDoubleArrayColumns(column)
                .build();
        
        IngestDataRequest request = IngestDataRequest.newBuilder()
                .setProviderId("provider-1")
                .setClientRequestId("request-1")
                .setIngestionDataFrame(frame)
                .build();
        
        ResultStatus result = IngestionValidationUtility.validateIngestionRequest(request);
        assertTrue(result.isError);
        assertTrue(result.msg.contains("values.length mismatch: expected 12"));
        assertTrue(result.msg.contains("got: 10"));
        assertTrue(result.msg.contains("sampleCount=2 * element_count=6"));
    }

    /**
     * Test ImageColumn validation - missing descriptor
     */
    @Test
    public void testValidateImageColumnMissingDescriptor() {
        Timestamp startTime = TimestampUtility.getTimestampNow();
        SamplingClock clock = SamplingClock.newBuilder()
                .setStartTime(startTime)
                .setPeriodNanos(1000000L)
                .setCount(1)
                .build();
        DataTimestamps timestamps = DataTimestamps.newBuilder()
                .setSamplingClock(clock)
                .build();
        
        ImageColumn column = ImageColumn.newBuilder()
                .setName("camera_01")
                // Missing imageDescriptor
                .addImages(ByteString.copyFromUtf8("fake-image-data"))
                .build();
        
        IngestionDataFrame frame = IngestionDataFrame.newBuilder()
                .setDataTimestamps(timestamps)
                .addImageColumns(column)
                .build();
        
        IngestDataRequest request = IngestDataRequest.newBuilder()
                .setProviderId("provider-1")
                .setClientRequestId("request-1")
                .setIngestionDataFrame(frame)
                .build();
        
        ResultStatus result = IngestionValidationUtility.validateIngestionRequest(request);
        assertTrue(result.isError);
        assertEquals("ingestionDataFrame.imageColumns[0].imageDescriptor must be specified for PV: camera_01", result.msg);
    }

    /**
     * Test ImageColumn validation - invalid dimensions
     */
    @Test
    public void testValidateImageColumnInvalidDimensions() {
        Timestamp startTime = TimestampUtility.getTimestampNow();
        SamplingClock clock = SamplingClock.newBuilder()
                .setStartTime(startTime)
                .setPeriodNanos(1000000L)
                .setCount(1)
                .build();
        DataTimestamps timestamps = DataTimestamps.newBuilder()
                .setSamplingClock(clock)
                .build();
        
        ImageDescriptor descriptor = ImageDescriptor.newBuilder()
                .setWidth(0) // Invalid width
                .setHeight(480)
                .setChannels(3)
                .setEncoding("jpeg")
                .build();
        
        ImageColumn column = ImageColumn.newBuilder()
                .setName("camera_01")
                .setImageDescriptor(descriptor)
                .addImages(ByteString.copyFromUtf8("fake-image-data"))
                .build();
        
        IngestionDataFrame frame = IngestionDataFrame.newBuilder()
                .setDataTimestamps(timestamps)
                .addImageColumns(column)
                .build();
        
        IngestDataRequest request = IngestDataRequest.newBuilder()
                .setProviderId("provider-1")
                .setClientRequestId("request-1")
                .setIngestionDataFrame(frame)
                .build();
        
        ResultStatus result = IngestionValidationUtility.validateIngestionRequest(request);
        assertTrue(result.isError);
        assertTrue(result.msg.contains("imageDescriptor.width must be > 0, got: 0"));
    }

    /**
     * Test ImageColumn validation - missing encoding
     */
    @Test
    public void testValidateImageColumnMissingEncoding() {
        Timestamp startTime = TimestampUtility.getTimestampNow();
        SamplingClock clock = SamplingClock.newBuilder()
                .setStartTime(startTime)
                .setPeriodNanos(1000000L)
                .setCount(1)
                .build();
        DataTimestamps timestamps = DataTimestamps.newBuilder()
                .setSamplingClock(clock)
                .build();
        
        ImageDescriptor descriptor = ImageDescriptor.newBuilder()
                .setWidth(640)
                .setHeight(480)
                .setChannels(3)
                .setEncoding("") // Empty encoding
                .build();
        
        ImageColumn column = ImageColumn.newBuilder()
                .setName("camera_01")
                .setImageDescriptor(descriptor)
                .addImages(ByteString.copyFromUtf8("fake-image-data"))
                .build();
        
        IngestionDataFrame frame = IngestionDataFrame.newBuilder()
                .setDataTimestamps(timestamps)
                .addImageColumns(column)
                .build();
        
        IngestDataRequest request = IngestDataRequest.newBuilder()
                .setProviderId("provider-1")
                .setClientRequestId("request-1")
                .setIngestionDataFrame(frame)
                .build();
        
        ResultStatus result = IngestionValidationUtility.validateIngestionRequest(request);
        assertTrue(result.isError);
        assertEquals("ingestionDataFrame.imageColumns[0].imageDescriptor.encoding must be specified for PV: camera_01", result.msg);
    }

    /**
     * Test StructColumn validation - missing schemaId
     */
    @Test
    public void testValidateStructColumnMissingSchemaId() {
        Timestamp startTime = TimestampUtility.getTimestampNow();
        SamplingClock clock = SamplingClock.newBuilder()
                .setStartTime(startTime)
                .setPeriodNanos(1000000L)
                .setCount(1)
                .build();
        DataTimestamps timestamps = DataTimestamps.newBuilder()
                .setSamplingClock(clock)
                .build();
        
        StructColumn column = StructColumn.newBuilder()
                .setName("complex_sensor")
                .setSchemaId("") // Empty schemaId
                .addValues(ByteString.copyFromUtf8("fake-struct-data"))
                .build();
        
        IngestionDataFrame frame = IngestionDataFrame.newBuilder()
                .setDataTimestamps(timestamps)
                .addStructColumns(column)
                .build();
        
        IngestDataRequest request = IngestDataRequest.newBuilder()
                .setProviderId("provider-1")
                .setClientRequestId("request-1")
                .setIngestionDataFrame(frame)
                .build();
        
        ResultStatus result = IngestionValidationUtility.validateIngestionRequest(request);
        assertTrue(result.isError);
        assertEquals("ingestionDataFrame.structColumns[0].schemaId must be specified for PV: complex_sensor", result.msg);
    }

    /**
     * Test SerializedDataColumn validation - missing encoding
     */
    @Test
    public void testValidateSerializedColumnMissingEncoding() {
        Timestamp startTime = TimestampUtility.getTimestampNow();
        SamplingClock clock = SamplingClock.newBuilder()
                .setStartTime(startTime)
                .setPeriodNanos(1000000L)
                .setCount(1)
                .build();
        DataTimestamps timestamps = DataTimestamps.newBuilder()
                .setSamplingClock(clock)
                .build();
        
        SerializedDataColumn column = SerializedDataColumn.newBuilder()
                .setName("serialized_sensor")
                .setEncoding("") // Empty encoding
                .setPayload(ByteString.copyFromUtf8("fake-payload"))
                .build();
        
        IngestionDataFrame frame = IngestionDataFrame.newBuilder()
                .setDataTimestamps(timestamps)
                .addSerializedDataColumns(column)
                .build();
        
        IngestDataRequest request = IngestDataRequest.newBuilder()
                .setProviderId("provider-1")
                .setClientRequestId("request-1")
                .setIngestionDataFrame(frame)
                .build();
        
        ResultStatus result = IngestionValidationUtility.validateIngestionRequest(request);
        assertTrue(result.isError);
        assertEquals("ingestionDataFrame.serializedDataColumns[0].encoding must be specified for PV: serialized_sensor", result.msg);
    }

    /**
     * Test duplicate PV names across different column types
     */
    @Test
    public void testValidateDuplicatePVNames() {
        Timestamp startTime = TimestampUtility.getTimestampNow();
        SamplingClock clock = SamplingClock.newBuilder()
                .setStartTime(startTime)
                .setPeriodNanos(1000000L)
                .setCount(2)
                .build();
        DataTimestamps timestamps = DataTimestamps.newBuilder()
                .setSamplingClock(clock)
                .build();
        
        DoubleColumn doubleColumn = DoubleColumn.newBuilder()
                .setName("sensor_01") // Same name as float column
                .addAllValues(Arrays.asList(1.23, 4.56))
                .build();
        
        FloatColumn floatColumn = FloatColumn.newBuilder()
                .setName("sensor_01") // Duplicate name
                .addAllValues(Arrays.asList(1.0f, 2.0f))
                .build();
        
        IngestionDataFrame frame = IngestionDataFrame.newBuilder()
                .setDataTimestamps(timestamps)
                .addDoubleColumns(doubleColumn)
                .addFloatColumns(floatColumn)
                .build();
        
        IngestDataRequest request = IngestDataRequest.newBuilder()
                .setProviderId("provider-1")
                .setClientRequestId("request-1")
                .setIngestionDataFrame(frame)
                .build();
        
        ResultStatus result = IngestionValidationUtility.validateIngestionRequest(request);
        assertTrue(result.isError);
        assertTrue(result.msg.contains("Duplicate PV name found: 'sensor_01'"));
        assertTrue(result.msg.contains("floatColumns[0]"));
    }

    /**
     * Test timestamp validation - invalid timestamp values
     */
    @Test
    public void testValidateInvalidTimestamps() {
        Timestamp invalidTimestamp = Timestamp.newBuilder()
                .setEpochSeconds(123456789L)
                .setNanoseconds(1_500_000_000L) // Invalid - exceeds 1 billion nanos
                .build();
        
        TimestampList timestampList = TimestampList.newBuilder()
                .addTimestamps(invalidTimestamp)
                .build();
        
        DataTimestamps timestamps = DataTimestamps.newBuilder()
                .setTimestampList(timestampList)
                .build();
        
        DoubleColumn column = DoubleColumn.newBuilder()
                .setName("sensor_01")
                .addValues(1.23)
                .build();
        
        IngestionDataFrame frame = IngestionDataFrame.newBuilder()
                .setDataTimestamps(timestamps)
                .addDoubleColumns(column)
                .build();
        
        IngestDataRequest request = IngestDataRequest.newBuilder()
                .setProviderId("provider-1")
                .setClientRequestId("request-1")
                .setIngestionDataFrame(frame)
                .build();
        
        ResultStatus result = IngestionValidationUtility.validateIngestionRequest(request);
        assertTrue(result.isError);
        assertTrue(result.msg.contains("timestamps[0] has invalid values"));
        assertTrue(result.msg.contains("nanos=1500000000"));
    }

    /**
     * Test timestamp validation - non-increasing timestamps
     */
    @Test
    public void testValidateNonIncreasingTimestamps() {
        Timestamp timestamp1 = Timestamp.newBuilder()
                .setEpochSeconds(100L)
                .setNanoseconds(500_000_000L)
                .build();
        
        Timestamp timestamp2 = Timestamp.newBuilder()
                .setEpochSeconds(100L)
                .setNanoseconds(300_000_000L) // Earlier than timestamp1
                .build();
        
        TimestampList timestampList = TimestampList.newBuilder()
                .addTimestamps(timestamp1)
                .addTimestamps(timestamp2)
                .build();
        
        DataTimestamps timestamps = DataTimestamps.newBuilder()
                .setTimestampList(timestampList)
                .build();
        
        DoubleColumn column = DoubleColumn.newBuilder()
                .setName("sensor_01")
                .addAllValues(Arrays.asList(1.23, 4.56))
                .build();
        
        IngestionDataFrame frame = IngestionDataFrame.newBuilder()
                .setDataTimestamps(timestamps)
                .addDoubleColumns(column)
                .build();
        
        IngestDataRequest request = IngestDataRequest.newBuilder()
                .setProviderId("provider-1")
                .setClientRequestId("request-1")
                .setIngestionDataFrame(frame)
                .build();
        
        ResultStatus result = IngestionValidationUtility.validateIngestionRequest(request);
        assertTrue(result.isError);
        assertTrue(result.msg.contains("timestamps[1] is not non-decreasing"));
        assertTrue(result.msg.contains("previous=100.500000000"));
        assertTrue(result.msg.contains("current=100.300000000"));
    }

    /**
     * Test successful validation with multiple new column types
     */
    @Test
    public void testValidateSuccessfulNewColumns() {
        Timestamp startTime = TimestampUtility.getTimestampNow();
        SamplingClock clock = SamplingClock.newBuilder()
                .setStartTime(startTime)
                .setPeriodNanos(1000000L)
                .setCount(2)
                .build();
        DataTimestamps timestamps = DataTimestamps.newBuilder()
                .setSamplingClock(clock)
                .build();
        
        DoubleColumn doubleColumn = DoubleColumn.newBuilder()
                .setName("temperature")
                .addAllValues(Arrays.asList(25.5, 26.1))
                .build();
        
        StringColumn stringColumn = StringColumn.newBuilder()
                .setName("status")
                .addAllValues(Arrays.asList("OK", "WARNING"))
                .build();
        
        ArrayDimensions dimensions = ArrayDimensions.newBuilder()
                .addAllDims(Arrays.asList(2, 2))
                .build();
        
        DoubleArrayColumn arrayColumn = DoubleArrayColumn.newBuilder()
                .setName("matrix_data")
                .setDimensions(dimensions)
                .addAllValues(Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0)) // 2 samples * 4 elements
                .build();
        
        IngestionDataFrame frame = IngestionDataFrame.newBuilder()
                .setDataTimestamps(timestamps)
                .addDoubleColumns(doubleColumn)
                .addStringColumns(stringColumn)
                .addDoubleArrayColumns(arrayColumn)
                .build();
        
        IngestDataRequest request = IngestDataRequest.newBuilder()
                .setProviderId("provider-1")
                .setClientRequestId("request-1")
                .setIngestionDataFrame(frame)
                .build();
        
        ResultStatus result = IngestionValidationUtility.validateIngestionRequest(request);
        assertFalse(result.isError);
        assertEquals("", result.msg);
    }

}
