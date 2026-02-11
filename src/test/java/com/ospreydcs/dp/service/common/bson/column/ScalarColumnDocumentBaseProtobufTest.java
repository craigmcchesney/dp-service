package com.ospreydcs.dp.service.common.bson.column;

import com.google.protobuf.Message;
import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.common.DoubleColumn;
import com.ospreydcs.dp.service.common.exception.DpException;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Test coverage for ScalarColumnDocumentBase.toProtobufColumn() method
 * which is critical for tabular query results and export functionality.
 */
public class ScalarColumnDocumentBaseProtobufTest {

    private DoubleColumnDocument doubleColumnDocument;

    @Before
    public void setUp() {
        // Create a DoubleColumnDocument with test data
        doubleColumnDocument = new DoubleColumnDocument();
        doubleColumnDocument.setName("test_pv_protobuf");
        doubleColumnDocument.setValues(Arrays.asList(1.1, 2.2, 3.3, 4.4, 5.5));
    }

    @Test
    public void testToProtobufColumn() {
        // Test the generic toProtobufColumn() method
        Message protobufMessage = doubleColumnDocument.toProtobufColumn();
        
        // Verify it returns the correct type
        assertTrue("Should return DoubleColumn", protobufMessage instanceof DoubleColumn);
        
        DoubleColumn doubleColumn = (DoubleColumn) protobufMessage;
        
        // Verify the protobuf content
        assertEquals("test_pv_protobuf", doubleColumn.getName());
        assertEquals(5, doubleColumn.getValuesCount());
        assertEquals(1.1, doubleColumn.getValues(0), 0.001);
        assertEquals(2.2, doubleColumn.getValues(1), 0.001);
        assertEquals(3.3, doubleColumn.getValues(2), 0.001);
        assertEquals(4.4, doubleColumn.getValues(3), 0.001);
        assertEquals(5.5, doubleColumn.getValues(4), 0.001);
    }

    @Test
    public void testToProtobufColumnRoundTrip() {
        // Test round-trip: protobuf -> document -> protobuf
        DoubleColumn originalColumn = DoubleColumn.newBuilder()
                .setName("round_trip_test")
                .addAllValues(Arrays.asList(10.1, 20.2, 30.3))
                .build();
        
        // Create document from protobuf
        DoubleColumnDocument document = DoubleColumnDocument.fromDoubleColumn(originalColumn);
        
        // Convert back to protobuf using toProtobufColumn()
        DoubleColumn reconstructed = (DoubleColumn) document.toProtobufColumn();
        
        // Verify they match
        assertEquals(originalColumn.getName(), reconstructed.getName());
        assertEquals(originalColumn.getValuesCount(), reconstructed.getValuesCount());
        for (int i = 0; i < originalColumn.getValuesCount(); i++) {
            assertEquals(originalColumn.getValues(i), reconstructed.getValues(i), 0.001);
        }
    }

    @Test
    public void testToDataColumnConversion() throws DpException {
        // Test the inherited toDataColumn() method which uses toProtobufColumn() internally
        DataColumn dataColumn = doubleColumnDocument.toDataColumn();
        
        assertEquals("test_pv_protobuf", dataColumn.getName());
        assertEquals(5, dataColumn.getDataValuesCount());
        
        // Verify each DataValue
        for (int i = 0; i < 5; i++) {
            DataValue dataValue = dataColumn.getDataValues(i);
            assertTrue("Should have double value", dataValue.hasDoubleValue());
            
            double expectedValue = (i + 1) * 1.1; // 1.1, 2.2, 3.3, 4.4, 5.5
            assertEquals(expectedValue, dataValue.getDoubleValue(), 0.001);
        }
    }

    @Test
    public void testToByteArrayUsesToProtobufColumn() {
        // Test that toByteArray() uses toProtobufColumn() internally
        byte[] bytes = doubleColumnDocument.toByteArray();
        
        assertNotNull("Bytes should not be null", bytes);
        assertTrue("Bytes should not be empty", bytes.length > 0);
        
        // Verify we can deserialize the bytes back to the original protobuf
        try {
            DoubleColumn deserialized = DoubleColumn.parseFrom(bytes);
            assertEquals("test_pv_protobuf", deserialized.getName());
            assertEquals(5, deserialized.getValuesCount());
            assertEquals(1.1, deserialized.getValues(0), 0.001);
        } catch (Exception e) {
            fail("Should be able to deserialize bytes: " + e.getMessage());
        }
    }

    @Test
    public void testEmptyValues() {
        // Test with empty values list
        doubleColumnDocument.setValues(Arrays.asList());
        
        DoubleColumn doubleColumn = (DoubleColumn) doubleColumnDocument.toProtobufColumn();
        assertEquals("test_pv_protobuf", doubleColumn.getName());
        assertEquals(0, doubleColumn.getValuesCount());
    }

    @Test
    public void testNullName() {
        // Test with null name
        doubleColumnDocument.setName(null);
        
        DoubleColumn doubleColumn = (DoubleColumn) doubleColumnDocument.toProtobufColumn();
        assertTrue("Name should be empty string when null", doubleColumn.getName().isEmpty());
    }

    @Test
    public void testLargeDataset() {
        // Test with larger dataset (simulating high-frequency data)
        List<Double> largeDataset = Arrays.asList(
            generateDoubleSequence(1000) // 1000 values
        );
        doubleColumnDocument.setValues(largeDataset);
        
        DoubleColumn doubleColumn = (DoubleColumn) doubleColumnDocument.toProtobufColumn();
        assertEquals(1000, doubleColumn.getValuesCount());
        
        // Spot check some values
        assertEquals(0.0, doubleColumn.getValues(0), 0.001);
        assertEquals(500.0, doubleColumn.getValues(500), 0.001);
        assertEquals(999.0, doubleColumn.getValues(999), 0.001);
    }

    private Double[] generateDoubleSequence(int count) {
        Double[] values = new Double[count];
        for (int i = 0; i < count; i++) {
            values[i] = (double) i;
        }
        return values;
    }
}