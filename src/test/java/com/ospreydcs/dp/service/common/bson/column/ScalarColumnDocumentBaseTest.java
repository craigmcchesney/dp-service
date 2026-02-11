package com.ospreydcs.dp.service.common.bson.column;

import com.ospreydcs.dp.grpc.v1.common.DataBucket;
import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataValue;
import com.ospreydcs.dp.grpc.v1.common.DoubleColumn;
import com.ospreydcs.dp.service.common.exception.DpException;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class ScalarColumnDocumentBaseTest {

    private DoubleColumnDocument document;
    private DoubleColumn testDoubleColumn;

    @Before
    public void setUp() {
        testDoubleColumn = DoubleColumn.newBuilder()
                .setName("test_pv")
                .addAllValues(Arrays.asList(1.1, 2.2, 3.3, 4.4))
                .build();
        
        document = DoubleColumnDocument.fromDoubleColumn(testDoubleColumn);
    }

    @Test
    public void testFromDoubleColumn() {
        assertEquals("test_pv", document.getName());
        List<Double> values = document.getValues();
        assertEquals(4, values.size());
        assertEquals(Double.valueOf(1.1), values.get(0));
        assertEquals(Double.valueOf(2.2), values.get(1));
        assertEquals(Double.valueOf(3.3), values.get(2));
        assertEquals(Double.valueOf(4.4), values.get(3));
    }

    @Test
    public void testAddColumnToBucket() throws DpException {
        DataBucket.Builder bucketBuilder = DataBucket.newBuilder();
        document.addColumnToBucket(bucketBuilder);
        
        DataBucket bucket = bucketBuilder.build();
        assertTrue(bucket.hasDoubleColumn());
        DoubleColumn doubleColumn = bucket.getDoubleColumn();
        assertEquals("test_pv", doubleColumn.getName());
        assertEquals(4, doubleColumn.getValuesCount());
        assertEquals(1.1, doubleColumn.getValues(0), 0.001);
        assertEquals(2.2, doubleColumn.getValues(1), 0.001);
        assertEquals(3.3, doubleColumn.getValues(2), 0.001);
        assertEquals(4.4, doubleColumn.getValues(3), 0.001);
    }

    @Test
    public void testToDataColumn() throws DpException {
        DataColumn dataColumn = document.toDataColumn();
        assertEquals("test_pv", dataColumn.getName());
        assertEquals(4, dataColumn.getDataValuesCount());
        
        DataValue value0 = dataColumn.getDataValues(0);
        assertTrue(value0.hasDoubleValue());
        assertEquals(1.1, value0.getDoubleValue(), 0.001);
        
        DataValue value1 = dataColumn.getDataValues(1);
        assertTrue(value1.hasDoubleValue());
        assertEquals(2.2, value1.getDoubleValue(), 0.001);
        
        DataValue value2 = dataColumn.getDataValues(2);
        assertTrue(value2.hasDoubleValue());
        assertEquals(3.3, value2.getDoubleValue(), 0.001);
        
        DataValue value3 = dataColumn.getDataValues(3);
        assertTrue(value3.hasDoubleValue());
        assertEquals(4.4, value3.getDoubleValue(), 0.001);
    }

    @Test
    public void testToByteArray() {
        byte[] bytes = document.toByteArray();
        assertNotNull(bytes);
        assertTrue(bytes.length > 0);
    }
}