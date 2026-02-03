package com.ospreydcs.dp.service.ingest.handler;

import com.ospreydcs.dp.grpc.v1.common.*;
import com.ospreydcs.dp.grpc.v1.ingestion.*;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest.IngestionDataFrame;
import com.ospreydcs.dp.service.common.model.ResultStatus;
import com.ospreydcs.dp.service.ingest.service.IngestionServiceImpl;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class IngestionValidationUtility {

    // Configuration constants
    private static final int MAX_STRING_LENGTH = 256;
    private static final int MAX_ARRAY_ELEMENT_COUNT = 10_000_000;
    private static final int MAX_IMAGE_SIZE_BYTES = 50_000_000;  // 50MB
    private static final int MAX_STRUCT_SIZE_BYTES = 1_000_000;   // 1MB

    public static ResultStatus validateIngestionRequest(IngestDataRequest request) {
        // Layer 1: Basic request validation
        ResultStatus basicValidation = validateBasicRequest(request);
        if (basicValidation.isError) {
            return basicValidation;
        }

        IngestionDataFrame frame = request.getIngestionDataFrame();
        int sampleCount = getSampleCount(frame.getDataTimestamps());

        // Layer 2: Timestamp validation
        ResultStatus timestampValidation = validateDataTimestamps(frame.getDataTimestamps(), sampleCount);
        if (timestampValidation.isError) {
            return timestampValidation;
        }

        // Layer 3: Legacy column validation
        ResultStatus legacyValidation = validateLegacyColumns(frame, sampleCount);
        if (legacyValidation.isError) {
            return legacyValidation;
        }

        // Layer 4: New column validation
        ResultStatus newColumnsValidation = validateNewColumns(frame, sampleCount);
        if (newColumnsValidation.isError) {
            return newColumnsValidation;
        }

        // Layer 5: Cross-cutting validation
        return validateUniqueColumnNames(frame);
    }

    private static ResultStatus validateBasicRequest(IngestDataRequest request) {
        String providerId = request.getProviderId();
        String clientRequestId = request.getClientRequestId();

        if (providerId == null || providerId.isBlank()) {
            return new ResultStatus(true, "providerId must be specified");
        }

        if (clientRequestId == null || clientRequestId.isEmpty()) {
            return new ResultStatus(true, "clientRequestId must be specified");
        }

        if (!request.hasIngestionDataFrame()) {
            return new ResultStatus(true, "ingestionDataFrame must be provided");
        }

        if (!request.getIngestionDataFrame().hasDataTimestamps()) {
            return new ResultStatus(true, "ingestionDataFrame.dataTimestamps must be provided");
        }

        // Check that at least one column is provided
        IngestionDataFrame frame = request.getIngestionDataFrame();
        int totalColumns = frame.getDataColumnsCount()
                + frame.getSerializedDataColumnsCount()
                + frame.getDoubleColumnsCount()
                + frame.getFloatColumnsCount()
                + frame.getInt64ColumnsCount()
                + frame.getInt32ColumnsCount()
                + frame.getBoolColumnsCount()
                + frame.getStringColumnsCount()
                + frame.getEnumColumnsCount()
                + frame.getImageColumnsCount()
                + frame.getStructColumnsCount()
                + frame.getDoubleArrayColumnsCount()
                + frame.getFloatArrayColumnsCount()
                + frame.getInt32ArrayColumnsCount()
                + frame.getInt64ArrayColumnsCount()
                + frame.getBoolArrayColumnsCount();

        if (totalColumns == 0) {
            return new ResultStatus(true, "ingestionDataFrame must contain at least one column");
        }

        return new ResultStatus(false, "");
    }

    private static ResultStatus validateDataTimestamps(DataTimestamps dataTimestamps, int expectedSampleCount) {
        switch (dataTimestamps.getValueCase()) {
            case SAMPLINGCLOCK -> {
                SamplingClock clock = dataTimestamps.getSamplingClock();
                
                if (clock.getCount() <= 0) {
                    return new ResultStatus(true, "ingestionDataFrame.dataTimestamps.samplingClock.count must be > 0, got: " + clock.getCount());
                }
                
                if (clock.getPeriodNanos() <= 0) {
                    return new ResultStatus(true, "ingestionDataFrame.dataTimestamps.samplingClock.periodNanos must be > 0, got: " + clock.getPeriodNanos());
                }
                
                if (!clock.hasStartTime()) {
                    return new ResultStatus(true, "ingestionDataFrame.dataTimestamps.samplingClock.startTime must be provided");
                }
                
                Timestamp startTime = clock.getStartTime();
                if (startTime.getEpochSeconds() < 0 || startTime.getNanoseconds() < 0 || startTime.getNanoseconds() >= 1_000_000_000) {
                    return new ResultStatus(true, "ingestionDataFrame.dataTimestamps.samplingClock.startTime has invalid values: seconds=" + 
                            startTime.getEpochSeconds() + ", nanos=" + startTime.getNanoseconds());
                }
            }
            case TIMESTAMPLIST -> {
                TimestampList timestampList = dataTimestamps.getTimestampList();
                List<Timestamp> timestamps = timestampList.getTimestampsList();
                
                if (timestamps.isEmpty()) {
                    return new ResultStatus(true, "ingestionDataFrame.dataTimestamps.timestampList.timestamps cannot be empty");
                }
                
                if (timestamps.size() != expectedSampleCount) {
                    return new ResultStatus(true, "ingestionDataFrame.dataTimestamps.timestampList.timestamps.length mismatch: expected " + 
                            expectedSampleCount + ", got: " + timestamps.size());
                }
                
                // Validate individual timestamps and ordering
                Timestamp previous = null;
                for (int i = 0; i < timestamps.size(); i++) {
                    Timestamp current = timestamps.get(i);
                    
                    if (current.getEpochSeconds() < 0 || current.getNanoseconds() < 0 || current.getNanoseconds() >= 1_000_000_000) {
                        return new ResultStatus(true, "ingestionDataFrame.dataTimestamps.timestampList.timestamps[" + i + "] has invalid values: seconds=" + 
                                current.getEpochSeconds() + ", nanos=" + current.getNanoseconds());
                    }
                    
                    if (previous != null) {
                        if (current.getEpochSeconds() < previous.getEpochSeconds() || 
                           (current.getEpochSeconds() == previous.getEpochSeconds() && current.getNanoseconds() < previous.getNanoseconds())) {
                            return new ResultStatus(true, "ingestionDataFrame.dataTimestamps.timestampList.timestamps[" + i + "] is not non-decreasing: " +
                                    "previous=" + previous.getEpochSeconds() + "." + previous.getNanoseconds() + 
                                    ", current=" + current.getEpochSeconds() + "." + current.getNanoseconds());
                        }
                    }
                    previous = current;
                }
            }
            case VALUE_NOT_SET -> {
                return new ResultStatus(true, "ingestionDataFrame.dataTimestamps must specify either SamplingClock or TimestampList");
            }
        }
        
        return new ResultStatus(false, "");
    }

    private static ResultStatus validateLegacyColumns(IngestionDataFrame frame, int sampleCount) {
        // Validate DataColumns
        List<DataColumn> dataColumns = frame.getDataColumnsList();
        for (int i = 0; i < dataColumns.size(); i++) {
            DataColumn column = dataColumns.get(i);
            String fieldPath = "ingestionDataFrame.dataColumns[" + i + "]";
            
            if (column.getName() == null || column.getName().isEmpty()) {
                return new ResultStatus(true, fieldPath + ".name must be specified");
            }
            
            if (column.getDataValuesList().size() != sampleCount) {
                return new ResultStatus(true, fieldPath + ".dataValues.length mismatch: expected " + sampleCount + 
                        ", got: " + column.getDataValuesList().size() + " for PV: " + column.getName());
            }
        }

        // Validate SerializedDataColumns
        List<SerializedDataColumn> serializedColumns = frame.getSerializedDataColumnsList();
        for (int i = 0; i < serializedColumns.size(); i++) {
            SerializedDataColumn column = serializedColumns.get(i);
            String fieldPath = "ingestionDataFrame.serializedDataColumns[" + i + "]";
            
            if (column.getName() == null || column.getName().isEmpty()) {
                return new ResultStatus(true, fieldPath + ".name must be specified");
            }
            
            if (column.getEncoding() == null || column.getEncoding().isEmpty()) {
                return new ResultStatus(true, fieldPath + ".encoding must be specified for PV: " + column.getName());
            }
        }

        return new ResultStatus(false, "");
    }

    private static ResultStatus validateNewColumns(IngestionDataFrame frame, int sampleCount) {
        // Validate scalar columns
        ResultStatus scalarValidation = validateScalarColumns(frame, sampleCount);
        if (scalarValidation.isError) {
            return scalarValidation;
        }

        // Validate array columns
        ResultStatus arrayValidation = validateArrayColumns(frame, sampleCount);
        if (arrayValidation.isError) {
            return arrayValidation;
        }

        // Validate image columns
        ResultStatus imageValidation = validateImageColumns(frame, sampleCount);
        if (imageValidation.isError) {
            return imageValidation;
        }

        // Validate struct columns
        return validateStructColumns(frame, sampleCount);
    }

    private static ResultStatus validateScalarColumns(IngestionDataFrame frame, int sampleCount) {
        // DoubleColumn validation
        List<DoubleColumn> doubleColumns = frame.getDoubleColumnsList();
        for (int i = 0; i < doubleColumns.size(); i++) {
            DoubleColumn column = doubleColumns.get(i);
            String fieldPath = "ingestionDataFrame.doubleColumns[" + i + "]";
            
            if (column.getName() == null || column.getName().isEmpty()) {
                return new ResultStatus(true, fieldPath + ".name must be specified");
            }
            
            if (column.getValuesCount() != sampleCount) {
                return new ResultStatus(true, fieldPath + ".values.length mismatch: expected " + sampleCount + 
                        ", got: " + column.getValuesCount() + " for PV: " + column.getName());
            }
        }

        // FloatColumn validation
        List<FloatColumn> floatColumns = frame.getFloatColumnsList();
        for (int i = 0; i < floatColumns.size(); i++) {
            FloatColumn column = floatColumns.get(i);
            String fieldPath = "ingestionDataFrame.floatColumns[" + i + "]";
            
            if (column.getName() == null || column.getName().isEmpty()) {
                return new ResultStatus(true, fieldPath + ".name must be specified");
            }
            
            if (column.getValuesCount() != sampleCount) {
                return new ResultStatus(true, fieldPath + ".values.length mismatch: expected " + sampleCount + 
                        ", got: " + column.getValuesCount() + " for PV: " + column.getName());
            }
        }

        // Int64Column validation
        List<Int64Column> int64Columns = frame.getInt64ColumnsList();
        for (int i = 0; i < int64Columns.size(); i++) {
            Int64Column column = int64Columns.get(i);
            String fieldPath = "ingestionDataFrame.int64Columns[" + i + "]";
            
            if (column.getName() == null || column.getName().isEmpty()) {
                return new ResultStatus(true, fieldPath + ".name must be specified");
            }
            
            if (column.getValuesCount() != sampleCount) {
                return new ResultStatus(true, fieldPath + ".values.length mismatch: expected " + sampleCount + 
                        ", got: " + column.getValuesCount() + " for PV: " + column.getName());
            }
        }

        // Int32Column validation
        List<Int32Column> int32Columns = frame.getInt32ColumnsList();
        for (int i = 0; i < int32Columns.size(); i++) {
            Int32Column column = int32Columns.get(i);
            String fieldPath = "ingestionDataFrame.int32Columns[" + i + "]";
            
            if (column.getName() == null || column.getName().isEmpty()) {
                return new ResultStatus(true, fieldPath + ".name must be specified");
            }
            
            if (column.getValuesCount() != sampleCount) {
                return new ResultStatus(true, fieldPath + ".values.length mismatch: expected " + sampleCount + 
                        ", got: " + column.getValuesCount() + " for PV: " + column.getName());
            }
        }

        // BoolColumn validation
        List<BoolColumn> boolColumns = frame.getBoolColumnsList();
        for (int i = 0; i < boolColumns.size(); i++) {
            BoolColumn column = boolColumns.get(i);
            String fieldPath = "ingestionDataFrame.boolColumns[" + i + "]";
            
            if (column.getName() == null || column.getName().isEmpty()) {
                return new ResultStatus(true, fieldPath + ".name must be specified");
            }
            
            if (column.getValuesCount() != sampleCount) {
                return new ResultStatus(true, fieldPath + ".values.length mismatch: expected " + sampleCount + 
                        ", got: " + column.getValuesCount() + " for PV: " + column.getName());
            }
        }

        // StringColumn validation
        List<StringColumn> stringColumns = frame.getStringColumnsList();
        for (int i = 0; i < stringColumns.size(); i++) {
            StringColumn column = stringColumns.get(i);
            String fieldPath = "ingestionDataFrame.stringColumns[" + i + "]";
            
            if (column.getName() == null || column.getName().isEmpty()) {
                return new ResultStatus(true, fieldPath + ".name must be specified");
            }
            
            if (column.getValuesCount() != sampleCount) {
                return new ResultStatus(true, fieldPath + ".values.length mismatch: expected " + sampleCount + 
                        ", got: " + column.getValuesCount() + " for PV: " + column.getName());
            }

            // Validate string length constraints
            List<String> values = column.getValuesList();
            for (int j = 0; j < values.size(); j++) {
                String value = values.get(j);
                if (value != null && value.length() > MAX_STRING_LENGTH) {
                    return new ResultStatus(true, fieldPath + ".values[" + j + "] string length exceeds maximum: " +
                            "length=" + value.length() + ", max=" + MAX_STRING_LENGTH + " for PV: " + column.getName());
                }
            }
        }

        // EnumColumn validation
        List<EnumColumn> enumColumns = frame.getEnumColumnsList();
        for (int i = 0; i < enumColumns.size(); i++) {
            EnumColumn column = enumColumns.get(i);
            String fieldPath = "ingestionDataFrame.enumColumns[" + i + "]";
            
            if (column.getName() == null || column.getName().isEmpty()) {
                return new ResultStatus(true, fieldPath + ".name must be specified");
            }
            
            if (column.getEnumId() == null || column.getEnumId().isEmpty()) {
                return new ResultStatus(true, fieldPath + ".enumId must be specified for PV: " + column.getName());
            }
            
            if (column.getValuesCount() != sampleCount) {
                return new ResultStatus(true, fieldPath + ".values.length mismatch: expected " + sampleCount + 
                        ", got: " + column.getValuesCount() + " for PV: " + column.getName());
            }
        }

        return new ResultStatus(false, "");
    }

    private static ResultStatus validateArrayColumns(IngestionDataFrame frame, int sampleCount) {
        // DoubleArrayColumn validation
        List<DoubleArrayColumn> doubleArrayColumns = frame.getDoubleArrayColumnsList();
        for (int i = 0; i < doubleArrayColumns.size(); i++) {
            DoubleArrayColumn column = doubleArrayColumns.get(i);
            String fieldPath = "ingestionDataFrame.doubleArrayColumns[" + i + "]";
            
            ResultStatus arrayValidation = validateArrayColumn(fieldPath, column.getName(), 
                    column.getDimensions(), column.getValuesCount(), sampleCount);
            if (arrayValidation.isError) {
                return arrayValidation;
            }
        }

        // FloatArrayColumn validation
        List<FloatArrayColumn> floatArrayColumns = frame.getFloatArrayColumnsList();
        for (int i = 0; i < floatArrayColumns.size(); i++) {
            FloatArrayColumn column = floatArrayColumns.get(i);
            String fieldPath = "ingestionDataFrame.floatArrayColumns[" + i + "]";
            
            ResultStatus arrayValidation = validateArrayColumn(fieldPath, column.getName(), 
                    column.getDimensions(), column.getValuesCount(), sampleCount);
            if (arrayValidation.isError) {
                return arrayValidation;
            }
        }

        // Int32ArrayColumn validation
        List<Int32ArrayColumn> int32ArrayColumns = frame.getInt32ArrayColumnsList();
        for (int i = 0; i < int32ArrayColumns.size(); i++) {
            Int32ArrayColumn column = int32ArrayColumns.get(i);
            String fieldPath = "ingestionDataFrame.int32ArrayColumns[" + i + "]";
            
            ResultStatus arrayValidation = validateArrayColumn(fieldPath, column.getName(), 
                    column.getDimensions(), column.getValuesCount(), sampleCount);
            if (arrayValidation.isError) {
                return arrayValidation;
            }
        }

        // Int64ArrayColumn validation
        List<Int64ArrayColumn> int64ArrayColumns = frame.getInt64ArrayColumnsList();
        for (int i = 0; i < int64ArrayColumns.size(); i++) {
            Int64ArrayColumn column = int64ArrayColumns.get(i);
            String fieldPath = "ingestionDataFrame.int64ArrayColumns[" + i + "]";
            
            ResultStatus arrayValidation = validateArrayColumn(fieldPath, column.getName(), 
                    column.getDimensions(), column.getValuesCount(), sampleCount);
            if (arrayValidation.isError) {
                return arrayValidation;
            }
        }

        // BoolArrayColumn validation
        List<BoolArrayColumn> boolArrayColumns = frame.getBoolArrayColumnsList();
        for (int i = 0; i < boolArrayColumns.size(); i++) {
            BoolArrayColumn column = boolArrayColumns.get(i);
            String fieldPath = "ingestionDataFrame.boolArrayColumns[" + i + "]";
            
            ResultStatus arrayValidation = validateArrayColumn(fieldPath, column.getName(), 
                    column.getDimensions(), column.getValuesCount(), sampleCount);
            if (arrayValidation.isError) {
                return arrayValidation;
            }
        }

        return new ResultStatus(false, "");
    }

    private static ResultStatus validateArrayColumn(String fieldPath, String name, ArrayDimensions dimensions, 
                                                   int valuesCount, int sampleCount) {
        if (name == null || name.isEmpty()) {
            return new ResultStatus(true, fieldPath + ".name must be specified");
        }

        if (dimensions == null) {
            return new ResultStatus(true, fieldPath + ".dimensions must be specified for PV: " + name);
        }

        List<Integer> dims = dimensions.getDimsList();
        if (dims.isEmpty() || dims.size() > 3) {
            return new ResultStatus(true, fieldPath + ".dimensions.dims.size must be in {1, 2, 3}, got: " + 
                    dims.size() + " for PV: " + name);
        }

        long elementCount = 1;
        for (int j = 0; j < dims.size(); j++) {
            int dim = dims.get(j);
            if (dim <= 0) {
                return new ResultStatus(true, fieldPath + ".dimensions.dims[" + j + "] must be > 0, got: " + 
                        dim + " for PV: " + name);
            }
            elementCount *= dim;
        }

        if (elementCount > MAX_ARRAY_ELEMENT_COUNT) {
            return new ResultStatus(true, fieldPath + ".dimensions element_count exceeds maximum: " +
                    "element_count=" + elementCount + ", max=" + MAX_ARRAY_ELEMENT_COUNT + " for PV: " + name);
        }

        long expectedValuesCount = sampleCount * elementCount;
        if (valuesCount != expectedValuesCount) {
            return new ResultStatus(true, fieldPath + ".values.length mismatch: expected " + expectedValuesCount + 
                    " (sampleCount=" + sampleCount + " * element_count=" + elementCount + "), got: " + valuesCount + " for PV: " + name);
        }

        return new ResultStatus(false, "");
    }

    private static ResultStatus validateImageColumns(IngestionDataFrame frame, int sampleCount) {
        List<ImageColumn> imageColumns = frame.getImageColumnsList();
        for (int i = 0; i < imageColumns.size(); i++) {
            ImageColumn column = imageColumns.get(i);
            String fieldPath = "ingestionDataFrame.imageColumns[" + i + "]";
            
            if (column.getName() == null || column.getName().isEmpty()) {
                return new ResultStatus(true, fieldPath + ".name must be specified");
            }

            if (!column.hasImageDescriptor()) {
                return new ResultStatus(true, fieldPath + ".imageDescriptor must be specified for PV: " + column.getName());
            }

            ImageDescriptor descriptor = column.getImageDescriptor();
            if (descriptor.getWidth() <= 0) {
                return new ResultStatus(true, fieldPath + ".imageDescriptor.width must be > 0, got: " + 
                        descriptor.getWidth() + " for PV: " + column.getName());
            }
            
            if (descriptor.getHeight() <= 0) {
                return new ResultStatus(true, fieldPath + ".imageDescriptor.height must be > 0, got: " + 
                        descriptor.getHeight() + " for PV: " + column.getName());
            }
            
            if (descriptor.getChannels() <= 0) {
                return new ResultStatus(true, fieldPath + ".imageDescriptor.channels must be > 0, got: " + 
                        descriptor.getChannels() + " for PV: " + column.getName());
            }

            if (descriptor.getEncoding() == null || descriptor.getEncoding().isEmpty()) {
                return new ResultStatus(true, fieldPath + ".imageDescriptor.encoding must be specified for PV: " + column.getName());
            }

            List<com.google.protobuf.ByteString> images = column.getImagesList();
            if (images.size() != sampleCount) {
                return new ResultStatus(true, fieldPath + ".images.length mismatch: expected " + sampleCount + 
                        ", got: " + images.size() + " for PV: " + column.getName());
            }

            for (int j = 0; j < images.size(); j++) {
                com.google.protobuf.ByteString image = images.get(j);
                if (image.size() > MAX_IMAGE_SIZE_BYTES) {
                    return new ResultStatus(true, fieldPath + ".images[" + j + "] size exceeds maximum: " +
                            "size=" + image.size() + ", max=" + MAX_IMAGE_SIZE_BYTES + " for PV: " + column.getName());
                }
            }
        }

        return new ResultStatus(false, "");
    }

    private static ResultStatus validateStructColumns(IngestionDataFrame frame, int sampleCount) {
        List<StructColumn> structColumns = frame.getStructColumnsList();
        for (int i = 0; i < structColumns.size(); i++) {
            StructColumn column = structColumns.get(i);
            String fieldPath = "ingestionDataFrame.structColumns[" + i + "]";
            
            if (column.getName() == null || column.getName().isEmpty()) {
                return new ResultStatus(true, fieldPath + ".name must be specified");
            }

            if (column.getSchemaId() == null || column.getSchemaId().isEmpty()) {
                return new ResultStatus(true, fieldPath + ".schemaId must be specified for PV: " + column.getName());
            }

            List<com.google.protobuf.ByteString> values = column.getValuesList();
            if (values.size() != sampleCount) {
                return new ResultStatus(true, fieldPath + ".values.length mismatch: expected " + sampleCount + 
                        ", got: " + values.size() + " for PV: " + column.getName());
            }

            for (int j = 0; j < values.size(); j++) {
                com.google.protobuf.ByteString value = values.get(j);
                if (value.size() > MAX_STRUCT_SIZE_BYTES) {
                    return new ResultStatus(true, fieldPath + ".values[" + j + "] size exceeds maximum: " +
                            "size=" + value.size() + ", max=" + MAX_STRUCT_SIZE_BYTES + " for PV: " + column.getName());
                }
            }
        }

        return new ResultStatus(false, "");
    }

    private static ResultStatus validateUniqueColumnNames(IngestionDataFrame frame) {
        Set<String> columnNames = new HashSet<>();

        // Check DataColumns
        for (int i = 0; i < frame.getDataColumnsList().size(); i++) {
            String name = frame.getDataColumnsList().get(i).getName();
            if (name != null && !name.isEmpty()) {
                if (!columnNames.add(name)) {
                    return new ResultStatus(true, "Duplicate PV name found: '" + name + "' in ingestionDataFrame.dataColumns[" + i + "]");
                }
            }
        }

        // Check SerializedDataColumns
        for (int i = 0; i < frame.getSerializedDataColumnsList().size(); i++) {
            String name = frame.getSerializedDataColumnsList().get(i).getName();
            if (name != null && !name.isEmpty()) {
                if (!columnNames.add(name)) {
                    return new ResultStatus(true, "Duplicate PV name found: '" + name + "' in ingestionDataFrame.serializedDataColumns[" + i + "]");
                }
            }
        }

        // Check new column types
        String[] columnTypes = {"doubleColumns", "floatColumns", "int64Columns", "int32Columns", "boolColumns", 
                               "stringColumns", "enumColumns", "imageColumns", "structColumns",
                               "doubleArrayColumns", "floatArrayColumns", "int32ArrayColumns", "int64ArrayColumns", "boolArrayColumns"};

        List<? extends Object>[] columnLists = new List[]{
            frame.getDoubleColumnsList(), frame.getFloatColumnsList(), frame.getInt64ColumnsList(), 
            frame.getInt32ColumnsList(), frame.getBoolColumnsList(), frame.getStringColumnsList(), 
            frame.getEnumColumnsList(), frame.getImageColumnsList(), frame.getStructColumnsList(),
            frame.getDoubleArrayColumnsList(), frame.getFloatArrayColumnsList(), frame.getInt32ArrayColumnsList(),
            frame.getInt64ArrayColumnsList(), frame.getBoolArrayColumnsList()
        };

        for (int t = 0; t < columnTypes.length; t++) {
            List<?> columns = columnLists[t];
            for (int i = 0; i < columns.size(); i++) {
                Object column = columns.get(i);
                String name = getColumnName(column);
                if (name != null && !name.isEmpty()) {
                    if (!columnNames.add(name)) {
                        return new ResultStatus(true, "Duplicate PV name found: '" + name + "' in ingestionDataFrame." + columnTypes[t] + "[" + i + "]");
                    }
                }
            }
        }

        return new ResultStatus(false, "");
    }

    private static String getColumnName(Object column) {
        if (column instanceof DoubleColumn) return ((DoubleColumn) column).getName();
        if (column instanceof FloatColumn) return ((FloatColumn) column).getName();
        if (column instanceof Int64Column) return ((Int64Column) column).getName();
        if (column instanceof Int32Column) return ((Int32Column) column).getName();
        if (column instanceof BoolColumn) return ((BoolColumn) column).getName();
        if (column instanceof StringColumn) return ((StringColumn) column).getName();
        if (column instanceof EnumColumn) return ((EnumColumn) column).getName();
        if (column instanceof ImageColumn) return ((ImageColumn) column).getName();
        if (column instanceof StructColumn) return ((StructColumn) column).getName();
        if (column instanceof DoubleArrayColumn) return ((DoubleArrayColumn) column).getName();
        if (column instanceof FloatArrayColumn) return ((FloatArrayColumn) column).getName();
        if (column instanceof Int32ArrayColumn) return ((Int32ArrayColumn) column).getName();
        if (column instanceof Int64ArrayColumn) return ((Int64ArrayColumn) column).getName();
        if (column instanceof BoolArrayColumn) return ((BoolArrayColumn) column).getName();
        return null;
    }

    private static int getSampleCount(DataTimestamps dataTimestamps) {
        switch (dataTimestamps.getValueCase()) {
            case SAMPLINGCLOCK -> {
                return (int) dataTimestamps.getSamplingClock().getCount();
            }
            case TIMESTAMPLIST -> {
                return dataTimestamps.getTimestampList().getTimestampsCount();
            }
            case VALUE_NOT_SET -> {
                return 0;
            }
        }
        return 0;
    }
}
