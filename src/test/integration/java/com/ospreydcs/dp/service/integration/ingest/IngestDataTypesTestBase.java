package com.ospreydcs.dp.service.integration.ingest;

import com.google.protobuf.ByteString;
import com.ospreydcs.dp.grpc.v1.common.*;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.ingest.IngestionTestBase;
import com.ospreydcs.dp.service.integration.GrpcIntegrationTestBase;
import com.ospreydcs.dp.service.query.QueryTestBase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.util.*;

import static org.junit.Assert.*;

public abstract class IngestDataTypesTestBase extends GrpcIntegrationTestBase {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    // constants
    private static final String INGESTION_PROVIDER_ID = "1";
    public static final String CFG_KEY_START_SECONDS = "IngestionBenchmark.startSeconds";
    public static final Long DEFAULT_START_SECONDS = 1698767462L;

    protected static class ArrayDataValueModel {
        public final List<List<String>> array2D;
        public ArrayDataValueModel(List<List<String>> array2D) {
            this.array2D = array2D;
        }
    }
    
    protected static class StructureDataValueModel {
        public final String stringField;
        public final Boolean booleanField;
        public final Double doubleField;
        public final Integer integerField;
        public final Long timestampSeconds;
        public final Long timestampNanos;
        public final List<List<String>> array2D;
        public StructureDataValueModel(
                String stringField,
                Boolean booleanField,
                Double doubleField,
                Integer integerField,
                Long timestampSeconds,
                Long timestampNanos,
                List<List<String>> array2D
        ) {
            this.stringField = stringField;
            this.booleanField = booleanField;
            this.doubleField = doubleField;
            this.integerField = integerField;
            this.timestampSeconds = timestampSeconds;
            this.timestampNanos = timestampNanos;
            this.array2D = array2D;
        }        
    }

    protected abstract List<BucketDocument> sendAndVerifyIngestionRpc_(
            IngestionTestBase.IngestionRequestParams params,
            IngestDataRequest ingestionRequest
    );

    public void ingestionDataTypesTest() {

        final long startSeconds = configMgr().getConfigLong(CFG_KEY_START_SECONDS, DEFAULT_START_SECONDS);
        final long startNanos = 0L;

        // register ingestion provider
        final String providerName = INGESTION_PROVIDER_ID;
        final String providerId = ingestionServiceWrapper.registerProvider(providerName, null);


        List<DataColumn> arrayDataColumnList = null;
        Map<DataColumn, List<ArrayDataValueModel>> arrayValidationMap = null;
        {
            // ingest 2D array data

            // create IngestionRequestParams
            final String requestId = "request-1";
            final List<String> pvNames = Arrays.asList(
                    "array_pv_01", "array_pv_02", "array_pv_03", "array_pv_04", "array_pv_05");
            final int numPvs = pvNames.size();
            final int numSamples = 5;
            final long samplePeriod = 1_000_000_000L / numSamples;
            final long endSeconds = startSeconds;
            final long endNanos = samplePeriod * (numSamples - 1);

            // build list of DataColumns
            arrayDataColumnList = new ArrayList<>(pvNames.size());
            arrayValidationMap = new HashMap<>(pvNames.size());
            for(int colIndex = 0 ; colIndex < numPvs ; ++colIndex) {

                final DataColumn.Builder dataColumnBuilder = DataColumn.newBuilder();
                final String pvName = pvNames.get(colIndex);
                dataColumnBuilder.setName(pvName);

                final List<ArrayDataValueModel> dataValueModelList = new ArrayList<>();

                for (int rowIndex = 0 ; rowIndex < numSamples ; ++rowIndex) {
                    final DataValue.Builder outerArrayValueBuilder = DataValue.newBuilder();
                    final Array.Builder outerArrayBuilder = Array.newBuilder();

                    final List<List<String>> array2D = new ArrayList<>();

                    for (int arrayValueIndex = 0 ; arrayValueIndex < 5 ; ++arrayValueIndex) {
                        final DataValue.Builder innerArrayValueBuilder = DataValue.newBuilder();
                        final Array.Builder innerArrayBuilder = Array.newBuilder();

                        final List<String> array1D = new ArrayList<>();

                        for (int arrayElementIndex = 0 ; arrayElementIndex < 5 ; ++arrayElementIndex) {
                            final String arrayElementValueString =
                                    pvName + ":" + rowIndex + ":" + arrayValueIndex + ":" + arrayElementIndex;
                            final DataValue arrayElementValue =
                                    DataValue.newBuilder().setStringValue(arrayElementValueString).build();
                            innerArrayBuilder.addDataValues(arrayElementValue);
                            array1D.add(arrayElementValueString);
                        }

                        innerArrayBuilder.build();
                        innerArrayValueBuilder.setArrayValue(innerArrayBuilder);
                        innerArrayValueBuilder.build();
                        outerArrayBuilder.addDataValues(innerArrayValueBuilder);
                        array2D.add(array1D);
                    }

                    outerArrayBuilder.build();
                    outerArrayValueBuilder.setArrayValue(outerArrayBuilder);
                    outerArrayValueBuilder.build();
                    dataColumnBuilder.addDataValues(outerArrayValueBuilder);
                    final ArrayDataValueModel arrayDataValueModel = new ArrayDataValueModel(array2D);
                    dataValueModelList.add(arrayDataValueModel);
                }

                DataColumn dataColumn = dataColumnBuilder.build();
                arrayDataColumnList.add(dataColumn);
                arrayValidationMap.put(dataColumn, dataValueModelList);
            }

            final IngestionTestBase.IngestionRequestParams params =
                    new IngestionTestBase.IngestionRequestParams(
                            providerId,
                            requestId,
                            null,
                            null,
                            startSeconds,
                            startNanos,
                            samplePeriod, // 5 values per second
                            numSamples, // each DataColumn must contain 5 DataValues
                            pvNames,
                            null,
                            null,
                            null,
                            false,
                            arrayDataColumnList
                    );

            // build request
            final IngestDataRequest ingestionRequest =
                    IngestionTestBase.buildIngestionRequest(params);

            // send request
            final List<BucketDocument> bucketDocumentList=
                    sendAndVerifyIngestionRpc_(params, ingestionRequest);

            // verify contents of mongo bucket documents - the comparison of DataColumns in request to those in
            // bucket document by sendAndVerifyIngestionRpc_() is probably sufficient, but checking that the
            // data values are as expected
            int columnIndex = 0;
            for (BucketDocument bucketDocument : bucketDocumentList) {

                final DataColumn requestColumn = arrayDataColumnList.get(columnIndex);

                DataColumn bucketDataColumn = null;
                try {
                    bucketDataColumn = bucketDocument.getDataColumn().toDataColumn();
                } catch (DpException e) {
                    throw new RuntimeException(e);
                }
                Objects.requireNonNull(bucketDataColumn);

                final List<ArrayDataValueModel> requestValueModelList = arrayValidationMap.get(requestColumn);
                assertEquals(requestValueModelList.size(), bucketDataColumn.getDataValuesCount());

                int rowIndex = 0;
                for (ArrayDataValueModel arrayDataValueModel : requestValueModelList) {
                    final List<List<String>> array2D = arrayDataValueModel.array2D;
                    final DataValue bucketValue = bucketDataColumn.getDataValues(rowIndex);
                    assertTrue(bucketValue.hasArrayValue());
                    final Array bucketArray = bucketValue.getArrayValue();
                    verify2DArray(array2D, bucketArray);

                    rowIndex = rowIndex + 1;
                }

                columnIndex = columnIndex + 1;
            }

            // perform query for single pv and verify results
            final List<String> queryPvNames = Arrays.asList("array_pv_01");
            final DataColumn requestColumn = arrayDataColumnList.get(0);

            final QueryTestBase.QueryDataRequestParams queryDataRequestParams =
                    new QueryTestBase.QueryDataRequestParams(
                            queryPvNames,
                            startSeconds,
                            startNanos,
                            endSeconds,
                            endNanos,
                            false
                    );
            final List<DataBucket> queryBuckets = queryServiceWrapper.queryDataStream(
                    queryDataRequestParams, false, "");
            assertEquals(queryPvNames.size(), queryBuckets.size());
            final DataBucket responseBucket = queryBuckets.get(0);
            QueryTestBase.verifyDataBucket(responseBucket, requestColumn, startSeconds, startNanos, samplePeriod, numSamples);
        }



        ByteString imageByteString = null;
        {
            // ingest Image data type
            try {
                InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream("test-image.bmp");
                imageByteString = ByteString.readFrom(inputStream);
            } catch (IOException ex) {
                fail("error loading test-image.bmp: " + ex.getMessage());
            }

            // create IngestionRequestParams
            final String requestId = "request-3";
            final List<String> pvNames = Arrays.asList(
                    "image_pv_01", "image_pv_02", "image_pv_03", "image_pv_04", "image_pv_05");
            final int numPvs = pvNames.size();
            final int numSamples = 5;
            final long samplePeriod = 1_000_000_000L / numSamples;
            final long endSeconds = startSeconds;
            final long endNanos = samplePeriod * (numSamples - 1);

            // build list of DataColumns
            final List<DataColumn> dataColumnList = new ArrayList<>();
            for(int colIndex = 0 ; colIndex < numPvs ; ++colIndex) {

                final DataColumn.Builder dataColumnBuilder = DataColumn.newBuilder();
                final String pvName = pvNames.get(colIndex);
                dataColumnBuilder.setName(pvName);

                for (int rowIndex = 0 ; rowIndex < numSamples ; ++rowIndex) {
                    final DataValue.Builder imageDataValueBuilder = DataValue.newBuilder();
                    final Image image =
                            Image.newBuilder().setFileType(Image.FileType.BMP).setImage(imageByteString).build();
                    imageDataValueBuilder.setImageValue(image);
                    imageDataValueBuilder.build();
                    dataColumnBuilder.addDataValues(imageDataValueBuilder);
                }

                DataColumn dataColumn = dataColumnBuilder.build();
                dataColumnList.add(dataColumn);
            }

            final IngestionTestBase.IngestionRequestParams params =
                    new IngestionTestBase.IngestionRequestParams(
                            providerId,
                            requestId,
                            null,
                            null,
                            startSeconds,
                            startNanos,
                            samplePeriod, // 5 values per second
                            numSamples, // each DataColumn must contain 5 DataValues
                            pvNames,
                            null,
                            null,
                            null,
                            false,
                            dataColumnList
                    );

            // build request
            final IngestDataRequest ingestionRequest =
                    IngestionTestBase.buildIngestionRequest(params);

            // send request
            final List<BucketDocument> bucketDocumentList=
                    sendAndVerifyIngestionRpc_(params, ingestionRequest);

            // verify contents of mongo bucket documents - the comparison of DataColumns in request to those in
            // bucket document by sendAndVerifyIngestionRpc_() is probably sufficient, but checking that the
            // data values are as expected
            int columnIndex = 0;
            for (BucketDocument bucketDocument : bucketDocumentList) {

                final DataColumn requestColumn = dataColumnList.get(columnIndex);

                DataColumn bucketDataColumn = null;
                try {
                    bucketDataColumn = bucketDocument.getDataColumn().toDataColumn();
                } catch (DpException e) {
                    throw new RuntimeException(e);
                }
                Objects.requireNonNull(bucketDataColumn);

                int rowIndex = 0;
                for (DataValue bucketDataValue : bucketDataColumn.getDataValuesList()) {
                    assertTrue(bucketDataValue.hasImageValue());
                    final Image bucketImage = bucketDataValue.getImageValue();
                    assertEquals(imageByteString, bucketImage.getImage());

                    rowIndex = rowIndex + 1;
                }

                columnIndex = columnIndex + 1;
            }

            // perform query for single pv and verify results
            final List<String> queryPvNames = Arrays.asList("image_pv_01");
            final DataColumn requestColumn = dataColumnList.get(0);

            final QueryTestBase.QueryDataRequestParams queryDataRequestParams =
                    new QueryTestBase.QueryDataRequestParams(
                            queryPvNames,
                            startSeconds,
                            startNanos,
                            endSeconds,
                            endNanos,
                            false
                    );

            final List<DataBucket> queryBuckets = queryServiceWrapper.queryDataStream(
                    queryDataRequestParams, false, "");
            assertEquals(queryPvNames.size(), queryBuckets.size());
            final DataBucket responseBucket = queryBuckets.get(0);
            QueryTestBase.verifyDataBucket(responseBucket, requestColumn, startSeconds, startNanos, samplePeriod, numSamples);

            // write image content from query to file
            final File outputFile = new File("src/test/resources/test-image-output.bmp");
            try {
                final FileOutputStream outputStream = new FileOutputStream(outputFile);
                outputStream.write(
                        responseBucket.getDataColumn().getDataValues(0).getImageValue().getImage().toByteArray());
            } catch (IOException ex) {
                fail("error writing test-image-output.bmp");
            }
        }


        {
            // ingest structure data

            final String ARRAY_MEMBER = "array2D";
            final String STRING_MEMBER = "string";
            final String STRING_VALUE = "junk";
            final String BOOLEAN_MEMBER = "boolean";
            final boolean BOOLEAN_VALUE = false;
            final String DOUBLE_MEMBER = "double";
            final double DOUBLE_VALUE = 3.14;
            final String IMAGE_MEMBER = "image";
            final String INTEGER_MEMBER = "integer";
            final int INTEGER_VALUE = 42;
            final String TIMESTAMP_MEMBER = "timestamp";
            final String STRUCTURE_MEMBER = "structure";

            // create IngestionRequestParams
            final String requestId = "request-2";
            final List<String> pvNames = Arrays.asList(
                    "structure_pv_01", "structure_pv_02", "structure_pv_03", "structure_pv_04", "structure_pv_05");
            final int numPvs = pvNames.size();
            final int numSamples = 5;
            final long samplePeriod = 1_000_000_000L / numSamples;
            final long endSeconds = startSeconds;
            final long endNanos = samplePeriod * (numSamples - 1);

            // build list of DataColumns
            final List<DataColumn> dataColumnList = new ArrayList<>();
            Map<DataColumn, List<StructureDataValueModel>> validationMap = new HashMap<>(pvNames.size());
            for (int colIndex = 0; colIndex < numPvs; ++colIndex) {

                final DataColumn.Builder dataColumnBuilder = DataColumn.newBuilder();
                final String pvName = pvNames.get(colIndex);
                dataColumnBuilder.setName(pvName);

                final List<StructureDataValueModel> dataValueModelList = new ArrayList<>();

                for (int rowIndex = 0; rowIndex < numSamples; ++rowIndex) {

                    // create structure for row
                    final DataValue.Builder structureValueBuilder = DataValue.newBuilder();
                    final Structure.Builder structureBuilder = Structure.newBuilder();
                    
                    // add some scalars string, bool, double

                    final DataValue stringDataValue = DataValue.newBuilder().setStringValue(STRING_VALUE).build();
                    final Structure.Field stringField =
                            Structure.Field.newBuilder().setName(STRING_MEMBER).setValue(stringDataValue).build();
                    structureBuilder.addFields(stringField);

                    final DataValue booleanDataValue = DataValue.newBuilder().setBooleanValue(BOOLEAN_VALUE).build();
                    final Structure.Field booleanField =
                            Structure.Field.newBuilder().setName(BOOLEAN_MEMBER).setValue(booleanDataValue).build();
                    structureBuilder.addFields(booleanField);

                    final DataValue doubleDataValue = DataValue.newBuilder().setDoubleValue(DOUBLE_VALUE).build();
                    final Structure.Field doubleField =
                            Structure.Field.newBuilder().setName(DOUBLE_MEMBER).setValue(doubleDataValue).build();
                    structureBuilder.addFields(doubleField);

                    // add image
                    final Image image =
                            Image.newBuilder().setFileType(Image.FileType.BMP).setImage(imageByteString).build();
                    final DataValue imageDataValue = DataValue.newBuilder().setImageValue(image).build();
                    final Structure.Field imageField =
                            Structure.Field.newBuilder().setName(IMAGE_MEMBER).setValue(imageDataValue).build();
                    structureBuilder.addFields(imageField);

                    // add nested structure with integer and timestamp
                    
                    final DataValue.Builder nestedStructureValueBuilder = DataValue.newBuilder();
                    final Structure.Builder nestedStructureBuilder = Structure.newBuilder();

                    final DataValue integerDataValue = DataValue.newBuilder().setIntValue(INTEGER_VALUE).build();
                    final Structure.Field integerField =
                            Structure.Field.newBuilder().setName(INTEGER_MEMBER).setValue(integerDataValue).build();
                    nestedStructureBuilder.addFields(integerField);

                    final Timestamp timestamp = Timestamp.newBuilder()
                            .setEpochSeconds(startSeconds)
                            .setNanoseconds(startNanos)
                            .build();
                    final DataValue timestampDataValue = DataValue.newBuilder().setTimestampValue(timestamp).build();
                    final Structure.Field timestampField =
                            Structure.Field.newBuilder().setName(TIMESTAMP_MEMBER).setValue(timestampDataValue).build();
                    nestedStructureBuilder.addFields(timestampField);

                    nestedStructureBuilder.build();
                    nestedStructureValueBuilder.setStructureValue(nestedStructureBuilder);
                    nestedStructureValueBuilder.build();

                    final Structure.Field nestedStructureField = Structure.Field.newBuilder()
                            .setName(STRUCTURE_MEMBER)
                            .setValue(nestedStructureValueBuilder)
                            .build();
                    structureBuilder.addFields(nestedStructureField);

                    // get array from array ingestion DataColumn list and add to structure
                    final DataColumn arrayDataColumn = arrayDataColumnList.get(colIndex);
                    DataValue arrayDataValue = arrayDataColumn.getDataValues(rowIndex);
                    List<List<String>> array2D = arrayValidationMap.get(arrayDataColumn).get(rowIndex).array2D;
                    assertTrue(arrayDataValue.hasArrayValue());
                    Structure.Field arrayField =
                            Structure.Field.newBuilder().setName(ARRAY_MEMBER).setValue(arrayDataValue).build();
                    structureBuilder.addFields(arrayField);

                    structureBuilder.build();
                    structureValueBuilder.setStructureValue(structureBuilder);
                    dataColumnBuilder.addDataValues(structureValueBuilder.build());

                    // create value model for column value (for later validation)
                    final StructureDataValueModel structureDataValueModel =
                            new StructureDataValueModel(
                                    STRING_VALUE,
                                    BOOLEAN_VALUE,
                                    DOUBLE_VALUE,
                                    INTEGER_VALUE,
                                    startSeconds,
                                    startNanos,
                                    array2D);
                    dataValueModelList.add(structureDataValueModel);
                }

                DataColumn dataColumn = dataColumnBuilder.build();
                dataColumnList.add(dataColumn);
                validationMap.put(dataColumn, dataValueModelList);
            }

            final IngestionTestBase.IngestionRequestParams params =
                    new IngestionTestBase.IngestionRequestParams(
                            providerId,
                            requestId,
                            null,
                            null,
                            startSeconds,
                            startNanos,
                            samplePeriod,
                            numSamples,
                            pvNames,
                            null,
                            null,
                            null,
                            false,
                            dataColumnList
                    );

            // build request
            final IngestDataRequest ingestionRequest =
                    IngestionTestBase.buildIngestionRequest(params);

            // send request
            final List<BucketDocument> bucketDocumentList =
                    sendAndVerifyIngestionRpc_(params, ingestionRequest);

            // verify contents of mongo bucket documents - the comparison of DataColumns in request to those in
            // bucket document by sendAndVerifyIngestionRpc_() is probably sufficient, but checking that the
            // data values are as expected
            int columnIndex = 0;
            for (BucketDocument bucketDocument : bucketDocumentList) {

                final DataColumn requestColumn = dataColumnList.get(columnIndex);

                DataColumn bucketDataColumn = null;
                try {
                    bucketDataColumn = bucketDocument.getDataColumn().toDataColumn();
                } catch (DpException e) {
                    throw new RuntimeException(e);
                }
                Objects.requireNonNull(bucketDataColumn);

                final List<StructureDataValueModel> requestValueModelList = validationMap.get(requestColumn);
                assertEquals(requestValueModelList.size(), bucketDataColumn.getDataValuesCount());

                int rowIndex = 0;
                for (StructureDataValueModel structureDataValueModel : requestValueModelList) {

                    // confirm DataValue is structure
                    final DataValue bucketValue = bucketDataColumn.getDataValues(rowIndex);
                    assertTrue(bucketValue.hasStructureValue());
                    final Structure bucketStructure = bucketValue.getStructureValue();

                    for (Structure.Field bucketStructureField : bucketStructure.getFieldsList()) {
                        // iterate structure fields, dispatching by field name
                        final String bucketStructureFieldName = bucketStructureField.getName();
                        final DataValue bucketStructureFieldDataValue = bucketStructureField.getValue();

                        switch (bucketStructureFieldName) {

                            case STRING_MEMBER:
                                assertTrue(bucketStructureFieldDataValue.hasStringValue());
                                assertEquals(
                                        structureDataValueModel.stringField,
                                        bucketStructureFieldDataValue.getStringValue());
                                break;

                            case BOOLEAN_MEMBER:
                                assertTrue(bucketStructureFieldDataValue.hasBooleanValue());
                                assertEquals(
                                        structureDataValueModel.booleanField,
                                        bucketStructureFieldDataValue.getBooleanValue());
                                break;

                            case DOUBLE_MEMBER:
                                assertTrue(bucketStructureFieldDataValue.hasDoubleValue());
                                assertEquals(
                                        structureDataValueModel.doubleField,
                                        bucketStructureFieldDataValue.getDoubleValue(),
                                        0.0);
                                break;

                            case IMAGE_MEMBER:
                                assertTrue(bucketStructureFieldDataValue.hasImageValue());
                                final Image bucketImage = bucketStructureFieldDataValue.getImageValue();
                                assertEquals(Image.FileType.BMP, bucketImage.getFileType());
                                assertEquals(imageByteString, bucketImage.getImage());
                                break;

                            case STRUCTURE_MEMBER:
                                assertTrue(bucketStructureFieldDataValue.hasStructureValue());
                                final Structure nestedBucketStructure =
                                        bucketStructureFieldDataValue.getStructureValue();
                                for (Structure.Field nestedStructureField : nestedBucketStructure.getFieldsList()) {

                                    final String nestedStructureFieldName = nestedStructureField.getName();
                                    final DataValue nestedStructureFieldDataValue = nestedStructureField.getValue();

                                    if (nestedStructureFieldName.equals(INTEGER_MEMBER)) {
                                        assertEquals(
                                                (int) structureDataValueModel.integerField,
                                                nestedStructureFieldDataValue.getIntValue());

                                    } else if (nestedStructureFieldName.equals(TIMESTAMP_MEMBER)) {
                                        assertTrue(nestedStructureFieldDataValue.hasTimestampValue());
                                        final Timestamp timestamp =
                                                nestedStructureFieldDataValue.getTimestampValue();
                                        assertEquals(
                                                (long) structureDataValueModel.timestampSeconds,
                                                timestamp.getEpochSeconds());
                                        assertEquals(
                                                (long) structureDataValueModel.timestampNanos,
                                                timestamp.getNanoseconds());
                                    } else {
                                        fail("unexpected nested structure field name: " + nestedStructureFieldName);
                                    }
                                }
                                break;

                            case ARRAY_MEMBER:
                                final List<List<String>> array2D = structureDataValueModel.array2D;
                                assertTrue(bucketStructureFieldDataValue.hasArrayValue());
                                final Array bucketArray = bucketStructureFieldDataValue.getArrayValue();
                                verify2DArray(array2D, bucketArray);
                                break;

                            default:
                                fail("unexpected structure field name: " + bucketStructureFieldName);
                                break;
                        }
                    }

                    rowIndex = rowIndex + 1;
                }

                columnIndex = columnIndex + 1;
            }

            // perform query for single pv and verify results
            final List<String> queryPvNames = Arrays.asList("structure_pv_01");
            final DataColumn requestColumn = dataColumnList.get(0);

            final QueryTestBase.QueryDataRequestParams queryDataRequestParams =
                    new QueryTestBase.QueryDataRequestParams(
                            queryPvNames,
                            startSeconds,
                            startNanos,
                            endSeconds,
                            endNanos,
                            false
                    );

            final List<DataBucket> queryBuckets = queryServiceWrapper.queryDataStream(
                    queryDataRequestParams, false, "");
            assertEquals(queryPvNames.size(), queryBuckets.size());
            final DataBucket responseBucket = queryBuckets.get(0);
            QueryTestBase.verifyDataBucket(responseBucket, requestColumn, startSeconds, startNanos, samplePeriod, numSamples);
        }
    }

    private void verify2DArray(List<List<String>> array2D, Array bucketArray) {
        assertEquals(array2D.size(), bucketArray.getDataValuesCount());
        final List<DataValue> bucketArrayValues = bucketArray.getDataValuesList();

        int arrayIndex = 0;
        for (List<String> array1D : array2D) {
            final DataValue bucketArray1DValue = bucketArrayValues.get(arrayIndex);
            assertTrue(bucketArray1DValue.hasArrayValue());
            final Array bucketArray1D = bucketArray1DValue.getArrayValue();
            assertEquals(array1D.size(), bucketArray1D.getDataValuesCount());

            int elementIndex = 0;
            for (String array1DElement : array1D) {
                final DataValue bucketArray1DElementValue = bucketArray1D.getDataValues(elementIndex);
                assertTrue(bucketArray1DElementValue.hasStringValue());
                final String bucketArray1DElement = bucketArray1DElementValue.getStringValue();
                assertEquals(array1DElement, bucketArray1DElement);

                elementIndex = elementIndex + 1;
            }

            arrayIndex = arrayIndex + 1;
        }
    }

}
