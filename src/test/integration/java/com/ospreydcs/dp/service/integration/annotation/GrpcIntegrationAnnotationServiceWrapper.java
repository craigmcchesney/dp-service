package com.ospreydcs.dp.service.integration.annotation;

import ch.systemsx.cisd.hdf5.HDF5Factory;
import ch.systemsx.cisd.hdf5.IHDF5Reader;
import com.mongodb.client.MongoCursor;
import com.ospreydcs.dp.grpc.v1.annotation.*;
import com.ospreydcs.dp.grpc.v1.common.CalculationsSpec;
import com.ospreydcs.dp.grpc.v1.common.DataColumn;
import com.ospreydcs.dp.grpc.v1.common.DataTimestamps;
import com.ospreydcs.dp.service.annotation.AnnotationTestBase;
import com.ospreydcs.dp.service.annotation.handler.interfaces.AnnotationHandlerInterface;
import com.ospreydcs.dp.service.annotation.handler.mongo.MongoAnnotationHandler;
import com.ospreydcs.dp.service.annotation.service.AnnotationServiceImpl;
import com.ospreydcs.dp.service.common.bson.annotation.AnnotationDocument;
import com.ospreydcs.dp.service.common.bson.bucket.BucketDocument;
import com.ospreydcs.dp.service.common.bson.configuration.ConfigurationActivationDocument;
import com.ospreydcs.dp.service.common.bson.configuration.ConfigurationDocument;
import com.ospreydcs.dp.grpc.v1.common.Configuration;
import com.ospreydcs.dp.grpc.v1.common.ConfigurationActivation;
import com.ospreydcs.dp.service.common.bson.pvmetadata.PvMetadataDocument;
import com.ospreydcs.dp.grpc.v1.common.PvMetadata;
import com.ospreydcs.dp.service.common.bson.calculations.CalculationsDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataBlockDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import com.ospreydcs.dp.service.common.exception.DpException;
import com.ospreydcs.dp.service.common.model.TimestampDataMap;
import com.ospreydcs.dp.service.common.model.TimestampMap;
import com.ospreydcs.dp.service.common.mongo.MongoTestClient;
import com.ospreydcs.dp.service.common.protobuf.AttributesUtility;
import com.ospreydcs.dp.service.common.protobuf.DataColumnUtility;
import com.ospreydcs.dp.service.common.utility.TabularDataUtility;
import com.ospreydcs.dp.service.integration.GrpcIntegrationServiceWrapperBase;
import com.ospreydcs.dp.service.integration.ingest.GrpcIntegrationIngestionServiceWrapper;
import de.siegmar.fastcsv.reader.CsvReader;
import de.siegmar.fastcsv.reader.CsvRecord;
import io.grpc.testing.GrpcCleanupRule;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.ClassRule;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.mock;

/**
 * This class provides utilities for calling various Annotation Service API methods in integration tests that use the
 * in-process gRPC communication framework.  For each API method, it provides utility methods for sending the API
 * method request and verifying the result.
 */
public class GrpcIntegrationAnnotationServiceWrapper extends GrpcIntegrationServiceWrapperBase<AnnotationServiceImpl> {

    // static variables
    private static final Logger logger = LogManager.getLogger();
    @ClassRule public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    // instance variables (common ones inherited from base class)
    protected Map<String, AnnotationTestBase.SaveDataSetParams> saveDataSetIdParamsMap;
    protected Map<AnnotationTestBase.SaveDataSetParams, String> saveDataSetParamsIdMap;
    protected Map<AnnotationTestBase.SaveAnnotationRequestParams, String> saveAnnotationParamsIdMap;


    public void init(MongoTestClient mongoClient) {
        // init data structures
        saveDataSetIdParamsMap = new TreeMap<>();
        saveDataSetParamsIdMap = new HashMap<>();
        saveAnnotationParamsIdMap = new HashMap<>();

        super.init(mongoClient);
    }

    @Override
    protected boolean initService() {
        AnnotationHandlerInterface annotationHandler = MongoAnnotationHandler.newMongoSyncAnnotationHandler();
        service = new AnnotationServiceImpl();
        return service.init(annotationHandler);
    }

    @Override
    protected void finiService() {
        service.fini();
    }

    @Override
    protected AnnotationServiceImpl createServiceMock(AnnotationServiceImpl service) {
        return mock(AnnotationServiceImpl.class, delegatesTo(service));
    }

    @Override
    protected GrpcCleanupRule getGrpcCleanupRule() {
        return grpcCleanup;
    }

    @Override
    protected String getServiceName() {
        return "AnnotationServiceImpl";
    }

    @Override
    public void fini() {
        super.fini();
        saveAnnotationParamsIdMap = null;
        saveDataSetParamsIdMap = null;
        saveDataSetIdParamsMap = null;
    }
    
    protected String sendSaveDataSet(
                SaveDataSetRequest request, boolean expectReject, String expectedRejectMessage
    ) {
        final DpAnnotationServiceGrpc.DpAnnotationServiceStub asyncStub =
                DpAnnotationServiceGrpc.newStub(channel);

        final AnnotationTestBase.SaveDataSetResponseObserver responseObserver =
                new AnnotationTestBase.SaveDataSetResponseObserver();

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            asyncStub.saveDataSet(request, responseObserver);
        }).start();

        responseObserver.await();

        if (expectReject) {
            assertTrue(responseObserver.isError());
            assertTrue(responseObserver.getErrorMessage().contains(expectedRejectMessage));
        } else {
            assertFalse(responseObserver.getErrorMessage(), responseObserver.isError());
        }

        return responseObserver.getDataSetId();
    }

    public String sendAndVerifySaveDataSet(
            AnnotationTestBase.SaveDataSetParams params,
            boolean isUpdate,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final SaveDataSetRequest request =
                AnnotationTestBase.buildSaveDataSetRequest(params);

        final String dataSetId = sendSaveDataSet(request, expectReject, expectedRejectMessage);

        if (expectReject) {
            assertNull(dataSetId);
            return "";
        }

        // validate response and database contents
        assertNotNull(dataSetId);
        assertFalse(dataSetId.isBlank());
        if (isUpdate) {
            assertEquals(params.dataSet().id(), dataSetId);
        }
        final DataSetDocument dataSetDocument = mongoClient.findDataSet(dataSetId);
        assertNotNull(dataSetDocument);
        assertNotNull(dataSetDocument.getCreatedAt());
        final List<String> requestDiffs = dataSetDocument.diffRequest(request);
        assertNotNull(requestDiffs);
        assertTrue(requestDiffs.toString(), requestDiffs.isEmpty());

        saveDataSetIdParamsMap.put(dataSetId, params);
        saveDataSetParamsIdMap.put(params, dataSetId);

        return dataSetId;
    }

    protected List<DataSet> sendQueryDataSets(
            QueryDataSetsRequest request,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final DpAnnotationServiceGrpc.DpAnnotationServiceStub asyncStub =
                DpAnnotationServiceGrpc.newStub(channel);

        final AnnotationTestBase.QueryDataSetsResponseObserver responseObserver =
                new AnnotationTestBase.QueryDataSetsResponseObserver();

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            asyncStub.queryDataSets(request, responseObserver);
        }).start();

        responseObserver.await();

        if (expectReject) {
            assertTrue(responseObserver.isError());
            assertTrue(responseObserver.getErrorMessage().contains(expectedRejectMessage));
        } else {
            assertFalse(responseObserver.getErrorMessage(), responseObserver.isError());
        }

        return responseObserver.getDataSetsList();
    }

    protected List<DataSet> sendAndVerifyQueryDataSets(
            AnnotationTestBase.QueryDataSetsParams queryParams,
            boolean expectReject,
            String expectedRejectMessage,
            List<AnnotationTestBase.SaveDataSetParams> expectedQueryResult
    ) {
        final QueryDataSetsRequest request =
                AnnotationTestBase.buildQueryDataSetsRequest(queryParams);

        final List<DataSet> resultDataSets = sendQueryDataSets(request, expectReject, expectedRejectMessage);

        if (expectReject || expectedQueryResult.isEmpty()) {
            assertTrue(resultDataSets.isEmpty());
            return new ArrayList<>();
        }

        // validate response

        assertEquals(expectedQueryResult.size(), resultDataSets.size());

        // find each expected result in actual result list and match field values against request
        for (AnnotationTestBase.SaveDataSetParams requestParams : expectedQueryResult) {
            boolean found = false;
            DataSet foundDataSet = null;
            for (DataSet resultDataSet : resultDataSets) {
                if (requestParams.dataSet().name().equals(resultDataSet.getName())) {
                    found = true;
                    foundDataSet = resultDataSet;
                    break;
                }
            }
            assertTrue(found);
            assertNotNull(foundDataSet);
            final String expectedDataSetId = this.saveDataSetParamsIdMap.get(requestParams);

            // check required dataset fields match
            assertTrue(expectedDataSetId.equals(foundDataSet.getId()));
            assertTrue(requestParams.dataSet().description().equals(foundDataSet.getDescription()));
            assertTrue(requestParams.dataSet().ownerId().equals(foundDataSet.getOwnerId()));

            // check that result corresponds to query criteria
            if (queryParams.idCriterion != null) {
                assertEquals(queryParams.idCriterion, foundDataSet.getId());
            }
            if (queryParams.ownerCriterion != null) {
                assertEquals(queryParams.ownerCriterion, foundDataSet.getOwnerId());
            }
            if (queryParams.textCriterion != null) {
                assertTrue(
                        foundDataSet.getName().contains(queryParams.textCriterion)
                                || foundDataSet.getDescription().contains(queryParams.textCriterion));
            }
            if (queryParams.pvNameCriterion != null) {
                boolean foundPvName = false;
                for (DataBlock foundDataSetDataBlock : foundDataSet.getDataBlocksList()) {
                    if (foundDataSetDataBlock.getPvNamesList().contains(queryParams.pvNameCriterion)) {
                        foundPvName = true;
                        break;
                    }
                }
                assertTrue(foundPvName);
            }

            // compare data blocks from result with request
            final AnnotationTestBase.AnnotationDataSet requestDataSet = requestParams.dataSet();
            assertEquals(requestDataSet.dataBlocks().size(), foundDataSet.getDataBlocksCount());
            for (AnnotationTestBase.AnnotationDataBlock requestBlock : requestDataSet.dataBlocks()) {
                boolean responseBlockFound = false;
                for (DataBlock responseBlock : foundDataSet.getDataBlocksList()) {
                    if (
                            (Objects.equals(requestBlock.beginSeconds(), responseBlock.getBeginTime().getEpochSeconds()))
                                    && (Objects.equals(requestBlock.beginNanos(), responseBlock.getBeginTime().getNanoseconds()))
                                    && (Objects.equals(requestBlock.endSeconds(), responseBlock.getEndTime().getEpochSeconds()))
                                    && (Objects.equals(requestBlock.endNanos(), responseBlock.getEndTime().getNanoseconds()))
                                    && (Objects.equals(requestBlock.pvNames(), responseBlock.getPvNamesList()))
                    ) {
                        responseBlockFound = true;
                        break;
                    }
                }
                assertTrue(responseBlockFound);
            }
        }

        return resultDataSets;
    }

    protected String sendSaveAnnotation(
            SaveAnnotationRequest request, boolean expectReject, String expectedRejectMessage
    ) {
        final DpAnnotationServiceGrpc.DpAnnotationServiceStub asyncStub =
                DpAnnotationServiceGrpc.newStub(channel);

        final AnnotationTestBase.SaveAnnotationResponseObserver responseObserver =
                new AnnotationTestBase.SaveAnnotationResponseObserver();

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            asyncStub.saveAnnotation(request, responseObserver);
        }).start();

        responseObserver.await();

        if (expectReject) {
            assertTrue(responseObserver.isError());
            assertFalse(expectedRejectMessage.isBlank());
            assertTrue(responseObserver.getErrorMessage().contains(expectedRejectMessage));
        } else {
            assertFalse(responseObserver.getErrorMessage(), responseObserver.isError());
        }

        return responseObserver.getAnnotationId();
    }

    protected String sendAndVerifySaveAnnotation(
            AnnotationTestBase.SaveAnnotationRequestParams params,
            boolean isUpdate,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final SaveAnnotationRequest request =
                AnnotationTestBase.buildSaveAnnotationRequest(params);

        final String annotationId = sendSaveAnnotation(request, expectReject, expectedRejectMessage);

        if (expectReject) {
            assertNull(annotationId);
            return null;
        }

        // validate response and database contents
        assertNotNull(annotationId);
        assertFalse(annotationId.isBlank());
        if (isUpdate) {
            assertEquals(params.id, annotationId);
        }
        final AnnotationDocument annotationDocument = mongoClient.findAnnotation(annotationId);
        assertNotNull(annotationDocument);
        assertNotNull(annotationDocument.getCreatedAt());
        final List<String> requestDiffs = annotationDocument.diffSaveAnnotationRequest(request);
        assertNotNull(requestDiffs);
        assertTrue(requestDiffs.toString(), requestDiffs.isEmpty());

        // validate calculations if specified
        if (params.calculations != null) {
            assertNotNull(annotationDocument.getCalculationsId());
            final CalculationsDocument calculationsDocument =
                    mongoClient.findCalculations(annotationDocument.getCalculationsId());
            assertNotNull(calculationsDocument);
            assertNotNull(calculationsDocument.getCreatedAt());
            final List<String> calculationsDiffs = calculationsDocument.diffCalculations(params.calculations);
            assertNotNull(calculationsDiffs);
            assertTrue(calculationsDiffs.toString(), calculationsDiffs.isEmpty());

        } else {
            assertNull(annotationDocument.getCalculationsId());
        }

        // save annotationId to map for use in validating queryAnnotations() result
        saveAnnotationParamsIdMap.put(params, annotationId);

        return annotationId;
    }

    protected List<QueryAnnotationsResponse.AnnotationsResult.Annotation> sendQueryAnnotations(
            QueryAnnotationsRequest request,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final DpAnnotationServiceGrpc.DpAnnotationServiceStub asyncStub =
                DpAnnotationServiceGrpc.newStub(channel);

        final AnnotationTestBase.QueryAnnotationsResponseObserver responseObserver =
                new AnnotationTestBase.QueryAnnotationsResponseObserver();

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            asyncStub.queryAnnotations(request, responseObserver);
        }).start();

        responseObserver.await();

        if (expectReject) {
            assertTrue(responseObserver.isError());
            assertTrue(responseObserver.getErrorMessage().contains(expectedRejectMessage));
        } else {
            assertFalse(responseObserver.getErrorMessage(), responseObserver.isError());
        }

        return responseObserver.getAnnotationsList();
    }

    protected List<QueryAnnotationsResponse.AnnotationsResult.Annotation> sendAndVerifyQueryAnnotations(
            AnnotationTestBase.QueryAnnotationsParams queryParams,
            boolean expectReject,
            String expectedRejectMessage,
            List<AnnotationTestBase.SaveAnnotationRequestParams> expectedQueryResult
    ) {
        final QueryAnnotationsRequest request =
                AnnotationTestBase.buildQueryAnnotationsRequest(queryParams);

        final List<QueryAnnotationsResponse.AnnotationsResult.Annotation> resultAnnotations =
                sendQueryAnnotations(request, expectReject, expectedRejectMessage);

        if (expectReject || expectedQueryResult.isEmpty()) {
            assertTrue(resultAnnotations.isEmpty());
            return new ArrayList<>();
        }

        // validate response
        assertEquals(expectedQueryResult.size(), resultAnnotations.size());
        // find each expected result in actual result list and match field values against request
        for (AnnotationTestBase.SaveAnnotationRequestParams requestParams : expectedQueryResult) {
            boolean found = false;
            QueryAnnotationsResponse.AnnotationsResult.Annotation foundAnnotation = null;
            for (QueryAnnotationsResponse.AnnotationsResult.Annotation resultAnnotation : resultAnnotations) {
                if (
                        (requestParams.ownerId.equals(resultAnnotation.getOwnerId())) &&
                        (Objects.equals(requestParams.dataSetIds, resultAnnotation.getDataSetIdsList())) &&
                        (requestParams.name.equals(resultAnnotation.getName()))
                ) {
                    found = true;
                    foundAnnotation = resultAnnotation;
                    break;
                }
            }
            assertTrue(found);
            assertNotNull(foundAnnotation);
            final String expectedAnnotationId = saveAnnotationParamsIdMap.get(requestParams);
            assertTrue(expectedAnnotationId.equals(foundAnnotation.getId()));

            // compare required fields from request against found annotation
            assertTrue(requestParams.ownerId.equals(foundAnnotation.getOwnerId()));
            assertTrue(requestParams.name.equals(foundAnnotation.getName()));
            // use Objects.equals to compare list elements
            assertTrue(Objects.equals(requestParams.dataSetIds, foundAnnotation.getDataSetIdsList()));

            // confirm that query results correspond to specify query criteria
            
            // check IdCriterion
            if (queryParams.idCriterion != null) {
                assertTrue(foundAnnotation.getId().equals(queryParams.idCriterion));
            }
            
            // check OwnerCriterion
            if (queryParams.ownerCriterion != null) {
                assertTrue(foundAnnotation.getOwnerId().equals(queryParams.ownerCriterion));
            }

            // check DataSetCriterion
            if (queryParams.datasetsCriterion != null) {
                assertTrue(foundAnnotation.getDataSetIdsList().contains(queryParams.datasetsCriterion));
            }

            // check AssociatedAnnotationCriterion
            if (queryParams.annotationsCriterion != null) {
                assertTrue(foundAnnotation.getAnnotationIdsList().contains(queryParams.annotationsCriterion));
            }

            // check TextCriterion
            if (queryParams.textCriterion != null) {
                assertTrue(
                        foundAnnotation.getComment().contains(queryParams.textCriterion)
                                || foundAnnotation.getName().contains(queryParams.textCriterion));
            }

            // check TagsCriterion
            if (queryParams.tagsCriterion != null) {
                assertTrue(foundAnnotation.getTagsList().contains(queryParams.tagsCriterion));
            }

            // check AttributesCriterion
            if (queryParams.attributesCriterionKey != null) {
                assertNotNull(queryParams.attributesCriterionValue);
                final Map<String, String> resultAttributeMap =
                        AttributesUtility.attributeMapFromList(foundAnnotation.getAttributesList());
                assertEquals(
                        resultAttributeMap.get(queryParams.attributesCriterionKey), queryParams.attributesCriterionValue);
            }

           // compare dataset content from result with dataset in database
            for (DataSet responseDataSet : foundAnnotation.getDataSetsList()) {
                final DataSetDocument dbDataSetDocument = mongoClient.findDataSet(responseDataSet.getId());
                final DataSet dbDataSet = dbDataSetDocument.toDataSet();
                assertEquals(dbDataSet.getDataBlocksList().size(), responseDataSet.getDataBlocksCount());
                assertTrue(dbDataSet.getName().equals(responseDataSet.getName()));
                assertTrue(dbDataSet.getOwnerId().equals(responseDataSet.getOwnerId()));
                assertTrue(dbDataSet.getDescription().equals(responseDataSet.getDescription()));
                for (DataBlock dbDataBlock : dbDataSet.getDataBlocksList()) {
                    boolean responseBlockFound = false;
                    for (DataBlock responseBlock : responseDataSet.getDataBlocksList()) {
                        if (
                                (Objects.equals(dbDataBlock.getBeginTime().getEpochSeconds(), responseBlock.getBeginTime().getEpochSeconds()))
                                        && (Objects.equals(dbDataBlock.getBeginTime().getNanoseconds(), responseBlock.getBeginTime().getNanoseconds()))
                                        && (Objects.equals(dbDataBlock.getEndTime().getEpochSeconds(), responseBlock.getEndTime().getEpochSeconds()))
                                        && (Objects.equals(dbDataBlock.getEndTime().getNanoseconds(), responseBlock.getEndTime().getNanoseconds()))
                                        && (Objects.equals(dbDataBlock.getPvNamesList(), responseBlock.getPvNamesList()))
                        ) {
                            responseBlockFound = true;
                            break;
                        }
                    }
                    assertTrue(responseBlockFound);
                }
            }

            // compare calculations content from result with calculations document in database
            if (requestParams.calculations != null) {
                assertTrue(foundAnnotation.hasCalculations());
                final Calculations resultCalculations = foundAnnotation.getCalculations();
                final CalculationsDocument dbCalculationsDocument =
                        mongoClient.findCalculations(resultCalculations.getId());
                try {
                    final Calculations dbCalculations = dbCalculationsDocument.toCalculations();
                    assertEquals(dbCalculations, resultCalculations);
                } catch (DpException e) {
                    fail("exception in CalculationsDocument.toCalculations(): " + e.getMessage());
                }
            }
        }

        return resultAnnotations;
    }

    public ExportDataResponse.ExportDataResult sendExportData(
            ExportDataRequest request,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final DpAnnotationServiceGrpc.DpAnnotationServiceStub asyncStub =
                DpAnnotationServiceGrpc.newStub(channel);

        final AnnotationTestBase.ExportDataResponseObserver responseObserver =
                new AnnotationTestBase.ExportDataResponseObserver();

        // start performance measurment timer
        final Instant t0 = Instant.now();

        // send request in separate thread to better simulate out of process grpc,
        // otherwise service handles request in this thread
        new Thread(() -> {
            asyncStub.exportData(request, responseObserver);
        }).start();

        responseObserver.await();

        // stop performance measurement timer
        final Instant t1 = Instant.now();
        final long dtMillis = t0.until(t1, ChronoUnit.MILLIS);
        final double secondsElapsed = dtMillis / 1_000.0;

        System.out.println("export format " + request.getOutputFormat().name() + " elapsed seconds: " + secondsElapsed);

        if (expectReject) {
            assertTrue(responseObserver.isError());
            assertTrue(responseObserver.getErrorMessage().contains(expectedRejectMessage));
        } else {
            assertFalse(responseObserver.getErrorMessage(), responseObserver.isError());
        }

        return responseObserver.getResult();
    }

    private TimestampDataMap getTimestampDataMapForDataset(DataSetDocument dataset) throws DpException {

        final TimestampDataMap expectedDataMap = new TimestampDataMap();
        for (DataBlockDocument dataBlock : dataset.getDataBlocks()) {
            final MongoCursor<BucketDocument> cursor = mongoClient.findDataBlockBuckets(dataBlock);
            final long beginSeconds = dataBlock.getBeginTime().getSeconds();
            final long beginNanos = dataBlock.getBeginTime().getNanos();
            final long endSeconds = dataBlock.getEndTime().getSeconds();
            final long endNanos = dataBlock.getEndTime().getNanos();
            final TabularDataUtility.TimestampDataMapSizeStats sizeStats =
                    TabularDataUtility.addBucketsToTable(
                            expectedDataMap,
                            cursor,
                            0,
                            null,
                            beginSeconds,
                            beginNanos,
                            endSeconds,
                            endNanos
                    );
        }

        return expectedDataMap;
    }

    public ExportDataResponse.ExportDataResult sendAndVerifyExportData(
            String dataSetId,
            AnnotationTestBase.SaveDataSetParams saveDataSetParams,
            CalculationsSpec calculationsSpec,
            Calculations calculationsRequestParams,
            ExportDataRequest.ExportOutputFormat outputFormat,
            int expectedNumBuckets,
            int expectedNumRows,
            Map<String, GrpcIntegrationIngestionServiceWrapper.IngestionStreamInfo> pvValidationMap,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final ExportDataRequest request =
                AnnotationTestBase.buildExportDataRequest(
                        dataSetId,
                        calculationsSpec,
                        outputFormat);

        final ExportDataResponse.ExportDataResult exportResult =
                sendExportData(request, expectReject, expectedRejectMessage);

        if (expectReject) {
            assertTrue(exportResult == null);
            return null;
        }

        // validate
        assertNotNull(exportResult);
        assertNotEquals("", exportResult.getFilePath());

//         assertNotEquals("", exportResult.getFileUrl());
//        // open file url to reproduce issue Mitch encountered from web app
//        String command = "curl " + exportResult.getFileUrl();
//        try {
//            Process process = Runtime.getRuntime().exec(command);
//            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
//            String line;
//            while((line = reader.readLine()) != null) {
//                System.out.println(line);
//            }
//            reader.close();
//        } catch (IOException e) {
//            fail("exception calling curl for " + exportResult.getFilePath() + ": " + e.getMessage());
//        }

        // check that file is available (this is the same as the check done by WatchService (e.g., WatchService
        // won't solve our problem with corrupt excel file if this test succeeds)
        final Path target = Path.of(exportResult.getFilePath());
        try {
            final BasicFileAttributes attributes = Files.readAttributes(target, BasicFileAttributes.class);
            System.out.println("got file attributes for: " + target);
        } catch (IOException ex) {
            fail("IOException getting file attributes for: " + target);
        }

//        // copy file from url to reproduce issue Mitch enountered from web app (opening URL from Javascript)
//        final int filenameIndex = exportResult.getFilePath().lastIndexOf('/') + 1;
//        final String filename = exportResult.getFilePath().substring(filenameIndex);
//        try {
//            FileUtils.copyURLToFile(new URL(exportResult.getFileUrl()), new File("/tmp/" + filename));
//        } catch (IOException e) {
//            fail("IOException copying file from url " + exportResult.getFileUrl() + ": " + e.getMessage());
//        }
//

        // create list of expected PV column names
        Set<String> expectedPvColumnNames = new HashSet<>();
        DataSetDocument datasetDocument = null;
        List<BucketDocument> datasetBuckets = null;
        if (dataSetId != null) {

            // retrieve dataset for id
            datasetDocument = mongoClient.findDataSet(dataSetId);
            assertNotNull(datasetDocument);

            // retrieve BucketDocuments for specified dataset
            datasetBuckets = mongoClient.findDataSetBuckets(datasetDocument);
            assertEquals(expectedNumBuckets, datasetBuckets.size());

            // generate collection of unique pv names for dataset from creation request params
            // these are the columns expected in the export output file
            for (AnnotationTestBase.AnnotationDataBlock requestDataBlock : saveDataSetParams.dataSet().dataBlocks()) {
                expectedPvColumnNames.addAll(requestDataBlock.pvNames());
            }
        }

        // create list of expected calculations column names
        CalculationsDocument calculationsDocument = null;
        List<String> expectedCalculationsColumnNames = new ArrayList<>();
        Map<String, CalculationsSpec.ColumnNameList> frameColumnNamesMap = null;
        if (calculationsSpec != null) {

            // retrieve calculations for id
            calculationsDocument = mongoClient.findCalculations(calculationsSpec.getCalculationsId());
            assertNotNull(calculationsDocument);

            // add expected column names for calculations
            if (calculationsSpec.getDataFrameColumnsMap().isEmpty()) {

                // add all columns for calculations
                for (Calculations.CalculationsDataFrame requestCalculationsFrame :
                        calculationsRequestParams.getCalculationDataFramesList()) {
                    final String requestCalculationsFrameName = requestCalculationsFrame.getName();
                    for (DataColumn requestCalculationsFrameColumn : requestCalculationsFrame.getDataColumnsList()) {
                        expectedCalculationsColumnNames.add(requestCalculationsFrameColumn.getName());
                    }
                }

            } else {

                // add only columns from map in calculationsSpec
                frameColumnNamesMap = calculationsSpec.getDataFrameColumnsMap();
                for (var frameColumnMapEntry : frameColumnNamesMap.entrySet()) {
                    final String frameName = frameColumnMapEntry.getKey();
                    final List<String> frameColumns = frameColumnMapEntry.getValue().getColumnNamesList();
                    for (String frameColumnName : frameColumns) {
                        expectedCalculationsColumnNames.add(frameColumnName);
                    }
                }
            }
        }

        // create map of calculations column data values for use in verification
        Map<String, TimestampMap<Double>> calculationsValidationMap = new HashMap<>();
        if (calculationsRequestParams != null) {
            for (Calculations.CalculationsDataFrame requestCalculationsFrame :
                    calculationsRequestParams.getCalculationDataFramesList()) {
                final DataTimestamps requestCalculationsFrameDataTimestamps =
                        requestCalculationsFrame.getDataTimestamps();
                for (DataColumn requestCalculationsFrameColumn : requestCalculationsFrame.getDataColumnsList()) {
                    final String requestCalculationsFrameColumnName = requestCalculationsFrameColumn.getName();
                    if (expectedCalculationsColumnNames.contains(requestCalculationsFrameColumnName)) {
                        calculationsValidationMap.put(
                                requestCalculationsFrameColumnName,
                                DataColumnUtility.toTimestampMapDouble(
                                        requestCalculationsFrameColumn, requestCalculationsFrameDataTimestamps));
                    }
                }
            }
        }

        // verify file content for specified output format
        switch (outputFormat) {

            case EXPORT_FORMAT_HDF5 -> {

                final IHDF5Reader reader = HDF5Factory.openForReading(exportResult.getFilePath());

                if (datasetDocument != null) {
                    AnnotationTestBase.verifyDatasetHdf5Content(reader, datasetDocument);
                }

                if (datasetBuckets != null) {
                    for (BucketDocument bucket : datasetBuckets) {
                        AnnotationTestBase.verifyBucketDocumentHdf5Content(reader, bucket);
                    }
                }

                if (calculationsDocument != null) {
                    AnnotationTestBase.verifyCalculationsDocumentHdf5Content(
                            reader, calculationsDocument, frameColumnNamesMap);
                }

                reader.close();
            }

            case EXPORT_FORMAT_CSV -> {
                // verify file content against data map
                verifyExportTabularCsv(
                        exportResult,
                        expectedPvColumnNames,
                        pvValidationMap,
                        expectedCalculationsColumnNames,
                        calculationsValidationMap,
                        expectedNumRows);
            }

            case EXPORT_FORMAT_XLSX -> {
                // verify file content against data map
                verifyExportTabularXlsx(
                        exportResult,
                        expectedPvColumnNames,
                        pvValidationMap,
                        expectedCalculationsColumnNames,
                        calculationsValidationMap,
                        expectedNumRows);
            }
        }

        return exportResult;
    }

    // =========================================================================
    // PvMetadata helper methods
    // =========================================================================

    public String sendAndVerifySavePvMetadata(
            AnnotationTestBase.SavePvMetadataParams params,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final SavePvMetadataRequest request = AnnotationTestBase.buildSavePvMetadataRequest(params);
        final DpAnnotationServiceGrpc.DpAnnotationServiceStub asyncStub = DpAnnotationServiceGrpc.newStub(channel);
        final AnnotationTestBase.SavePvMetadataResponseObserver responseObserver =
                new AnnotationTestBase.SavePvMetadataResponseObserver();

        new Thread(() -> asyncStub.savePvMetadata(request, responseObserver)).start();
        responseObserver.await();

        if (expectReject) {
            assertTrue(responseObserver.isError());
            assertTrue(responseObserver.getErrorMessage().contains(expectedRejectMessage));
            assertNull(responseObserver.getPvName());
            return null;
        }

        assertFalse(responseObserver.getErrorMessage(), responseObserver.isError());
        final String pvName = responseObserver.getPvName();
        assertNotNull(pvName);
        assertFalse(pvName.isBlank());

        return pvName;
    }

    public List<PvMetadata> sendAndVerifyQueryPvMetadata(
            List<QueryPvMetadataRequest.QueryPvMetadataCriterion> criteria,
            int limit,
            String pageToken,
            boolean expectReject,
            String expectedRejectMessage,
            int expectedResultCount
    ) {
        final QueryPvMetadataRequest request =
                AnnotationTestBase.buildQueryPvMetadataRequest(criteria, limit, pageToken);
        final DpAnnotationServiceGrpc.DpAnnotationServiceStub asyncStub = DpAnnotationServiceGrpc.newStub(channel);
        final AnnotationTestBase.QueryPvMetadataResponseObserver responseObserver =
                new AnnotationTestBase.QueryPvMetadataResponseObserver();

        new Thread(() -> asyncStub.queryPvMetadata(request, responseObserver)).start();
        responseObserver.await();

        if (expectReject) {
            assertTrue(responseObserver.isError());
            assertTrue(responseObserver.getErrorMessage().contains(expectedRejectMessage));
            return new ArrayList<>();
        }

        assertFalse(responseObserver.getErrorMessage(), responseObserver.isError());
        final List<PvMetadata> pvMetadataList = responseObserver.getPvMetadataList();
        assertEquals(expectedResultCount, pvMetadataList.size());
        return pvMetadataList;
    }

    public PvMetadata sendAndVerifyGetPvMetadata(
            String pvNameOrAlias,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final GetPvMetadataRequest request = AnnotationTestBase.buildGetPvMetadataRequest(pvNameOrAlias);
        final DpAnnotationServiceGrpc.DpAnnotationServiceStub asyncStub = DpAnnotationServiceGrpc.newStub(channel);
        final AnnotationTestBase.GetPvMetadataResponseObserver responseObserver =
                new AnnotationTestBase.GetPvMetadataResponseObserver();

        new Thread(() -> asyncStub.getPvMetadata(request, responseObserver)).start();
        responseObserver.await();

        if (expectReject) {
            assertTrue(responseObserver.isError());
            assertTrue(responseObserver.getErrorMessage().contains(expectedRejectMessage));
            return null;
        }

        assertFalse(responseObserver.getErrorMessage(), responseObserver.isError());
        final PvMetadata pvMetadata = responseObserver.getPvMetadata();
        assertNotNull(pvMetadata);
        return pvMetadata;
    }

    public String sendAndVerifyDeletePvMetadata(
            String pvNameOrAlias,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final DeletePvMetadataRequest request = AnnotationTestBase.buildDeletePvMetadataRequest(pvNameOrAlias);
        final DpAnnotationServiceGrpc.DpAnnotationServiceStub asyncStub = DpAnnotationServiceGrpc.newStub(channel);
        final AnnotationTestBase.DeletePvMetadataResponseObserver responseObserver =
                new AnnotationTestBase.DeletePvMetadataResponseObserver();

        new Thread(() -> asyncStub.deletePvMetadata(request, responseObserver)).start();
        responseObserver.await();

        if (expectReject) {
            assertTrue(responseObserver.isError());
            assertTrue(responseObserver.getErrorMessage().contains(expectedRejectMessage));
            return null;
        }

        assertFalse(responseObserver.getErrorMessage(), responseObserver.isError());
        final String deletedPvName = responseObserver.getDeletedPvName();
        assertNotNull(deletedPvName);
        assertFalse(deletedPvName.isBlank());
        return deletedPvName;
    }

    public void verifyExportTabularCsv(
            ExportDataResponse.ExportDataResult exportResult,
            Set<String> expectedPvColumnNames,
            Map<String, GrpcIntegrationIngestionServiceWrapper.IngestionStreamInfo> pvValidationMap,
            List<String> expectedCalculationsColumnNames,
            Map<String, TimestampMap<Double>> calculationsValidationMap,
            int expectedNumRows
    ) {
        // open csv file and create reader
        final Path exportFilePath = Paths.get(exportResult.getFilePath());
        CsvReader<CsvRecord> csvReader = null;
        try {
            csvReader = CsvReader.builder().ofCsvRecord(exportFilePath);
        } catch (IOException e) {
            fail("IOException reading csv file " + exportResult.getFilePath() + ": " + e.getMessage());
        }
        assertNotNull(csvReader);

        final Iterator<CsvRecord> csvRecordIterator = csvReader.iterator();
        final int expectedNumColumns = 2 + expectedPvColumnNames.size() + expectedCalculationsColumnNames.size();

        // verify header row
        List<String> csvColumnHeaders;
        {
            assertTrue(csvRecordIterator.hasNext());
            final CsvRecord csvRecord = csvRecordIterator.next();

            // check number of csv header columns matches expected
            assertEquals(expectedNumColumns, csvRecord.getFieldCount());

            csvColumnHeaders = csvRecord.getFields();

            // check that the csv file contains each of the expected PV columns
            for (String columnName : expectedPvColumnNames) {
                assertTrue(csvColumnHeaders.contains(columnName));
            }

            // check that csv file contains each of the expected calculations columns
            for (String columnName : expectedCalculationsColumnNames) {
                assertTrue(csvColumnHeaders.contains(columnName));
            }
        }

        // verify data rows
        {
            int dataRowCount = 0;
            while (csvRecordIterator.hasNext()) {

                // read row from csv file
                final CsvRecord csvRecord = csvRecordIterator.next();
                assertEquals(expectedNumColumns, csvRecord.getFieldCount());
                final List<String> csvRowValues = csvRecord.getFields();

                // get timestamp column values
                // we don't validate them directly, but by 1) checking the number of expected rows matches and
                // 2) accessing data from validationMap via seconds/nanos
                final long csvSeconds = Long.valueOf(csvRowValues.get(0));
                final long csvNanos = Long.valueOf(csvRowValues.get(1));

                // compare data values from csv file with expected
                for (int columnIndex = 2; columnIndex < csvRowValues.size(); columnIndex++) {

                    // get csv file data value and column name for column index
                    final String csvDataValue = csvRowValues.get(columnIndex);
                    final String csvColumnName = csvColumnHeaders.get(columnIndex);

                    Double expectedColumnDoubleValue = null;
                    if (expectedPvColumnNames.contains(csvColumnName)) {
                        // check expected data value for PV column
                        final TimestampMap<Double> columnValueMap = pvValidationMap.get(csvColumnName).valueMap;
                        expectedColumnDoubleValue = columnValueMap.get(csvSeconds, csvNanos);

                    } else if (expectedCalculationsColumnNames.contains(csvColumnName)) {
                        // check expected data value for calculations column
                        final TimestampMap<Double> columnValueMap = calculationsValidationMap.get(csvColumnName);
                        expectedColumnDoubleValue = columnValueMap.get(csvSeconds, csvNanos);

                    } else {
                        fail("unexpected export column (neither PV nor calculations): " + csvColumnName);
                    }
                }
                dataRowCount = dataRowCount + 1;
            }
            assertEquals(expectedNumRows, dataRowCount);
        }
    }

    public void verifyExportTabularXlsx(
            ExportDataResponse.ExportDataResult exportResult,
            Set<String> expectedPvColumnNames,
            Map<String, GrpcIntegrationIngestionServiceWrapper.IngestionStreamInfo> pvValidationMap,
            List<String> expectedCalculationsColumnNames,
            Map<String, TimestampMap<Double>> calculationsValidationMap,
            int expectedNumRows
    ) {
        // open excel file
        OPCPackage filePackage = null;
        try {
            filePackage = OPCPackage.open(new File(exportResult.getFilePath()));
        } catch (InvalidFormatException e) {
            fail(
                    "InvalidFormatException opening package for excel file "
                            + exportResult.getFilePath() + ": "
                            + e.getMessage());
        }
        assertNotNull(filePackage);

        // open excel workbook
        XSSFWorkbook fileWorkbook = null;
        try {
            fileWorkbook = new XSSFWorkbook(filePackage);
        } catch (IOException e) {
            fail(
                    "IOException creating workbook from excel file "
                            + exportResult.getFilePath() + ": "
                            + e.getMessage());;
        }
        assertNotNull(fileWorkbook);

        // open worksheet and create iterator
        Sheet fileSheet = fileWorkbook.getSheetAt(0);
        assertNotNull(fileSheet);
        final Iterator<Row> fileRowIterator = fileSheet.rowIterator();
        assertTrue(fileRowIterator.hasNext());

        final int expectedNumColumns = 2 + expectedPvColumnNames.size() + expectedCalculationsColumnNames.size();

        // verify header row from file
        List<String> fileColumnHeaders = new ArrayList<>();
        {
            final Row fileHeaderRow = fileRowIterator.next();
            assertNotNull(fileHeaderRow);

            // check number of header columns matches expected
            assertEquals(expectedNumColumns, fileHeaderRow.getLastCellNum());

            // build list of actual column headers
            for (int columnIndex = 0; columnIndex < fileHeaderRow.getLastCellNum(); columnIndex++) {
                final String fileHeaderValue = fileHeaderRow.getCell(columnIndex).getStringCellValue();
                fileColumnHeaders.add(fileHeaderValue);
            }

            // check that list of actual headers contains each of the expected PV column headers
            for (String columnName : expectedPvColumnNames) {
                assertTrue(fileColumnHeaders.contains(columnName));
            }

            // check list of actual headers contains each of the expected calculations column headers
            for (String columnName : expectedCalculationsColumnNames) {
                assertTrue(fileColumnHeaders.contains(columnName));
            }
        }

        // verify data rows from file
        {
            int dataRowCount = 0;
            while (fileRowIterator.hasNext()) {

                // read row from excel file
                final Row fileDataRow = fileRowIterator.next();
                assertEquals(expectedNumColumns, fileDataRow.getLastCellNum());

                // get timestamp column values
                // we don't validate them directly, but by 1) checking the number of expected rows matches and
                // 2) accessing data from validationMap via seconds/nanos
                final long fileSeconds = Double.valueOf(fileDataRow.getCell(0).getNumericCellValue()).longValue();
                final long fileNanos = Double.valueOf(fileDataRow.getCell(1).getNumericCellValue()).longValue();

                // verify data columns
                for (int fileColumnIndex = 2; fileColumnIndex < fileDataRow.getLastCellNum(); fileColumnIndex++) {

                    // get column data value and corresponding column name from file
                    final String fileColumnName = fileColumnHeaders.get(fileColumnIndex);
                    final Cell fileCell = fileDataRow.getCell(fileColumnIndex);
                    Double fileColumnDoubleValue = null;
                    if ( ! fileCell.getCellType().equals(CellType.STRING) ) {
                        fileColumnDoubleValue = Double.valueOf(fileCell.getNumericCellValue()).doubleValue();
                    }

                    Double expectedColumnDoubleValue = null;
                    if (expectedPvColumnNames.contains(fileColumnName)) {
                        // get expected data value for column (we assume value is double)
                        final TimestampMap<Double> columnValueMap = pvValidationMap.get(fileColumnName).valueMap;
                        expectedColumnDoubleValue = columnValueMap.get(fileSeconds, fileNanos);

                    } else if (expectedCalculationsColumnNames.contains(fileColumnName)) {
                        // check expected data value for calculations column
                        final TimestampMap<Double> columnValueMap = calculationsValidationMap.get(fileColumnName);
                        expectedColumnDoubleValue = columnValueMap.get(fileSeconds, fileNanos);

                    } else {
                        fail("unexpected export column (neither PV nor calculations): " + fileColumnName);
                    }

                    if (expectedColumnDoubleValue != null) {
                        assertEquals(expectedColumnDoubleValue, fileColumnDoubleValue, 0);
                    } else {
                        assertEquals(null, fileColumnDoubleValue);
                    }

// Keeping this code around in case we want to handle expectedDataValue with other type than Double.
//                    switch (expectedDataValue.getValueCase()) {
//                        case STRINGVALUE -> {
//                            assertEquals(expectedDataValue.getStringValue(), fileCell.getStringCellValue());
//                        }
//                        case BOOLEANVALUE -> {
//                            assertEquals(expectedDataValue.getBooleanValue(), fileCell.getBooleanCellValue());
//                        }
//                        case UINTVALUE -> {
//                            assertEquals(
//                                    expectedDataValue.getUintValue(),
//                                    Double.valueOf(fileCell.getNumericCellValue()).intValue());
//                        }
//                        case ULONGVALUE -> {
//                            assertEquals(
//                                    expectedDataValue.getUlongValue(),
//                                    Double.valueOf(fileCell.getNumericCellValue()).longValue());
//                        }
//                        case INTVALUE -> {
//                            assertEquals(
//                                    expectedDataValue.getIntValue(),
//                                    Double.valueOf(fileCell.getNumericCellValue()).intValue());
//                        }
//                        case LONGVALUE -> {
//                            assertEquals(
//                                    expectedDataValue.getLongValue(),
//                                    Double.valueOf(fileCell.getNumericCellValue()).longValue());
//                        }
//                        case FLOATVALUE -> {
//                            assertEquals(
//                                    expectedDataValue.getFloatValue(),
//                                    Double.valueOf(fileCell.getNumericCellValue()).floatValue(),
//                                    0.0);
//                        }
//                        case DOUBLEVALUE -> {
//                            assertEquals(
//                                    expectedDataValue.getDoubleValue(),
//                                    Double.valueOf(fileCell.getNumericCellValue()).doubleValue(),
//                                    0);
//                        }
//                        case BYTEARRAYVALUE -> {
//                        }
//                        case ARRAYVALUE -> {
//                        }
//                        case STRUCTUREVALUE -> {
//                        }
//                        case IMAGEVALUE -> {
//                        }
//                        case TIMESTAMPVALUE -> {
//                        }
//                        case VALUE_NOT_SET -> {
//                        }
//                        default -> {
//                            assertEquals(expectedDataValue.toString(), fileCell.getStringCellValue());
//                        }
//                    }
                }

                dataRowCount = dataRowCount + 1;
            }

            assertEquals(expectedNumRows, dataRowCount);
        }

        // close excel file
        try {
            filePackage.close();
        } catch (IOException e) {
            fail(
                    "IOException closing package for excel file "
                            + exportResult.getFilePath() + ": "
                            + e.getMessage());;
        }
    }

    public void sendAndVerifyPatchPvMetadataStub(String pvName) {
        final PatchPvMetadataRequest request = PatchPvMetadataRequest.newBuilder()
                .setPvName(pvName)
                .build();
        final DpAnnotationServiceGrpc.DpAnnotationServiceStub asyncStub =
                DpAnnotationServiceGrpc.newStub(channel);
        final AnnotationTestBase.PatchPvMetadataResponseObserver responseObserver =
                new AnnotationTestBase.PatchPvMetadataResponseObserver();
        new Thread(() -> asyncStub.patchPvMetadata(request, responseObserver)).start();
        responseObserver.await();
        assertTrue(responseObserver.isError());
        assertTrue(responseObserver.getErrorMessage().contains("not yet implemented"));
    }

    public void sendAndVerifyBulkSavePvMetadataStub() {
        final BulkSavePvMetadataRequest request = BulkSavePvMetadataRequest.newBuilder().build();
        final DpAnnotationServiceGrpc.DpAnnotationServiceStub asyncStub =
                DpAnnotationServiceGrpc.newStub(channel);
        final AnnotationTestBase.BulkSavePvMetadataResponseObserver responseObserver =
                new AnnotationTestBase.BulkSavePvMetadataResponseObserver();
        new Thread(() -> asyncStub.bulkSavePvMetadata(request, responseObserver)).start();
        responseObserver.await();
        assertTrue(responseObserver.isError());
        assertTrue(responseObserver.getErrorMessage().contains("not yet implemented"));
    }

    // =========================================================================
    // Configuration helper methods
    // =========================================================================

    public String sendAndVerifySaveConfiguration(
            AnnotationTestBase.SaveConfigurationParams params,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final SaveConfigurationRequest request = AnnotationTestBase.buildSaveConfigurationRequest(params);
        final DpAnnotationServiceGrpc.DpAnnotationServiceStub asyncStub = DpAnnotationServiceGrpc.newStub(channel);
        final AnnotationTestBase.SaveConfigurationResponseObserver responseObserver =
                new AnnotationTestBase.SaveConfigurationResponseObserver();

        new Thread(() -> asyncStub.saveConfiguration(request, responseObserver)).start();
        responseObserver.await();

        if (expectReject) {
            assertTrue(responseObserver.isError());
            assertTrue(responseObserver.getErrorMessage().contains(expectedRejectMessage));
            assertNull(responseObserver.getConfigurationName());
            return null;
        }

        assertFalse(responseObserver.getErrorMessage(), responseObserver.isError());
        final String configurationName = responseObserver.getConfigurationName();
        assertNotNull(configurationName);
        assertFalse(configurationName.isBlank());
        return configurationName;
    }

    public Configuration sendAndVerifyGetConfiguration(
            String configurationName,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final GetConfigurationRequest request = AnnotationTestBase.buildGetConfigurationRequest(configurationName);
        final DpAnnotationServiceGrpc.DpAnnotationServiceStub asyncStub = DpAnnotationServiceGrpc.newStub(channel);
        final AnnotationTestBase.GetConfigurationResponseObserver responseObserver =
                new AnnotationTestBase.GetConfigurationResponseObserver();

        new Thread(() -> asyncStub.getConfiguration(request, responseObserver)).start();
        responseObserver.await();

        if (expectReject) {
            assertTrue(responseObserver.isError());
            assertTrue(responseObserver.getErrorMessage().contains(expectedRejectMessage));
            return null;
        }

        assertFalse(responseObserver.getErrorMessage(), responseObserver.isError());
        final Configuration configuration = responseObserver.getConfiguration();
        assertNotNull(configuration);
        return configuration;
    }

    public List<Configuration> sendAndVerifyQueryConfigurations(
            List<QueryConfigurationsRequest.QueryConfigurationsCriterion> criteria,
            int limit,
            String pageToken,
            boolean expectReject,
            String expectedRejectMessage,
            int expectedResultCount
    ) {
        final QueryConfigurationsRequest request =
                AnnotationTestBase.buildQueryConfigurationsRequest(criteria, limit, pageToken);
        final DpAnnotationServiceGrpc.DpAnnotationServiceStub asyncStub = DpAnnotationServiceGrpc.newStub(channel);
        final AnnotationTestBase.QueryConfigurationsResponseObserver responseObserver =
                new AnnotationTestBase.QueryConfigurationsResponseObserver();

        new Thread(() -> asyncStub.queryConfigurations(request, responseObserver)).start();
        responseObserver.await();

        if (expectReject) {
            assertTrue(responseObserver.isError());
            assertTrue(responseObserver.getErrorMessage().contains(expectedRejectMessage));
            return new ArrayList<>();
        }

        assertFalse(responseObserver.getErrorMessage(), responseObserver.isError());
        final List<Configuration> configList = responseObserver.getConfigurationList();
        assertEquals(expectedResultCount, configList.size());
        return configList;
    }

    public String sendAndVerifyDeleteConfiguration(
            String configurationName,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final DeleteConfigurationRequest request = AnnotationTestBase.buildDeleteConfigurationRequest(configurationName);
        final DpAnnotationServiceGrpc.DpAnnotationServiceStub asyncStub = DpAnnotationServiceGrpc.newStub(channel);
        final AnnotationTestBase.DeleteConfigurationResponseObserver responseObserver =
                new AnnotationTestBase.DeleteConfigurationResponseObserver();

        new Thread(() -> asyncStub.deleteConfiguration(request, responseObserver)).start();
        responseObserver.await();

        if (expectReject) {
            assertTrue(responseObserver.isError());
            assertTrue(responseObserver.getErrorMessage().contains(expectedRejectMessage));
            return null;
        }

        assertFalse(responseObserver.getErrorMessage(), responseObserver.isError());
        final String deletedName = responseObserver.getConfigurationName();
        assertNotNull(deletedName);
        assertFalse(deletedName.isBlank());
        return deletedName;
    }

    public void sendAndVerifyPatchConfigurationStub(String configurationName) {
        final PatchConfigurationRequest request = PatchConfigurationRequest.newBuilder()
                .setConfigurationName(configurationName).build();
        final DpAnnotationServiceGrpc.DpAnnotationServiceStub asyncStub = DpAnnotationServiceGrpc.newStub(channel);
        final AnnotationTestBase.PatchConfigurationResponseObserver responseObserver =
                new AnnotationTestBase.PatchConfigurationResponseObserver();
        new Thread(() -> asyncStub.patchConfiguration(request, responseObserver)).start();
        responseObserver.await();
        assertTrue(responseObserver.isError());
        assertTrue(responseObserver.getErrorMessage().contains("not yet implemented"));
    }

    public void sendAndVerifyBulkSaveConfigurationStub() {
        final BulkSaveConfigurationRequest request = BulkSaveConfigurationRequest.newBuilder().build();
        final DpAnnotationServiceGrpc.DpAnnotationServiceStub asyncStub = DpAnnotationServiceGrpc.newStub(channel);
        final AnnotationTestBase.BulkSaveConfigurationResponseObserver responseObserver =
                new AnnotationTestBase.BulkSaveConfigurationResponseObserver();
        new Thread(() -> asyncStub.bulkSaveConfiguration(request, responseObserver)).start();
        responseObserver.await();
        assertTrue(responseObserver.isError());
        assertTrue(responseObserver.getErrorMessage().contains("not yet implemented"));
    }

    // =========================================================================
    // ConfigurationActivation helper methods
    // =========================================================================

    public String sendAndVerifySaveConfigurationActivation(
            AnnotationTestBase.SaveConfigurationActivationParams params,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final SaveConfigurationActivationRequest request =
                AnnotationTestBase.buildSaveConfigurationActivationRequest(params);
        final DpAnnotationServiceGrpc.DpAnnotationServiceStub asyncStub = DpAnnotationServiceGrpc.newStub(channel);
        final AnnotationTestBase.SaveConfigurationActivationResponseObserver responseObserver =
                new AnnotationTestBase.SaveConfigurationActivationResponseObserver();

        new Thread(() -> asyncStub.saveConfigurationActivation(request, responseObserver)).start();
        responseObserver.await();

        if (expectReject) {
            assertTrue(responseObserver.isError());
            assertTrue(responseObserver.getErrorMessage().contains(expectedRejectMessage));
            assertNull(responseObserver.getClientActivationId());
            return null;
        }

        assertFalse(responseObserver.getErrorMessage(), responseObserver.isError());
        final String clientActivationId = responseObserver.getClientActivationId();
        assertNotNull(clientActivationId);
        assertFalse(clientActivationId.isBlank());
        return clientActivationId;
    }

    public ConfigurationActivation sendAndVerifyGetConfigurationActivationById(
            String clientActivationId,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final GetConfigurationActivationRequest request =
                AnnotationTestBase.buildGetConfigurationActivationByIdRequest(clientActivationId);
        final DpAnnotationServiceGrpc.DpAnnotationServiceStub asyncStub = DpAnnotationServiceGrpc.newStub(channel);
        final AnnotationTestBase.GetConfigurationActivationResponseObserver responseObserver =
                new AnnotationTestBase.GetConfigurationActivationResponseObserver();

        new Thread(() -> asyncStub.getConfigurationActivation(request, responseObserver)).start();
        responseObserver.await();

        if (expectReject) {
            assertTrue(responseObserver.isError());
            assertTrue(responseObserver.getErrorMessage().contains(expectedRejectMessage));
            return null;
        }

        assertFalse(responseObserver.getErrorMessage(), responseObserver.isError());
        final ConfigurationActivation activation = responseObserver.getConfigurationActivation();
        assertNotNull(activation);
        return activation;
    }

    public ConfigurationActivation sendAndVerifyGetConfigurationActivationByCompositeKey(
            String configurationName,
            com.ospreydcs.dp.grpc.v1.common.Timestamp startTime,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final GetConfigurationActivationRequest request =
                AnnotationTestBase.buildGetConfigurationActivationByCompositeKeyRequest(configurationName, startTime);
        final DpAnnotationServiceGrpc.DpAnnotationServiceStub asyncStub = DpAnnotationServiceGrpc.newStub(channel);
        final AnnotationTestBase.GetConfigurationActivationResponseObserver responseObserver =
                new AnnotationTestBase.GetConfigurationActivationResponseObserver();

        new Thread(() -> asyncStub.getConfigurationActivation(request, responseObserver)).start();
        responseObserver.await();

        if (expectReject) {
            assertTrue(responseObserver.isError());
            assertTrue(responseObserver.getErrorMessage().contains(expectedRejectMessage));
            return null;
        }

        assertFalse(responseObserver.getErrorMessage(), responseObserver.isError());
        final ConfigurationActivation activation = responseObserver.getConfigurationActivation();
        assertNotNull(activation);
        return activation;
    }

    public List<ConfigurationActivation> sendAndVerifyQueryConfigurationActivations(
            List<QueryConfigurationActivationsRequest.QueryConfigurationActivationsCriterion> criteria,
            int limit,
            String pageToken,
            boolean expectReject,
            String expectedRejectMessage,
            int expectedResultCount
    ) {
        final QueryConfigurationActivationsRequest request =
                AnnotationTestBase.buildQueryConfigurationActivationsRequest(criteria, limit, pageToken);
        final DpAnnotationServiceGrpc.DpAnnotationServiceStub asyncStub = DpAnnotationServiceGrpc.newStub(channel);
        final AnnotationTestBase.QueryConfigurationActivationsResponseObserver responseObserver =
                new AnnotationTestBase.QueryConfigurationActivationsResponseObserver();

        new Thread(() -> asyncStub.queryConfigurationActivations(request, responseObserver)).start();
        responseObserver.await();

        if (expectReject) {
            assertTrue(responseObserver.isError());
            assertTrue(responseObserver.getErrorMessage().contains(expectedRejectMessage));
            return new ArrayList<>();
        }

        assertFalse(responseObserver.getErrorMessage(), responseObserver.isError());
        final List<ConfigurationActivation> activationList = responseObserver.getActivationList();
        assertEquals(expectedResultCount, activationList.size());
        return activationList;
    }

    public String sendAndVerifyDeleteConfigurationActivationById(
            String clientActivationId,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final DeleteConfigurationActivationRequest request =
                AnnotationTestBase.buildDeleteConfigurationActivationByIdRequest(clientActivationId);
        final DpAnnotationServiceGrpc.DpAnnotationServiceStub asyncStub = DpAnnotationServiceGrpc.newStub(channel);
        final AnnotationTestBase.DeleteConfigurationActivationResponseObserver responseObserver =
                new AnnotationTestBase.DeleteConfigurationActivationResponseObserver();

        new Thread(() -> asyncStub.deleteConfigurationActivation(request, responseObserver)).start();
        responseObserver.await();

        if (expectReject) {
            assertTrue(responseObserver.isError());
            assertTrue(responseObserver.getErrorMessage().contains(expectedRejectMessage));
            return null;
        }

        assertFalse(responseObserver.getErrorMessage(), responseObserver.isError());
        final String deletedId = responseObserver.getClientActivationId();
        assertNotNull(deletedId);
        assertFalse(deletedId.isBlank());
        return deletedId;
    }

    public String sendAndVerifyDeleteConfigurationActivationByCompositeKey(
            String configurationName,
            com.ospreydcs.dp.grpc.v1.common.Timestamp startTime,
            boolean expectReject,
            String expectedRejectMessage
    ) {
        final DeleteConfigurationActivationRequest request =
                AnnotationTestBase.buildDeleteConfigurationActivationByCompositeKeyRequest(configurationName, startTime);
        final DpAnnotationServiceGrpc.DpAnnotationServiceStub asyncStub = DpAnnotationServiceGrpc.newStub(channel);
        final AnnotationTestBase.DeleteConfigurationActivationResponseObserver responseObserver =
                new AnnotationTestBase.DeleteConfigurationActivationResponseObserver();

        new Thread(() -> asyncStub.deleteConfigurationActivation(request, responseObserver)).start();
        responseObserver.await();

        if (expectReject) {
            assertTrue(responseObserver.isError());
            assertTrue(responseObserver.getErrorMessage().contains(expectedRejectMessage));
            return null;
        }

        assertFalse(responseObserver.getErrorMessage(), responseObserver.isError());
        final String deletedId = responseObserver.getClientActivationId();
        assertNotNull(deletedId);
        assertFalse(deletedId.isBlank());
        return deletedId;
    }

    public List<ConfigurationActivation> sendAndVerifyGetActiveConfigurations(
            com.ospreydcs.dp.grpc.v1.common.Timestamp timestamp,
            boolean expectReject,
            String expectedRejectMessage,
            int expectedResultCount
    ) {
        final GetActiveConfigurationsRequest request = timestamp != null
                ? AnnotationTestBase.buildGetActiveConfigurationsRequest(timestamp)
                : GetActiveConfigurationsRequest.newBuilder().build();
        final DpAnnotationServiceGrpc.DpAnnotationServiceStub asyncStub = DpAnnotationServiceGrpc.newStub(channel);
        final AnnotationTestBase.GetActiveConfigurationsResponseObserver responseObserver =
                new AnnotationTestBase.GetActiveConfigurationsResponseObserver();

        new Thread(() -> asyncStub.getActiveConfigurations(request, responseObserver)).start();
        responseObserver.await();

        if (expectReject) {
            assertTrue(responseObserver.isError());
            assertTrue(responseObserver.getErrorMessage().contains(expectedRejectMessage));
            return new ArrayList<>();
        }

        assertFalse(responseObserver.getErrorMessage(), responseObserver.isError());
        final List<ConfigurationActivation> activationList = responseObserver.getActivationList();
        assertEquals(expectedResultCount, activationList.size());
        return activationList;
    }

    public void sendAndVerifyPatchConfigurationActivationStub(String clientActivationId) {
        final PatchConfigurationActivationRequest request = PatchConfigurationActivationRequest.newBuilder()
                .setClientActivationId(clientActivationId).build();
        final DpAnnotationServiceGrpc.DpAnnotationServiceStub asyncStub = DpAnnotationServiceGrpc.newStub(channel);
        final AnnotationTestBase.PatchConfigurationActivationResponseObserver responseObserver =
                new AnnotationTestBase.PatchConfigurationActivationResponseObserver();
        new Thread(() -> asyncStub.patchConfigurationActivation(request, responseObserver)).start();
        responseObserver.await();
        assertTrue(responseObserver.isError());
        assertTrue(responseObserver.getErrorMessage().contains("not yet implemented"));
    }

    public void sendAndVerifyBulkSaveConfigurationActivationStub() {
        final BulkSaveConfigurationActivationRequest request =
                BulkSaveConfigurationActivationRequest.newBuilder().build();
        final DpAnnotationServiceGrpc.DpAnnotationServiceStub asyncStub = DpAnnotationServiceGrpc.newStub(channel);
        final AnnotationTestBase.BulkSaveConfigurationActivationResponseObserver responseObserver =
                new AnnotationTestBase.BulkSaveConfigurationActivationResponseObserver();
        new Thread(() -> asyncStub.bulkSaveConfigurationActivation(request, responseObserver)).start();
        responseObserver.await();
        assertTrue(responseObserver.isError());
        assertTrue(responseObserver.getErrorMessage().contains("not yet implemented"));
    }

}
