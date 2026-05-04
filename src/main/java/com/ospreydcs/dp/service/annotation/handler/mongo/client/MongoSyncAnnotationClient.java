package com.ospreydcs.dp.service.annotation.handler.mongo.client;

import com.mongodb.MongoException;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import com.ospreydcs.dp.grpc.v1.annotation.QueryAnnotationsRequest;
import com.ospreydcs.dp.grpc.v1.annotation.QueryConfigurationActivationsRequest;
import com.ospreydcs.dp.grpc.v1.annotation.QueryConfigurationsRequest;
import com.ospreydcs.dp.grpc.v1.annotation.QueryDataSetsRequest;
import com.ospreydcs.dp.grpc.v1.annotation.QueryPvMetadataRequest;
import com.ospreydcs.dp.service.common.bson.BsonConstants;
import com.ospreydcs.dp.service.common.bson.annotation.AnnotationDocument;
import com.ospreydcs.dp.service.common.bson.calculations.CalculationsDocument;
import com.ospreydcs.dp.service.common.bson.dataset.DataSetDocument;
import com.ospreydcs.dp.service.common.bson.configuration.ConfigurationActivationDocument;
import com.ospreydcs.dp.service.common.bson.configuration.ConfigurationDocument;
import com.ospreydcs.dp.service.common.bson.pvmetadata.PvMetadataDocument;
import com.ospreydcs.dp.service.common.model.ConfigurationActivationQueryResult;
import com.ospreydcs.dp.service.common.model.ConfigurationQueryResult;
import com.ospreydcs.dp.service.common.model.MongoDeleteResult;
import com.ospreydcs.dp.service.common.model.MongoInsertOneResult;
import com.ospreydcs.dp.service.common.model.MongoSaveResult;
import com.ospreydcs.dp.service.common.model.PvMetadataQueryResult;
import com.ospreydcs.dp.service.common.mongo.MongoSyncClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.UUID;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Indexes.ascending;

public class MongoSyncAnnotationClient extends MongoSyncClient implements MongoAnnotationClientInterface {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    @Override
    public DataSetDocument findDataSet(String dataSetId) {
        // TODO: do we need to wrap this in a retry loop?  I'm not adding it now, my reasoning is that if the caller
        // sending request has a dataSetId, it already exists in the database.
        List<DataSetDocument> matchingDocuments = new ArrayList<>();

        // wrap this in a try/catch because otherwise we take out the thread if mongo throws an exception
        try {
            mongoCollectionDataSets.find(
                    eq(BsonConstants.BSON_KEY_DATA_SET_ID, new ObjectId(dataSetId))).into(matchingDocuments);
        } catch (Exception ex) {
            logger.error("findDataSet: mongo exception in find(): {}", ex.getMessage());
            return null;
        }

        if (matchingDocuments.size() > 0) {
            return matchingDocuments.get(0);
        } else {
            return null;
        }
    }

    @Override
    public MongoSaveResult saveDataSet(DataSetDocument dataSetDocument, String existingDocumentId) {

        logger.debug("saving DataSetDocument existingDocumentId: {}", existingDocumentId);

        // try to fetch existing document
        DataSetDocument existingDocument = null;
        if (!existingDocumentId.isBlank()) {
            existingDocument = findDataSet(existingDocumentId);
            if (existingDocument == null) {
                final String errorMsg = "saveDataSet no DataSetDocument found with id: " + existingDocumentId;
                logger.error(errorMsg);
                return new MongoSaveResult(true, errorMsg, existingDocumentId, false);
            }
        }

        if (existingDocument == null) {
            // create a new document
            dataSetDocument.addCreationTime(); // set creation time
            InsertOneResult insertOneResult = mongoCollectionDataSets.insertOne(dataSetDocument);

            if (!insertOneResult.wasAcknowledged()) {
                final String errorMsg = "insertOne failed for new DataSetDocument, result not acknowledged";
                logger.error(errorMsg);
                return new MongoSaveResult(true, errorMsg, null, true);
            }

            // check if result contains id inserted
            if (insertOneResult.getInsertedId() == null) {
                final String errorMsg = "DataSetDocument insert failed to return document id";
                logger.error(errorMsg);
                return new MongoSaveResult(true, errorMsg, null, true);
            }

            // insert was successful
            return new MongoSaveResult(
                    false,
                    "",
                    insertOneResult.getInsertedId().asObjectId().getValue().toString(),
                    true);

        } else {
            // update existing document

            // use original creation time and add update time
            dataSetDocument.setCreatedAt(existingDocument.getCreatedAt());
            dataSetDocument.addUpdatedTime();

            UpdateResult result = null;
            try {
                final ReplaceOptions replaceOptions = new ReplaceOptions().upsert(true);
                final Bson idFilter = eq(BsonConstants.BSON_KEY_DATA_SET_ID, new ObjectId(existingDocumentId));
                result = mongoCollectionDataSets.replaceOne(idFilter, dataSetDocument, replaceOptions);
            } catch (MongoException ex) {
                final String errorMsg = "MongoException replacing DataSetDocument: " + ex.getMessage();
                logger.error(errorMsg);
                return new MongoSaveResult(true, errorMsg, existingDocumentId, false);
            }

            if (!result.wasAcknowledged()) {
                final String errorMsg = "replaceOne not acknowledged for existing DataSetDocument id: "
                        + existingDocumentId;
                logger.error(errorMsg);
                return new MongoSaveResult(true, errorMsg, existingDocumentId, false);
            }

            if (result.getModifiedCount() != 1) {
                final String errorMsg = "replaceOne DataSetDocument unexpected modified count: " + result.getModifiedCount();
                logger.error(errorMsg);
                return new MongoSaveResult(true, errorMsg, existingDocumentId, false);
            }

            return new MongoSaveResult(false, "", existingDocumentId, false);
        }
    }

    @Override
    public MongoCursor<DataSetDocument> executeQueryDataSets(QueryDataSetsRequest request) {

        // create query filter from request search criteria
        final List<Bson> globalFilterList = new ArrayList<>();
        final List<Bson> criteriaFilterList = new ArrayList<>();

        final List<QueryDataSetsRequest.QueryDataSetsCriterion> criterionList = request.getCriteriaList();
        for (QueryDataSetsRequest.QueryDataSetsCriterion criterion : criterionList) {
            switch (criterion.getCriterionCase()) {

                case IDCRITERION -> {
                    final String datasetId = criterion.getIdCriterion().getId();
                    if (! datasetId.isBlank()) {
                        Bson idFilter = Filters.eq(BsonConstants.BSON_KEY_DATA_SET_ID, new ObjectId(datasetId));
                        globalFilterList.add(idFilter);
                    }
                }

                case OWNERCRITERION -> {
                    // update ownerFilter from OwnerCriterion
                    final String ownerId = criterion.getOwnerCriterion().getOwnerId();
                    if (! ownerId.isBlank()) {
                        Bson ownerFilter = Filters.eq(BsonConstants.BSON_KEY_DATA_SET_OWNER_ID, ownerId);
                        globalFilterList.add(ownerFilter);
                    }
                }

                case TEXTCRITERION -> {
                    final String text = criterion.getTextCriterion().getText();
                    if (! text.isBlank()) {
                        final Bson descriptionFilter = Filters.text(text);
                        criteriaFilterList.add(descriptionFilter);
                    }
                }

                case PVNAMECRITERION -> {
                    final String name = criterion.getPvNameCriterion().getName();
                    if (! name.isBlank()) {
                        final Bson descriptionFilter = Filters.in(BsonConstants.BSON_KEY_DATA_SET_BLOCK_PV_NAMES, name);
                        criteriaFilterList.add(descriptionFilter);
                    }
                }

                case CRITERION_NOT_SET -> {
                    // shouldn't happen since validation checks for this, but...
                    logger.error("executeQueryDataSets unexpected error criterion case not set");
                }
            }
        }

        if (globalFilterList.isEmpty() && criteriaFilterList.isEmpty()) {
            // shouldn't happen since validation checks for this, but...
            logger.debug("no search criteria specified in QueryDataSetsRequest filter");
            return null;
        }

        // create global filter to be combined with and operator (default matches all Annotations)
        Bson globalFilter = Filters.exists(BsonConstants.BSON_KEY_DATA_SET_ID);
        if (globalFilterList.size() > 0) {
            globalFilter = and(globalFilterList);
        }

        // create criteria filter to be combined with or operator (default matches all Annotations)
        Bson criteriaFilter = Filters.exists(BsonConstants.BSON_KEY_DATA_SET_ID);
        if (criteriaFilterList.size() > 0) {
            criteriaFilter = or(criteriaFilterList);
        }

        // combine global filter with criteria filter using and operator
        final Bson queryFilter = and(globalFilter, criteriaFilter);

        logger.debug("executing queryDataSets filter: " + queryFilter.toString());

        final MongoCursor<DataSetDocument> resultCursor = mongoCollectionDataSets
                .find(queryFilter)
                .sort(ascending(BsonConstants.BSON_KEY_DATA_SET_ID))
                .cursor();

        if (resultCursor == null) {
            logger.error("executeQueryDataSets received null cursor from mongodb.find");
        }

        return resultCursor;
    }

    @Override
    public AnnotationDocument findAnnotation(String annotationId) {

        // TODO: do we need to wrap this in a retry loop?  I'm not adding it now, my reasoning is that if the caller
        // sending request has an annotationId, it already exists in the database.
        List<AnnotationDocument> matchingDocuments = new ArrayList<>();

        // wrap this in a try/catch because otherwise we take out the thread if mongo throws an exception
        try {
            mongoCollectionAnnotations.find(
                    eq(BsonConstants.BSON_KEY_ANNOTATION_ID, new ObjectId(annotationId))).into(matchingDocuments);
        } catch (Exception ex) {
            logger.error("findAnnotation: mongo exception in find(): {}", ex.getMessage());
            return null;
        }

        if (!matchingDocuments.isEmpty()) {
            return matchingDocuments.get(0);
        } else {
            return null;
        }
     }

    @Override
    public MongoSaveResult saveAnnotation(AnnotationDocument annotationDocument, String existingDocumentId) {

        logger.debug("saving AnnotationDocument existingDocumentId: {}", existingDocumentId);

        // try to fetch existing document
        AnnotationDocument existingDocument = null;
        if (!existingDocumentId.isBlank()) {
            existingDocument = findAnnotation(existingDocumentId);
            if (existingDocument == null) {
                final String errorMsg = "saveAnnotation no AnnotationDocument found with id: " + existingDocumentId;
                logger.error(errorMsg);
                return new MongoSaveResult(true, errorMsg, existingDocumentId, false);
            }
        }

        if (existingDocument == null) {
            // create a new document
            annotationDocument.addCreationTime(); // set creation time
            InsertOneResult insertOneResult = mongoCollectionAnnotations.insertOne(annotationDocument);

            if (!insertOneResult.wasAcknowledged()) {
                final String errorMsg = "insertOne failed for new AnnotationDocument, result not acknowledged";
                logger.error(errorMsg);
                return new MongoSaveResult(true, errorMsg, null, true);
            }

            // check if result contains id inserted
            if (insertOneResult.getInsertedId() == null) {
                final String errorMsg = "AnnotationDocument insert failed to return document id";
                logger.error(errorMsg);
                return new MongoSaveResult(true, errorMsg, null, true);
            }

            // insert was successful
            return new MongoSaveResult(
                    false,
                    "",
                    insertOneResult.getInsertedId().asObjectId().getValue().toString(),
                    true);

        } else {
            // update existing document

            // use original creation time and add update time
            annotationDocument.setCreatedAt(existingDocument.getCreatedAt());
            annotationDocument.addUpdatedTime();

            UpdateResult result = null;
            try {
                final ReplaceOptions replaceOptions = new ReplaceOptions().upsert(true);
                final Bson idFilter = eq(BsonConstants.BSON_KEY_DATA_SET_ID, new ObjectId(existingDocumentId));
                result = mongoCollectionAnnotations.replaceOne(idFilter, annotationDocument, replaceOptions);
            } catch (MongoException ex) {
                final String errorMsg = "MongoException replacing AnnotationDocument: " + ex.getMessage();
                logger.error(errorMsg);
                return new MongoSaveResult(true, errorMsg, existingDocumentId, false);
            }

            if (!result.wasAcknowledged()) {
                final String errorMsg = "replaceOne not acknowledged for existing AnnotationDocument id: "
                        + existingDocumentId;
                logger.error(errorMsg);
                return new MongoSaveResult(true, errorMsg, existingDocumentId, false);
            }

            if (result.getModifiedCount() != 1) {
                final String errorMsg =
                        "replaceOne AnnotationDocument unexpected modified count: " + result.getModifiedCount();
                logger.error(errorMsg);
                return new MongoSaveResult(true, errorMsg, existingDocumentId, false);
            }

            return new MongoSaveResult(false, "", existingDocumentId, false);
        }
    }

    @Override
    public MongoCursor<AnnotationDocument> executeQueryAnnotations(QueryAnnotationsRequest request) {

        // create query filter from request search criteria
        final List<Bson> globalFilterList = new ArrayList<>();
        final List<Bson> criteriaFilterList = new ArrayList<>();
        final List<QueryAnnotationsRequest.QueryAnnotationsCriterion> criterionList = request.getCriteriaList();
        for (QueryAnnotationsRequest.QueryAnnotationsCriterion criterion : criterionList) {
            switch (criterion.getCriterionCase()) {

                case IDCRITERION -> {
                    // annotation id filter, combined with other filters by AND operator
                    final String annotationId = criterion.getIdCriterion().getId();
                    if ( ! annotationId.isBlank()) {
                        Bson idFilter = Filters.eq(BsonConstants.BSON_KEY_ANNOTATION_ID, new ObjectId(annotationId));
                        globalFilterList.add(idFilter);
                    }
                }

                case OWNERCRITERION -> {
                    // owner id filter, combined with other filters by AND operator
                    final String ownerId = criterion.getOwnerCriterion().getOwnerId();
                    if ( ! ownerId.isBlank()) {
                        Bson ownerFilter = Filters.eq(BsonConstants.BSON_KEY_ANNOTATION_OWNER_ID, ownerId);
                        globalFilterList.add(ownerFilter);
                    }
                }

                case DATASETSCRITERION -> {
                    // associated dataset id filter, combined with other filters by AND operator
                    final String dataSetId = criterion.getDataSetsCriterion().getDataSetId();
                    if ( ! dataSetId.isBlank()) {
                        Bson dataSetIdFilter = Filters.in(BsonConstants.BSON_KEY_ANNOTATION_DATASET_IDS, dataSetId);
                        globalFilterList.add(dataSetIdFilter);
                    }
                }

                case ANNOTATIONSCRITERION -> {
                    // assciated annotation ids filter, combined with other filters by OR operator
                    final String annotationId = criterion.getAnnotationsCriterion().getAnnotationId();
                    if ( ! annotationId.isBlank()) {
                        Bson associatedAnnotationFilter = Filters.in(BsonConstants.BSON_KEY_ANNOTATION_ANNOTATION_IDS, annotationId);
                        criteriaFilterList.add(associatedAnnotationFilter);
                    }
                }

                case TEXTCRITERION -> {
                    // name filter, combined with other filters by AND operator
                    final String nameText = criterion.getTextCriterion().getText();
                    if ( ! nameText.isBlank()) {
                        final Bson nameFilter = Filters.text(nameText);
                        globalFilterList.add(nameFilter);
                    }
                }

                case TAGSCRITERION -> {
                    // tags filter, combined with other filters by OR operator
                    final String tagValue = criterion.getTagsCriterion().getTagValue();
                    if ( ! tagValue.isBlank()) {
                        Bson tagsFilter = Filters.in(BsonConstants.BSON_KEY_TAGS, tagValue);
                        criteriaFilterList.add(tagsFilter);
                    }
                }

                case ATTRIBUTESCRITERION -> {
                    // attributes filter, combined with other filters by OR operator
                    final String attributeKey = criterion.getAttributesCriterion().getKey();
                    final String attributeValue = criterion.getAttributesCriterion().getValue();
                    if ( ! attributeKey.isBlank() && ! attributeValue.isBlank()) {
                        final String mapKey = BsonConstants.BSON_KEY_ATTRIBUTES + "." + attributeKey;
                        Bson attributesFilter = Filters.eq(mapKey, attributeValue);
                        criteriaFilterList.add(attributesFilter);
                    }
                }

                case CRITERION_NOT_SET -> {
                    // shouldn't happen since validation checks for this, but...
                    logger.error("executeQueryAnnotations unexpected error criterion case not set");
                }
            }
        }

        if (globalFilterList.isEmpty() && criteriaFilterList.isEmpty()) {
            // shouldn't happen since validation checks for this, but...
            logger.debug("no search criteria specified in QueryAnnotationsRequest filter");
            return null;
        }

        // create global filter to be combined with and operator (default matches all Annotations)
        Bson globalFilter = Filters.exists(BsonConstants.BSON_KEY_ANNOTATION_ID);
        if (globalFilterList.size() > 0) {
            globalFilter = and(globalFilterList);
        }

        // create criteria filter to be combined with or operator (default matches all Annotations)
        Bson criteriaFilter = Filters.exists(BsonConstants.BSON_KEY_ANNOTATION_ID);
        if (criteriaFilterList.size() > 0) {
            criteriaFilter = or(criteriaFilterList);
        }

        // combine global filter with criteria filter using and operator
        final Bson queryFilter = and(globalFilter, criteriaFilter);

        logger.debug("executing queryAnnotations filter: " + queryFilter.toString());

        final MongoCursor<AnnotationDocument> resultCursor = mongoCollectionAnnotations
                .find(queryFilter)
                .sort(ascending(BsonConstants.BSON_KEY_ANNOTATION_ID))
                .cursor();

        if (resultCursor == null) {
            logger.error("executeQueryAnnotations received null cursor from mongodb.find");
        }

        return resultCursor;
    }

    @Override
    public MongoInsertOneResult insertCalculations(CalculationsDocument calculationsDocument) {

        logger.debug("inserting CalculationsDocument id: {}", calculationsDocument.getId());

        // set createdAt field for document
        calculationsDocument.addCreationTime();

        InsertOneResult result = null;
        boolean isError = false;
        String errorMsg = "";
        try {
            result = mongoCollectionCalculations.insertOne(calculationsDocument);
        } catch (MongoException ex) {
            isError = true;
            errorMsg = "MongoException inserting CalculationsDocument: " + ex.getMessage();
            logger.error(errorMsg);
        }

        return new MongoInsertOneResult(isError, errorMsg, result);
    }

    @Override
    public CalculationsDocument findCalculations(String calculationsId) {

        // TODO: do we need to wrap this in a retry loop?  I'm not adding it now, my reasoning is that if the caller
        // sending request has a calculationsId, it already exists in the database.
        List<CalculationsDocument> matchingDocuments = new ArrayList<>();

        // wrap this in a try/catch because otherwise we take out the thread if mongo throws an exception
        try {
            mongoCollectionCalculations.find(
                    eq(BsonConstants.BSON_KEY_CALCULATIONS_ID, new ObjectId(calculationsId))).into(matchingDocuments);
        } catch (Exception ex) {
            logger.error("findCalculations: mongo exception in find(): {}", ex.getMessage());
            return null;
        }

        if (!matchingDocuments.isEmpty()) {
            return matchingDocuments.get(0);
        } else {
            return null;
        }
    }

    @Override
    public MongoSaveResult savePvMetadata(PvMetadataDocument document) {

        logger.debug("saving PvMetadataDocument pvName: {}", document.getPvName());

        // Look up only by canonical pvName (not alias) to avoid mistakenly copying createdAt
        // from an unrelated document that merely has this pvName as one of its aliases.
        final List<PvMetadataDocument> exactMatches = new ArrayList<>();
        try {
            mongoCollectionPvMetadata.find(
                    eq(BsonConstants.BSON_KEY_PV_METADATA_PV_NAME, document.getPvName())
            ).into(exactMatches);
        } catch (Exception ex) {
            final String errorMsg = "MongoException looking up PvMetadataDocument by pvName: " + ex.getMessage();
            logger.error(errorMsg);
            return new MongoSaveResult(true, errorMsg, null, false);
        }
        final PvMetadataDocument existingDocument = exactMatches.isEmpty() ? null : exactMatches.get(0);

        try {
            if (existingDocument == null) {
                document.addCreationTime();
                InsertOneResult insertOneResult = mongoCollectionPvMetadata.insertOne(document);

                if (!insertOneResult.wasAcknowledged()) {
                    final String errorMsg = "insertOne failed for PvMetadataDocument, result not acknowledged";
                    logger.error(errorMsg);
                    return new MongoSaveResult(true, errorMsg, null, true);
                }
                if (insertOneResult.getInsertedId() == null) {
                    final String errorMsg = "PvMetadataDocument insert failed to return document id";
                    logger.error(errorMsg);
                    return new MongoSaveResult(true, errorMsg, null, true);
                }
                return new MongoSaveResult(false, "", document.getPvName(), true);

            } else {
                document.setCreatedAt(existingDocument.getCreatedAt());
                document.addUpdatedTime();

                final Bson filter = eq(BsonConstants.BSON_KEY_PV_METADATA_PV_NAME, document.getPvName());
                final ReplaceOptions replaceOptions = new ReplaceOptions().upsert(true);
                final UpdateResult result = mongoCollectionPvMetadata.replaceOne(filter, document, replaceOptions);

                if (!result.wasAcknowledged()) {
                    final String errorMsg = "replaceOne not acknowledged for PvMetadataDocument pvName: "
                            + document.getPvName();
                    logger.error(errorMsg);
                    return new MongoSaveResult(true, errorMsg, document.getPvName(), false);
                }
                return new MongoSaveResult(false, "", document.getPvName(), false);
            }
        } catch (MongoException ex) {
            final String errorMsg = "MongoException saving PvMetadataDocument: " + ex.getMessage();
            logger.error(errorMsg);
            return new MongoSaveResult(true, errorMsg, null, false);
        }
    }

    @Override
    public PvMetadataQueryResult executeQueryPvMetadata(QueryPvMetadataRequest request) {

        final List<Bson> filterList = new ArrayList<>();

        for (QueryPvMetadataRequest.QueryPvMetadataCriterion criterion : request.getCriteriaList()) {
            switch (criterion.getCriterionCase()) {

                case PVNAMECRITERION -> {
                    final QueryPvMetadataRequest.QueryPvMetadataCriterion.PvNameCriterion c =
                            criterion.getPvNameCriterion();
                    final List<Bson> nameFilters = new ArrayList<>();
                    if (!c.getExactList().isEmpty()) {
                        nameFilters.add(Filters.in(BsonConstants.BSON_KEY_PV_METADATA_PV_NAME, c.getExactList()));
                    }
                    for (String prefix : c.getPrefixList()) {
                        nameFilters.add(Filters.regex(BsonConstants.BSON_KEY_PV_METADATA_PV_NAME,
                                "^" + java.util.regex.Pattern.quote(prefix)));
                    }
                    for (String contains : c.getContainsList()) {
                        nameFilters.add(Filters.regex(BsonConstants.BSON_KEY_PV_METADATA_PV_NAME,
                                ".*" + java.util.regex.Pattern.quote(contains) + ".*"));
                    }
                    if (!nameFilters.isEmpty()) {
                        filterList.add(nameFilters.size() == 1 ? nameFilters.get(0) : or(nameFilters));
                    }
                }

                case ALIASESCRITERION -> {
                    final QueryPvMetadataRequest.QueryPvMetadataCriterion.AliasesCriterion c =
                            criterion.getAliasesCriterion();
                    final List<Bson> aliasFilters = new ArrayList<>();
                    if (!c.getExactList().isEmpty()) {
                        aliasFilters.add(Filters.in("aliases", c.getExactList()));
                    }
                    for (String prefix : c.getPrefixList()) {
                        aliasFilters.add(Filters.regex("aliases",
                                "^" + java.util.regex.Pattern.quote(prefix)));
                    }
                    for (String contains : c.getContainsList()) {
                        aliasFilters.add(Filters.regex("aliases",
                                ".*" + java.util.regex.Pattern.quote(contains) + ".*"));
                    }
                    if (!aliasFilters.isEmpty()) {
                        filterList.add(aliasFilters.size() == 1 ? aliasFilters.get(0) : or(aliasFilters));
                    }
                }

                case TAGSCRITERION -> {
                    final QueryPvMetadataRequest.QueryPvMetadataCriterion.TagsCriterion c =
                            criterion.getTagsCriterion();
                    filterList.add(Filters.in(BsonConstants.BSON_KEY_TAGS, c.getValuesList()));
                }

                case ATTRIBUTESCRITERION -> {
                    final QueryPvMetadataRequest.QueryPvMetadataCriterion.AttributesCriterion c =
                            criterion.getAttributesCriterion();
                    final String mapKey = BsonConstants.BSON_KEY_ATTRIBUTES + "." + c.getKey();
                    if (c.getValuesList().isEmpty()) {
                        filterList.add(Filters.exists(mapKey));
                    } else {
                        filterList.add(Filters.in(mapKey, c.getValuesList()));
                    }
                }

                default -> {
                    logger.error("executeQueryPvMetadata unexpected criterion case: {}", criterion.getCriterionCase());
                }
            }
        }

        final Bson queryFilter = filterList.isEmpty()
                ? Filters.exists(BsonConstants.BSON_KEY_PV_METADATA_PV_NAME)
                : and(filterList);

        final int limit = request.getLimit() > 0 ? request.getLimit() : 0;
        int skip = 0;
        if (!request.getPageToken().isBlank()) {
            try {
                skip = Integer.parseInt(
                        new String(Base64.getDecoder().decode(request.getPageToken()), StandardCharsets.UTF_8));
            } catch (Exception e) {
                logger.warn("invalid page token, ignoring: {}", request.getPageToken());
            }
        }

        var query = mongoCollectionPvMetadata
                .find(queryFilter)
                .sort(ascending(BsonConstants.BSON_KEY_PV_METADATA_PV_NAME));

        if (skip > 0) query = query.skip(skip);

        // Fetch limit+1 to detect whether a next page exists without an extra count query.
        final List<PvMetadataDocument> documents = new ArrayList<>();
        try {
            if (limit > 0) {
                query.limit(limit + 1).into(documents);
            } else {
                query.into(documents);
            }
        } catch (Exception ex) {
            logger.error("executeQueryPvMetadata: mongo exception: {}", ex.getMessage());
            return null;
        }

        // Determine next-page token: only produce one when the result set is full (has more).
        String nextPageToken = "";
        if (limit > 0 && documents.size() > limit) {
            documents.remove(documents.size() - 1); // trim the extra probe document
            final int nextSkip = skip + limit;
            nextPageToken = Base64.getEncoder().encodeToString(
                    Integer.toString(nextSkip).getBytes(StandardCharsets.UTF_8));
        }

        return new PvMetadataQueryResult(documents, nextPageToken);
    }

    @Override
    public PvMetadataDocument findPvMetadataByNameOrAlias(String pvNameOrAlias) {

        final List<PvMetadataDocument> matchingDocuments = new ArrayList<>();

        try {
            final Bson filter = or(
                    eq(BsonConstants.BSON_KEY_PV_METADATA_PV_NAME, pvNameOrAlias),
                    eq("aliases", pvNameOrAlias));
            mongoCollectionPvMetadata.find(filter).into(matchingDocuments);
        } catch (Exception ex) {
            final String errorMsg = "findPvMetadataByNameOrAlias: mongo exception: " + ex.getMessage();
            logger.error(errorMsg);
            throw new RuntimeException(errorMsg, ex);
        }

        return matchingDocuments.isEmpty() ? null : matchingDocuments.get(0);
    }

    @Override
    public MongoDeleteResult deletePvMetadata(String pvNameOrAlias) {

        final PvMetadataDocument existingDocument = findPvMetadataByNameOrAlias(pvNameOrAlias);
        if (existingDocument == null) {
            return new MongoDeleteResult(false, "", null);
        }

        final String canonicalPvName = existingDocument.getPvName();

        try {
            final Bson filter = eq(BsonConstants.BSON_KEY_PV_METADATA_PV_NAME, canonicalPvName);
            final DeleteResult result = mongoCollectionPvMetadata.deleteOne(filter);
            if (!result.wasAcknowledged()) {
                final String errorMsg = "deleteOne not acknowledged for pvName: " + canonicalPvName;
                logger.error(errorMsg);
                return new MongoDeleteResult(true, errorMsg, null);
            }
            return new MongoDeleteResult(false, "", canonicalPvName);
        } catch (MongoException ex) {
            final String errorMsg = "MongoException deleting PvMetadataDocument: " + ex.getMessage();
            logger.error(errorMsg);
            return new MongoDeleteResult(true, errorMsg, null);
        }
    }

    // =========================================================
    // Configuration CRUD
    // =========================================================

    private boolean activationsExistForConfiguration(String configurationName) {
        // Note: no exception catch here — callers must handle MongoException so that a transient
        // DB error is not silently treated as "no activations exist", which could allow an unsafe
        // category change or delete to proceed.
        final long count = mongoCollectionConfigurationActivations.countDocuments(
                eq(BsonConstants.BSON_KEY_ACTIVATION_CONFIGURATION_NAME, configurationName));
        return count > 0;
    }

    @Override
    public MongoSaveResult saveConfiguration(ConfigurationDocument document) {

        logger.debug("saving ConfigurationDocument configurationName: {}", document.getConfigurationName());

        final List<ConfigurationDocument> exactMatches = new ArrayList<>();
        try {
            mongoCollectionConfigurations.find(
                    eq(BsonConstants.BSON_KEY_CONFIGURATION_NAME, document.getConfigurationName())
            ).into(exactMatches);
        } catch (Exception ex) {
            final String errorMsg = "MongoException looking up ConfigurationDocument by configurationName: " + ex.getMessage();
            logger.error(errorMsg);
            return new MongoSaveResult(true, errorMsg, null, false);
        }
        final ConfigurationDocument existingDocument = exactMatches.isEmpty() ? null : exactMatches.get(0);

        // reject category change if activations exist
        if (existingDocument != null && !existingDocument.getCategory().equals(document.getCategory())) {
            final boolean hasActivations;
            try {
                hasActivations = activationsExistForConfiguration(document.getConfigurationName());
            } catch (MongoException ex) {
                final String errorMsg = "MongoException checking activations for configurationName '"
                        + document.getConfigurationName() + "': " + ex.getMessage();
                logger.error(errorMsg);
                return new MongoSaveResult(true, errorMsg, null, false);
            }
            if (hasActivations) {
                final String errorMsg = "cannot change category for configurationName '"
                        + document.getConfigurationName()
                        + "': existing activations must be deleted first";
                return new MongoSaveResult(true, errorMsg, null, false);
            }
        }
        // NOTE: the activation existence check above and the subsequent replaceOne are not atomic.
        // A concurrent saveConfigurationActivation could slip in between them on a multi-threaded
        // deployment. Full atomicity would require MongoDB transactions (replica set only). This is
        // an accepted limitation for v1; tracked for resolution when transaction support is added.

        try {
            if (existingDocument == null) {
                document.addCreationTime();
                final com.mongodb.client.result.InsertOneResult insertOneResult =
                        mongoCollectionConfigurations.insertOne(document);
                if (!insertOneResult.wasAcknowledged()) {
                    final String errorMsg = "insertOne failed for ConfigurationDocument, result not acknowledged";
                    logger.error(errorMsg);
                    return new MongoSaveResult(true, errorMsg, null, true);
                }
                if (insertOneResult.getInsertedId() == null) {
                    final String errorMsg = "ConfigurationDocument insert failed to return document id";
                    logger.error(errorMsg);
                    return new MongoSaveResult(true, errorMsg, null, true);
                }
                return new MongoSaveResult(false, "", document.getConfigurationName(), true);

            } else {
                document.setCreatedAt(existingDocument.getCreatedAt());
                document.addUpdatedTime();

                final Bson filter = eq(BsonConstants.BSON_KEY_CONFIGURATION_NAME, document.getConfigurationName());
                final ReplaceOptions replaceOptions = new ReplaceOptions().upsert(true);
                final UpdateResult result = mongoCollectionConfigurations.replaceOne(filter, document, replaceOptions);

                if (!result.wasAcknowledged()) {
                    final String errorMsg = "replaceOne not acknowledged for ConfigurationDocument configurationName: "
                            + document.getConfigurationName();
                    logger.error(errorMsg);
                    return new MongoSaveResult(true, errorMsg, document.getConfigurationName(), false);
                }
                return new MongoSaveResult(false, "", document.getConfigurationName(), false);
            }
        } catch (MongoException ex) {
            final String errorMsg = "MongoException saving ConfigurationDocument: " + ex.getMessage();
            logger.error(errorMsg);
            return new MongoSaveResult(true, errorMsg, null, false);
        }
    }

    @Override
    public ConfigurationDocument findConfigurationByName(String configurationName) {
        try {
            return mongoCollectionConfigurations.find(
                    eq(BsonConstants.BSON_KEY_CONFIGURATION_NAME, configurationName)).first();
        } catch (Exception ex) {
            final String errorMsg = "findConfigurationByName: mongo exception: " + ex.getMessage();
            logger.error(errorMsg);
            throw new RuntimeException(errorMsg, ex);
        }
    }

    @Override
    public ConfigurationQueryResult executeQueryConfigurations(QueryConfigurationsRequest request) {

        final List<Bson> filterList = new ArrayList<>();

        for (QueryConfigurationsRequest.QueryConfigurationsCriterion criterion : request.getCriteriaList()) {
            switch (criterion.getCriterionCase()) {

                case NAMECRITERION -> {
                    final var c = criterion.getNameCriterion();
                    final List<Bson> nameFilters = new ArrayList<>();
                    if (!c.getExactList().isEmpty()) {
                        nameFilters.add(Filters.in(BsonConstants.BSON_KEY_CONFIGURATION_NAME, c.getExactList()));
                    }
                    for (String prefix : c.getPrefixList()) {
                        nameFilters.add(Filters.regex(BsonConstants.BSON_KEY_CONFIGURATION_NAME,
                                "^" + java.util.regex.Pattern.quote(prefix)));
                    }
                    for (String contains : c.getContainsList()) {
                        nameFilters.add(Filters.regex(BsonConstants.BSON_KEY_CONFIGURATION_NAME,
                                ".*" + java.util.regex.Pattern.quote(contains) + ".*"));
                    }
                    if (!nameFilters.isEmpty()) {
                        filterList.add(nameFilters.size() == 1 ? nameFilters.get(0) : or(nameFilters));
                    }
                }

                case CATEGORYCRITERION -> {
                    filterList.add(Filters.in(BsonConstants.BSON_KEY_CONFIGURATION_CATEGORY,
                            criterion.getCategoryCriterion().getValuesList()));
                }

                case PARENTCRITERION -> {
                    filterList.add(Filters.in(BsonConstants.BSON_KEY_CONFIGURATION_PARENT_NAME,
                            criterion.getParentCriterion().getValuesList()));
                }

                case TAGSCRITERION -> {
                    filterList.add(Filters.in(BsonConstants.BSON_KEY_TAGS,
                            criterion.getTagsCriterion().getValuesList()));
                }

                case ATTRIBUTESCRITERION -> {
                    final var c = criterion.getAttributesCriterion();
                    final String mapKey = BsonConstants.BSON_KEY_ATTRIBUTES + "." + c.getKey();
                    if (c.getValuesList().isEmpty()) {
                        filterList.add(Filters.exists(mapKey));
                    } else {
                        filterList.add(Filters.in(mapKey, c.getValuesList()));
                    }
                }

                default -> {
                    logger.error("executeQueryConfigurations unexpected criterion case: {}",
                            criterion.getCriterionCase());
                }
            }
        }

        final Bson queryFilter = filterList.isEmpty()
                ? Filters.exists(BsonConstants.BSON_KEY_CONFIGURATION_NAME)
                : and(filterList);

        final int limit = request.getLimit() > 0 ? request.getLimit() : 100;
        int skip = 0;
        if (!request.getPageToken().isBlank()) {
            try {
                skip = Integer.parseInt(
                        new String(Base64.getDecoder().decode(request.getPageToken()), java.nio.charset.StandardCharsets.UTF_8));
            } catch (Exception e) {
                logger.warn("invalid page token, ignoring: {}", request.getPageToken());
            }
        }

        var query = mongoCollectionConfigurations
                .find(queryFilter)
                .sort(ascending(BsonConstants.BSON_KEY_CONFIGURATION_NAME));

        if (skip > 0) query = query.skip(skip);

        final List<ConfigurationDocument> documents = new ArrayList<>();
        try {
            query.limit(limit + 1).into(documents);
        } catch (Exception ex) {
            logger.error("executeQueryConfigurations: mongo exception: {}", ex.getMessage());
            return null;
        }

        String nextPageToken = "";
        if (documents.size() > limit) {
            documents.remove(documents.size() - 1);
            final int nextSkip = skip + limit;
            nextPageToken = Base64.getEncoder().encodeToString(
                    Integer.toString(nextSkip).getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }

        return new ConfigurationQueryResult(documents, nextPageToken);
    }

    @Override
    public MongoDeleteResult deleteConfiguration(String configurationName) {

        final boolean hasActivations;
        try {
            hasActivations = activationsExistForConfiguration(configurationName);
        } catch (MongoException ex) {
            final String errorMsg = "MongoException checking activations for configurationName '"
                    + configurationName + "': " + ex.getMessage();
            logger.error(errorMsg);
            return new MongoDeleteResult(true, errorMsg, null);
        }
        if (hasActivations) {
            final String errorMsg = "cannot delete configurationName '" + configurationName
                    + "': existing activations must be deleted first";
            return new MongoDeleteResult(true, errorMsg, null);
        }

        try {
            final Bson filter = eq(BsonConstants.BSON_KEY_CONFIGURATION_NAME, configurationName);
            final DeleteResult result = mongoCollectionConfigurations.deleteOne(filter);
            if (!result.wasAcknowledged()) {
                final String errorMsg = "deleteOne not acknowledged for configurationName: " + configurationName;
                logger.error(errorMsg);
                return new MongoDeleteResult(true, errorMsg, null);
            }
            if (result.getDeletedCount() == 0) {
                // not found — signal via null deletedPvName
                return new MongoDeleteResult(false, "", null);
            }
            return new MongoDeleteResult(false, "", configurationName);
        } catch (MongoException ex) {
            final String errorMsg = "MongoException deleting ConfigurationDocument: " + ex.getMessage();
            logger.error(errorMsg);
            return new MongoDeleteResult(true, errorMsg, null);
        }
    }

    // ---- Configuration Activation methods ----

    /**
     * Checks if an overlap exists for the given activation parameters.
     * Overlap rules: same configurationName OR same internalCategory, time intervals overlap.
     * excludeClientActivationId: exclude this record from the overlap check (used for updates).
     *
     * Note: no exception catch here — callers must handle MongoException so that a transient DB
     * error is not silently treated as "no overlap", which would allow overlapping activations to
     * be inserted in violation of the API contract.
     *
     * Also note: the overlap check and the subsequent insert/replace are not atomic operations.
     * Concurrent saves on multiple worker threads could both pass this check before either write
     * completes, resulting in overlapping activations being persisted. Full atomicity would require
     * MongoDB transactions (replica set only). This is an accepted limitation for v1; tracked for
     * resolution when transaction support is added.
     */
    private boolean overlapExists(String configurationName, String internalCategory,
                                   Instant startTime, Instant endTime,
                                   String excludeClientActivationId) {
        // endTime filter for the candidate: candidate.endTime > startTime OR candidate.endTime absent
        final Bson candidateEndTimeFilter = or(
                exists(BsonConstants.BSON_KEY_ACTIVATION_END_TIME, false),
                gt(BsonConstants.BSON_KEY_ACTIVATION_END_TIME, startTime)
        );

        // startTime filter for the candidate: candidate.startTime < endTime (skip if endTime null — always overlaps)
        final Bson candidateStartTimeFilter = endTime != null
                ? lt(BsonConstants.BSON_KEY_ACTIVATION_START_TIME, endTime)
                : null;

        final Bson excludeFilter = (excludeClientActivationId != null && !excludeClientActivationId.isBlank())
                ? ne(BsonConstants.BSON_KEY_ACTIVATION_CLIENT_ID, excludeClientActivationId)
                : null;

        // Query 1: same configurationName overlap
        final List<Bson> q1Filters = new ArrayList<>();
        q1Filters.add(eq(BsonConstants.BSON_KEY_ACTIVATION_CONFIGURATION_NAME, configurationName));
        if (excludeFilter != null) q1Filters.add(excludeFilter);
        if (candidateStartTimeFilter != null) q1Filters.add(candidateStartTimeFilter);
        q1Filters.add(candidateEndTimeFilter);
        final long count1 = mongoCollectionConfigurationActivations.countDocuments(and(q1Filters));
        if (count1 > 0) return true;

        // Query 2: same category overlap
        final List<Bson> q2Filters = new ArrayList<>();
        q2Filters.add(eq(BsonConstants.BSON_KEY_ACTIVATION_INTERNAL_CATEGORY, internalCategory));
        if (excludeFilter != null) q2Filters.add(excludeFilter);
        if (candidateStartTimeFilter != null) q2Filters.add(candidateStartTimeFilter);
        q2Filters.add(candidateEndTimeFilter);
        final long count2 = mongoCollectionConfigurationActivations.countDocuments(and(q2Filters));
        return count2 > 0;
    }

    @Override
    public MongoSaveResult saveConfigurationActivation(ConfigurationActivationDocument document) {
        try {
            // look up Configuration to get internalCategory
            final ConfigurationDocument config = findConfigurationByName(document.getConfigurationName());
            if (config == null) {
                return new MongoSaveResult(true,
                        "no Configuration found for configurationName: '" + document.getConfigurationName() + "'",
                        null, false);
            }
            document.setInternalCategory(config.getCategory());

            // determine excludeId for overlap check (non-blank clientActivationId = potential update)
            final String excludeId = (document.getClientActivationId() != null
                    && !document.getClientActivationId().isBlank())
                    ? document.getClientActivationId() : null;

            // check for overlap
            final boolean overlap;
            try {
                overlap = overlapExists(document.getConfigurationName(), document.getInternalCategory(),
                        document.getStartTime(), document.getEndTime(), excludeId);
            } catch (MongoException ex) {
                final String errorMsg = "MongoException checking activation overlap for configurationName '"
                        + document.getConfigurationName() + "': " + ex.getMessage();
                logger.error(errorMsg);
                return new MongoSaveResult(true, errorMsg, null, false);
            }
            if (overlap) {
                return new MongoSaveResult(true,
                        "overlapping activation exists for configurationName '"
                                + document.getConfigurationName() + "' or category '"
                                + document.getInternalCategory() + "'",
                        null, false);
            }

            // generate server-side UUID if clientActivationId not supplied
            if (document.getClientActivationId() == null || document.getClientActivationId().isBlank()) {
                document.setClientActivationId(UUID.randomUUID().toString());
            }

            // find existing record by clientActivationId
            final ConfigurationActivationDocument existing = findConfigurationActivationById(
                    document.getClientActivationId());

            if (existing == null) {
                // new record
                document.addCreationTime();
                mongoCollectionConfigurationActivations.insertOne(document);
            } else {
                // update existing: preserve createdAt, set updatedAt
                document.setCreatedAt(existing.getCreatedAt());
                document.addUpdatedTime();
                final Bson filter = eq(BsonConstants.BSON_KEY_ACTIVATION_CLIENT_ID,
                        document.getClientActivationId());
                final ReplaceOptions replaceOptions = new ReplaceOptions().upsert(true);
                mongoCollectionConfigurationActivations.replaceOne(filter, document, replaceOptions);
            }

            return new MongoSaveResult(false, "", document.getClientActivationId(), existing == null);

        } catch (MongoException ex) {
            final String errorMsg = "MongoException saving ConfigurationActivationDocument: " + ex.getMessage();
            logger.error(errorMsg);
            return new MongoSaveResult(true, errorMsg, null, false);
        }
    }

    @Override
    public ConfigurationActivationDocument findConfigurationActivationById(String clientActivationId) {
        // Note: MongoException is not caught here — callers must handle it so that a backend failure
        // is surfaced as RESULT_STATUS_ERROR rather than silently reported as "not found".
        return mongoCollectionConfigurationActivations.find(
                eq(BsonConstants.BSON_KEY_ACTIVATION_CLIENT_ID, clientActivationId)).first();
    }

    @Override
    public ConfigurationActivationDocument findConfigurationActivationByCompositeKey(
            String configurationName, Instant startTime) {
        // Note: MongoException is not caught here — callers must handle it so that a backend failure
        // is surfaced as RESULT_STATUS_ERROR rather than silently reported as "not found".
        return mongoCollectionConfigurationActivations.find(
                and(
                        eq(BsonConstants.BSON_KEY_ACTIVATION_CONFIGURATION_NAME, configurationName),
                        eq(BsonConstants.BSON_KEY_ACTIVATION_START_TIME, startTime)
                )).first();
    }

    @Override
    public ConfigurationActivationQueryResult executeQueryConfigurationActivations(
            QueryConfigurationActivationsRequest request) {

        final List<Bson> filterList = new ArrayList<>();

        for (var criterion : request.getCriteriaList()) {
            switch (criterion.getCriterionCase()) {
                case TIMESTAMPCRITERION -> {
                    final Instant ts = com.ospreydcs.dp.service.common.protobuf.TimestampUtility
                            .instantFromTimestamp(criterion.getTimestampCriterion().getTimestamp());
                    filterList.add(and(
                            lte(BsonConstants.BSON_KEY_ACTIVATION_START_TIME, ts),
                            or(
                                    exists(BsonConstants.BSON_KEY_ACTIVATION_END_TIME, false),
                                    gt(BsonConstants.BSON_KEY_ACTIVATION_END_TIME, ts)
                            )
                    ));
                }
                case TIMERANGECRITERION -> {
                    final Instant rangeStart = com.ospreydcs.dp.service.common.protobuf.TimestampUtility
                            .instantFromTimestamp(criterion.getTimeRangeCriterion().getStartTime());
                    final Instant rangeEnd = com.ospreydcs.dp.service.common.protobuf.TimestampUtility
                            .instantFromTimestamp(criterion.getTimeRangeCriterion().getEndTime());
                    filterList.add(and(
                            lt(BsonConstants.BSON_KEY_ACTIVATION_START_TIME, rangeEnd),
                            or(
                                    exists(BsonConstants.BSON_KEY_ACTIVATION_END_TIME, false),
                                    gt(BsonConstants.BSON_KEY_ACTIVATION_END_TIME, rangeStart)
                            )
                    ));
                }
                case CONFIGURATIONNAMECRITERION -> {
                    filterList.add(in(BsonConstants.BSON_KEY_ACTIVATION_CONFIGURATION_NAME,
                            criterion.getConfigurationNameCriterion().getValuesList()));
                }
                case CLIENTACTIVATIONIDCRITERION -> {
                    filterList.add(in(BsonConstants.BSON_KEY_ACTIVATION_CLIENT_ID,
                            criterion.getClientActivationIdCriterion().getValuesList()));
                }
                case CATEGORYCRITERION -> {
                    filterList.add(in(BsonConstants.BSON_KEY_ACTIVATION_INTERNAL_CATEGORY,
                            criterion.getCategoryCriterion().getValuesList()));
                }
                case TAGSCRITERION -> {
                    filterList.add(in(BsonConstants.BSON_KEY_TAGS,
                            criterion.getTagsCriterion().getValuesList()));
                }
                case ATTRIBUTESCRITERION -> {
                    final var ac = criterion.getAttributesCriterion();
                    if (ac.getValuesList().isEmpty()) {
                        filterList.add(exists("attributes." + ac.getKey()));
                    } else {
                        filterList.add(in("attributes." + ac.getKey(), ac.getValuesList()));
                    }
                }
                default -> {
                    // unknown criterion — ignored
                }
            }
        }

        final Bson filter = filterList.isEmpty() ? new org.bson.Document() : and(filterList);

        // pagination
        final String pageToken = request.getPageToken();
        final int skip;
        try {
            skip = (pageToken == null || pageToken.isEmpty()) ? 0
                    : Integer.parseInt(new String(Base64.getDecoder().decode(pageToken), StandardCharsets.UTF_8));
        } catch (IllegalArgumentException ex) {
            logger.error("executeQueryConfigurationActivations: invalid pageToken: {}", ex.getMessage());
            return null;
        }
        int limit = request.getLimit();
        if (limit <= 0) limit = 100;

        final List<ConfigurationActivationDocument> documents = new ArrayList<>();
        try {
            mongoCollectionConfigurationActivations.find(filter)
                    .sort(ascending(BsonConstants.BSON_KEY_ACTIVATION_START_TIME))
                    .skip(skip)
                    .limit(limit + 1)
                    .into(documents);
        } catch (MongoException ex) {
            logger.error("executeQueryConfigurationActivations: mongo exception: {}", ex.getMessage());
            return null;
        }

        String nextPageToken = "";
        if (documents.size() > limit) {
            documents.remove(documents.size() - 1);
            final int nextSkip = skip + limit;
            nextPageToken = Base64.getEncoder().encodeToString(
                    Integer.toString(nextSkip).getBytes(StandardCharsets.UTF_8));
        }

        return new ConfigurationActivationQueryResult(documents, nextPageToken);
    }

    @Override
    public MongoDeleteResult deleteConfigurationActivation(String clientActivationId) {
        try {
            final Bson filter = eq(BsonConstants.BSON_KEY_ACTIVATION_CLIENT_ID, clientActivationId);
            final DeleteResult result = mongoCollectionConfigurationActivations.deleteOne(filter);
            if (!result.wasAcknowledged()) {
                final String errorMsg = "deleteOne not acknowledged for clientActivationId: " + clientActivationId;
                logger.error(errorMsg);
                return new MongoDeleteResult(true, errorMsg, null);
            }
            if (result.getDeletedCount() == 0) {
                return new MongoDeleteResult(false, "", null);
            }
            return new MongoDeleteResult(false, "", clientActivationId);
        } catch (MongoException ex) {
            final String errorMsg = "MongoException deleting ConfigurationActivationDocument by id: " + ex.getMessage();
            logger.error(errorMsg);
            return new MongoDeleteResult(true, errorMsg, null);
        }
    }

    @Override
    public MongoDeleteResult deleteConfigurationActivationByCompositeKey(
            String configurationName, Instant startTime) {
        try {
            final Bson filter = and(
                    eq(BsonConstants.BSON_KEY_ACTIVATION_CONFIGURATION_NAME, configurationName),
                    eq(BsonConstants.BSON_KEY_ACTIVATION_START_TIME, startTime)
            );
            // Fetch the document first so we can return its actual clientActivationId in the response.
            final ConfigurationActivationDocument existing =
                    mongoCollectionConfigurationActivations.find(filter).first();
            if (existing == null) {
                return new MongoDeleteResult(false, "", null);
            }
            final DeleteResult result = mongoCollectionConfigurationActivations.deleteOne(filter);
            if (!result.wasAcknowledged()) {
                final String errorMsg = "deleteOne not acknowledged for compositeKey configurationName: "
                        + configurationName;
                logger.error(errorMsg);
                return new MongoDeleteResult(true, errorMsg, null);
            }
            return new MongoDeleteResult(false, "", existing.getClientActivationId());
        } catch (MongoException ex) {
            final String errorMsg = "MongoException deleting ConfigurationActivationDocument by compositeKey: "
                    + ex.getMessage();
            logger.error(errorMsg);
            return new MongoDeleteResult(true, errorMsg, null);
        }
    }

    @Override
    public ConfigurationActivationQueryResult getActiveConfigurations(Instant timestamp) {
        try {
            final Bson filter = and(
                    lte(BsonConstants.BSON_KEY_ACTIVATION_START_TIME, timestamp),
                    or(
                            exists(BsonConstants.BSON_KEY_ACTIVATION_END_TIME, false),
                            gt(BsonConstants.BSON_KEY_ACTIVATION_END_TIME, timestamp)
                    )
            );

            final List<ConfigurationActivationDocument> documents = new ArrayList<>();
            mongoCollectionConfigurationActivations.find(filter)
                    .sort(ascending(BsonConstants.BSON_KEY_ACTIVATION_START_TIME))
                    .into(documents);

            return new ConfigurationActivationQueryResult(documents, "");
        } catch (MongoException ex) {
            logger.error("getActiveConfigurations: mongo exception: {}", ex.getMessage());
            return null;
        }
    }

}
