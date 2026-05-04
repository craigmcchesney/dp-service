package com.ospreydcs.dp.service.integration.annotation;

import com.ospreydcs.dp.grpc.v1.annotation.QueryConfigurationActivationsRequest;
import com.ospreydcs.dp.grpc.v1.annotation.QueryConfigurationsRequest;
import com.ospreydcs.dp.grpc.v1.common.Attribute;
import com.ospreydcs.dp.grpc.v1.common.Configuration;
import com.ospreydcs.dp.grpc.v1.common.ConfigurationActivation;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.service.annotation.AnnotationTestBase;
import com.ospreydcs.dp.service.common.bson.configuration.ConfigurationActivationDocument;
import com.ospreydcs.dp.service.common.bson.configuration.ConfigurationDocument;
import com.ospreydcs.dp.service.common.protobuf.TimestampUtility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.Assert.*;

public class ConfigurationIT extends AnnotationIntegrationTestIntermediate {

    // =========================================================================
    // test timestamps
    // =========================================================================

    // t0 = 2024-01-01T00:00:00Z
    private static final long T0_SECONDS = 1704067200L;
    private static final long T0_NANOS = 0L;
    // t1 = 2024-02-01T00:00:00Z
    private static final long T1_SECONDS = 1706745600L;
    private static final long T1_NANOS = 0L;
    // t2 = 2024-03-01T00:00:00Z
    private static final long T2_SECONDS = 1709251200L;
    private static final long T2_NANOS = 0L;
    // t3 = 2024-04-01T00:00:00Z
    private static final long T3_SECONDS = 1711929600L;
    private static final long T3_NANOS = 0L;
    // t4 = 2024-05-01T00:00:00Z
    private static final long T4_SECONDS = 1714521600L;
    private static final long T4_NANOS = 0L;
    // t5 = 2024-06-01T00:00:00Z (in the middle of t1-t3 range, after t2)
    private static final long T5_SECONDS = 1717200000L;
    private static final long T5_NANOS = 0L;

    private static Timestamp ts(long seconds, long nanos) {
        return Timestamp.newBuilder().setEpochSeconds(seconds).setNanoseconds(nanos).build();
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() {
        super.tearDown();
    }

    // =========================================================================
    // saveConfiguration tests
    // =========================================================================

    @Test
    public void testSaveConfigurationRejectBlankName() {
        final AnnotationTestBase.SaveConfigurationParams params = new AnnotationTestBase.SaveConfigurationParams(
                "", "category1", null, null, null, null, null);
        annotationServiceWrapper.sendAndVerifySaveConfiguration(
                params, true, "SaveConfigurationRequest.configurationName must be specified");
    }

    @Test
    public void testSaveConfigurationRejectBlankCategory() {
        final AnnotationTestBase.SaveConfigurationParams params = new AnnotationTestBase.SaveConfigurationParams(
                "config-A", "", null, null, null, null, null);
        annotationServiceWrapper.sendAndVerifySaveConfiguration(
                params, true, "SaveConfigurationRequest.category must be specified");
    }

    @Test
    public void testSaveConfigurationRejectDuplicateAttributeKeys() {
        final List<Attribute> attributes = List.of(
                Attribute.newBuilder().setName("key1").setValue("v1").build(),
                Attribute.newBuilder().setName("key1").setValue("v2").build()
        );
        final AnnotationTestBase.SaveConfigurationParams params = new AnnotationTestBase.SaveConfigurationParams(
                "config-A", "cat1", null, null, null, attributes, null);
        annotationServiceWrapper.sendAndVerifySaveConfiguration(
                params, true, "duplicate key");
    }

    @Test
    public void testSaveConfigurationCreateAndUpdate() {
        // create a new Configuration record
        final List<String> tags = List.of("TEST", "Unit", "test"); // should be normalized to lowercase unique sorted
        final List<Attribute> attributes = List.of(
                Attribute.newBuilder().setName("system").setValue("vacuum").build(),
                Attribute.newBuilder().setName("sector").setValue("01").build()
        );
        final AnnotationTestBase.SaveConfigurationParams createParams = new AnnotationTestBase.SaveConfigurationParams(
                "config-A", "category1", "A test configuration", null, tags, attributes, "craigmcc");

        final String configName = annotationServiceWrapper.sendAndVerifySaveConfiguration(
                createParams, false, null);
        assertNotNull(configName);
        assertEquals("config-A", configName);

        // verify document in database: createdAt set, updatedAt null, tags normalized
        final ConfigurationDocument doc = mongoClient.findConfiguration("config-A");
        assertNotNull(doc);
        assertNotNull(doc.getCreatedAt());
        assertNull(doc.getUpdatedAt());
        assertEquals("config-A", doc.getConfigurationName());
        assertEquals("category1", doc.getCategory());
        // tags should be normalized to lowercase unique sorted list
        final List<String> normalizedTags = doc.getTags();
        assertNotNull(normalizedTags);
        assertEquals(List.of("test", "unit"), normalizedTags);

        // update same Configuration record with different description
        final AnnotationTestBase.SaveConfigurationParams updateParams = new AnnotationTestBase.SaveConfigurationParams(
                "config-A", "category1", "Updated description", null, tags, attributes, "allenck");

        final String updatedName = annotationServiceWrapper.sendAndVerifySaveConfiguration(
                updateParams, false, null);
        assertEquals("config-A", updatedName);

        // verify update: createdAt preserved, updatedAt set
        final ConfigurationDocument updatedDoc = mongoClient.findConfiguration("config-A");
        assertNotNull(updatedDoc);
        assertNotNull(updatedDoc.getCreatedAt());
        assertNotNull(updatedDoc.getUpdatedAt());
        assertEquals("Updated description", updatedDoc.getDescription());
        assertEquals("allenck", updatedDoc.getModifiedBy());
        assertEquals(doc.getCreatedAt(), updatedDoc.getCreatedAt());
    }

    @Test
    public void testSaveConfigurationWithParent() {
        // save parent configuration
        final AnnotationTestBase.SaveConfigurationParams parentParams = new AnnotationTestBase.SaveConfigurationParams(
                "parent-config", "category1", "Parent configuration", null, null, null, "craigmcc");
        annotationServiceWrapper.sendAndVerifySaveConfiguration(parentParams, false, null);

        // save child configuration referencing parent
        final AnnotationTestBase.SaveConfigurationParams childParams = new AnnotationTestBase.SaveConfigurationParams(
                "child-config", "category1", "Child configuration", "parent-config", null, null, "craigmcc");
        final String childName = annotationServiceWrapper.sendAndVerifySaveConfiguration(childParams, false, null);
        assertEquals("child-config", childName);

        final ConfigurationDocument childDoc = mongoClient.findConfiguration("child-config");
        assertNotNull(childDoc);
        assertEquals("parent-config", childDoc.getParentConfigurationName());
    }

    // =========================================================================
    // getConfiguration tests
    // =========================================================================

    @Test
    public void testGetConfigurationSuccess() {
        // save configuration
        final AnnotationTestBase.SaveConfigurationParams params = new AnnotationTestBase.SaveConfigurationParams(
                "get-config-1", "category1", "Get test config", null, null, null, "craigmcc");
        annotationServiceWrapper.sendAndVerifySaveConfiguration(params, false, null);

        // get by name
        final Configuration result = annotationServiceWrapper.sendAndVerifyGetConfiguration(
                "get-config-1", false, null);
        assertNotNull(result);
        assertEquals("get-config-1", result.getConfigurationName());
        assertEquals("category1", result.getCategory());
        assertEquals("Get test config", result.getDescription());
    }

    @Test
    public void testGetConfigurationRejectBlankName() {
        annotationServiceWrapper.sendAndVerifyGetConfiguration(
                "", true, "GetConfigurationRequest.configurationName must be specified");
    }

    @Test
    public void testGetConfigurationRejectNotFound() {
        annotationServiceWrapper.sendAndVerifyGetConfiguration(
                "no-such-config", true, "no Configuration record found for: no-such-config");
    }

    // =========================================================================
    // queryConfigurations tests
    // =========================================================================

    private void createQueryConfigurationsScenario() {
        // Create configurations for query tests
        // config-Q1: category=cat1, tags=[tag1, tag2], attrs={system=vacuum}
        final AnnotationTestBase.SaveConfigurationParams q1 = new AnnotationTestBase.SaveConfigurationParams(
                "config-Q1", "cat1", "Query config 1", null,
                List.of("tag1", "tag2"),
                List.of(Attribute.newBuilder().setName("system").setValue("vacuum").build()),
                "craigmcc");
        annotationServiceWrapper.sendAndVerifySaveConfiguration(q1, false, null);

        // config-Q2: category=cat1, tags=[tag2, tag3], attrs={system=cryo}
        final AnnotationTestBase.SaveConfigurationParams q2 = new AnnotationTestBase.SaveConfigurationParams(
                "config-Q2", "cat1", "Query config 2", null,
                List.of("tag2", "tag3"),
                List.of(Attribute.newBuilder().setName("system").setValue("cryo").build()),
                "allenck");
        annotationServiceWrapper.sendAndVerifySaveConfiguration(q2, false, null);

        // config-Q3: category=cat2, tags=[tag3], attrs={system=vacuum, sector=01}
        final AnnotationTestBase.SaveConfigurationParams q3 = new AnnotationTestBase.SaveConfigurationParams(
                "config-Q3", "cat2", "Query config 3", "config-Q1",
                List.of("tag3"),
                List.of(
                        Attribute.newBuilder().setName("system").setValue("vacuum").build(),
                        Attribute.newBuilder().setName("sector").setValue("01").build()),
                "craigmcc");
        annotationServiceWrapper.sendAndVerifySaveConfiguration(q3, false, null);

        // config-prefix1: category=cat2
        final AnnotationTestBase.SaveConfigurationParams qp1 = new AnnotationTestBase.SaveConfigurationParams(
                "prefix-config-A", "cat2", "Prefix config A", null, null, null, "craigmcc");
        annotationServiceWrapper.sendAndVerifySaveConfiguration(qp1, false, null);

        // config-prefix2: category=cat3
        final AnnotationTestBase.SaveConfigurationParams qp2 = new AnnotationTestBase.SaveConfigurationParams(
                "prefix-config-B", "cat3", "Prefix config B", null, null, null, "craigmcc");
        annotationServiceWrapper.sendAndVerifySaveConfiguration(qp2, false, null);
    }

    @Test
    public void testQueryConfigurationsRejectEmptyCriteria() {
        annotationServiceWrapper.sendAndVerifyQueryConfigurations(
                List.of(), 0, null,
                true, "QueryConfigurationsRequest.criteria list must not be empty", 0);
    }

    @Test
    public void testQueryConfigurationsByNameExact() {
        createQueryConfigurationsScenario();
        final QueryConfigurationsRequest.QueryConfigurationsCriterion criterion =
                QueryConfigurationsRequest.QueryConfigurationsCriterion.newBuilder()
                        .setNameCriterion(
                                QueryConfigurationsRequest.QueryConfigurationsCriterion.NameCriterion.newBuilder()
                                        .addExact("config-Q1")
                                        .build())
                        .build();
        final List<Configuration> results = annotationServiceWrapper.sendAndVerifyQueryConfigurations(
                List.of(criterion), 0, null, false, null, 1);
        assertEquals("config-Q1", results.get(0).getConfigurationName());
    }

    @Test
    public void testQueryConfigurationsByNamePrefix() {
        createQueryConfigurationsScenario();
        final QueryConfigurationsRequest.QueryConfigurationsCriterion criterion =
                QueryConfigurationsRequest.QueryConfigurationsCriterion.newBuilder()
                        .setNameCriterion(
                                QueryConfigurationsRequest.QueryConfigurationsCriterion.NameCriterion.newBuilder()
                                        .addPrefix("prefix-")
                                        .build())
                        .build();
        final List<Configuration> results = annotationServiceWrapper.sendAndVerifyQueryConfigurations(
                List.of(criterion), 0, null, false, null, 2);
        assertTrue(results.stream().anyMatch(c -> c.getConfigurationName().equals("prefix-config-A")));
        assertTrue(results.stream().anyMatch(c -> c.getConfigurationName().equals("prefix-config-B")));
    }

    @Test
    public void testQueryConfigurationsByNameContains() {
        createQueryConfigurationsScenario();
        final QueryConfigurationsRequest.QueryConfigurationsCriterion criterion =
                QueryConfigurationsRequest.QueryConfigurationsCriterion.newBuilder()
                        .setNameCriterion(
                                QueryConfigurationsRequest.QueryConfigurationsCriterion.NameCriterion.newBuilder()
                                        .addContains("Q")
                                        .build())
                        .build();
        final List<Configuration> results = annotationServiceWrapper.sendAndVerifyQueryConfigurations(
                List.of(criterion), 0, null, false, null, 3);
    }

    @Test
    public void testQueryConfigurationsByCategory() {
        createQueryConfigurationsScenario();
        final QueryConfigurationsRequest.QueryConfigurationsCriterion criterion =
                QueryConfigurationsRequest.QueryConfigurationsCriterion.newBuilder()
                        .setCategoryCriterion(
                                QueryConfigurationsRequest.QueryConfigurationsCriterion.CategoryCriterion.newBuilder()
                                        .addValues("cat1")
                                        .build())
                        .build();
        final List<Configuration> results = annotationServiceWrapper.sendAndVerifyQueryConfigurations(
                List.of(criterion), 0, null, false, null, 2);
        assertTrue(results.stream().allMatch(c -> c.getCategory().equals("cat1")));
    }

    @Test
    public void testQueryConfigurationsByParent() {
        createQueryConfigurationsScenario();
        final QueryConfigurationsRequest.QueryConfigurationsCriterion criterion =
                QueryConfigurationsRequest.QueryConfigurationsCriterion.newBuilder()
                        .setParentCriterion(
                                QueryConfigurationsRequest.QueryConfigurationsCriterion.ParentCriterion.newBuilder()
                                        .addValues("config-Q1")
                                        .build())
                        .build();
        final List<Configuration> results = annotationServiceWrapper.sendAndVerifyQueryConfigurations(
                List.of(criterion), 0, null, false, null, 1);
        assertEquals("config-Q3", results.get(0).getConfigurationName());
    }

    @Test
    public void testQueryConfigurationsByTags() {
        createQueryConfigurationsScenario();
        final QueryConfigurationsRequest.QueryConfigurationsCriterion criterion =
                QueryConfigurationsRequest.QueryConfigurationsCriterion.newBuilder()
                        .setTagsCriterion(
                                QueryConfigurationsRequest.QueryConfigurationsCriterion.TagsCriterion.newBuilder()
                                        .addValues("tag2")
                                        .build())
                        .build();
        // tag2 is on config-Q1 and config-Q2 (after normalization to lowercase)
        final List<Configuration> results = annotationServiceWrapper.sendAndVerifyQueryConfigurations(
                List.of(criterion), 0, null, false, null, 2);
    }

    @Test
    public void testQueryConfigurationsByAttributeKeyOnly() {
        createQueryConfigurationsScenario();
        final QueryConfigurationsRequest.QueryConfigurationsCriterion criterion =
                QueryConfigurationsRequest.QueryConfigurationsCriterion.newBuilder()
                        .setAttributesCriterion(
                                QueryConfigurationsRequest.QueryConfigurationsCriterion.AttributesCriterion.newBuilder()
                                        .setKey("sector")
                                        .build())
                        .build();
        // only config-Q3 has sector attribute
        final List<Configuration> results = annotationServiceWrapper.sendAndVerifyQueryConfigurations(
                List.of(criterion), 0, null, false, null, 1);
        assertEquals("config-Q3", results.get(0).getConfigurationName());
    }

    @Test
    public void testQueryConfigurationsByAttributeKeyAndValue() {
        createQueryConfigurationsScenario();
        final QueryConfigurationsRequest.QueryConfigurationsCriterion criterion =
                QueryConfigurationsRequest.QueryConfigurationsCriterion.newBuilder()
                        .setAttributesCriterion(
                                QueryConfigurationsRequest.QueryConfigurationsCriterion.AttributesCriterion.newBuilder()
                                        .setKey("system")
                                        .addValues("vacuum")
                                        .build())
                        .build();
        // config-Q1 and config-Q3 have system=vacuum
        final List<Configuration> results = annotationServiceWrapper.sendAndVerifyQueryConfigurations(
                List.of(criterion), 0, null, false, null, 2);
    }

    @Test
    public void testQueryConfigurationsEmptyResult() {
        createQueryConfigurationsScenario();
        final QueryConfigurationsRequest.QueryConfigurationsCriterion criterion =
                QueryConfigurationsRequest.QueryConfigurationsCriterion.newBuilder()
                        .setNameCriterion(
                                QueryConfigurationsRequest.QueryConfigurationsCriterion.NameCriterion.newBuilder()
                                        .addExact("no-such-config")
                                        .build())
                        .build();
        annotationServiceWrapper.sendAndVerifyQueryConfigurations(
                List.of(criterion), 0, null, false, null, 0);
    }

    @Test
    public void testQueryConfigurationsPagination() {
        createQueryConfigurationsScenario();
        // All 5 configs: config-Q1, config-Q2, config-Q3, prefix-config-A, prefix-config-B
        final QueryConfigurationsRequest.QueryConfigurationsCriterion criterion =
                QueryConfigurationsRequest.QueryConfigurationsCriterion.newBuilder()
                        .setNameCriterion(
                                QueryConfigurationsRequest.QueryConfigurationsCriterion.NameCriterion.newBuilder()
                                        .addContains("config")
                                        .build())
                        .build();

        // page 1: limit 3 — use wrapper but capture nextPageToken separately
        // sendAndVerifyQueryConfigurations only returns the list, so we send this page via a direct observer call
        // We use the wrapper with limit=3 for page 1 and examine a page 2 exists
        // To get the nextPageToken, we use a QueryConfigurationsResponseObserver via the wrapper's channel getter
        final AnnotationTestBase.QueryConfigurationsResponseObserver page1Observer =
                new AnnotationTestBase.QueryConfigurationsResponseObserver();
        new Thread(() -> {
            final com.ospreydcs.dp.grpc.v1.annotation.DpAnnotationServiceGrpc.DpAnnotationServiceStub stub =
                    com.ospreydcs.dp.grpc.v1.annotation.DpAnnotationServiceGrpc.newStub(
                            annotationServiceWrapper.getChannel());
            stub.queryConfigurations(
                    AnnotationTestBase.buildQueryConfigurationsRequest(List.of(criterion), 3, null),
                    page1Observer);
        }).start();
        page1Observer.await();
        assertFalse(page1Observer.getErrorMessage(), page1Observer.isError());
        assertEquals(3, page1Observer.getConfigurationList().size());
        final String nextPageToken = page1Observer.getNextPageToken();
        assertNotNull(nextPageToken);
        assertFalse(nextPageToken.isBlank());

        // page 2: continue from token
        final List<Configuration> page2Results = annotationServiceWrapper.sendAndVerifyQueryConfigurations(
                List.of(criterion), 3, nextPageToken, false, null, 2);
    }

    // =========================================================================
    // deleteConfiguration tests
    // =========================================================================

    @Test
    public void testDeleteConfigurationSuccess() {
        // save then delete
        final AnnotationTestBase.SaveConfigurationParams params = new AnnotationTestBase.SaveConfigurationParams(
                "del-config-1", "cat1", null, null, null, null, null);
        annotationServiceWrapper.sendAndVerifySaveConfiguration(params, false, null);

        final String deleted = annotationServiceWrapper.sendAndVerifyDeleteConfiguration(
                "del-config-1", false, null);
        assertEquals("del-config-1", deleted);
    }

    @Test
    public void testDeleteConfigurationRejectBlankName() {
        annotationServiceWrapper.sendAndVerifyDeleteConfiguration(
                "", true, "DeleteConfigurationRequest.configurationName must be specified");
    }

    @Test
    public void testDeleteConfigurationRejectNotFound() {
        annotationServiceWrapper.sendAndVerifyDeleteConfiguration(
                "no-such-config", true, "no Configuration record found for: no-such-config");
    }

    @Test
    public void testDeleteConfigurationDoubleDeleteRejected() {
        // save once, delete once succeeds, delete again is rejected
        final AnnotationTestBase.SaveConfigurationParams params = new AnnotationTestBase.SaveConfigurationParams(
                "del-config-2", "cat1", null, null, null, null, null);
        annotationServiceWrapper.sendAndVerifySaveConfiguration(params, false, null);
        annotationServiceWrapper.sendAndVerifyDeleteConfiguration("del-config-2", false, null);
        annotationServiceWrapper.sendAndVerifyDeleteConfiguration(
                "del-config-2", true, "no Configuration record found for: del-config-2");
    }

    // =========================================================================
    // Configuration stub tests
    // =========================================================================

    @Test
    public void testPatchConfigurationStub() {
        annotationServiceWrapper.sendAndVerifyPatchConfigurationStub("some-config");
    }

    @Test
    public void testBulkSaveConfigurationStub() {
        annotationServiceWrapper.sendAndVerifyBulkSaveConfigurationStub();
    }

    // =========================================================================
    // saveConfigurationActivation tests
    // =========================================================================

    private String saveConfigForActivationTests(String name, String category) {
        final AnnotationTestBase.SaveConfigurationParams params = new AnnotationTestBase.SaveConfigurationParams(
                name, category, null, null, null, null, null);
        return annotationServiceWrapper.sendAndVerifySaveConfiguration(params, false, null);
    }

    @Test
    public void testSaveConfigurationActivationRejectBlankConfigName() {
        final AnnotationTestBase.SaveConfigurationActivationParams params =
                new AnnotationTestBase.SaveConfigurationActivationParams(
                        null, "", ts(T0_SECONDS, T0_NANOS), null, null, null, null, null);
        annotationServiceWrapper.sendAndVerifySaveConfigurationActivation(
                params, true, "SaveConfigurationActivationRequest.configurationName must be specified");
    }

    @Test
    public void testSaveConfigurationActivationRejectMissingStartTime() {
        saveConfigForActivationTests("act-config-1", "cat1");
        final AnnotationTestBase.SaveConfigurationActivationParams params =
                new AnnotationTestBase.SaveConfigurationActivationParams(
                        null, "act-config-1", null, null, null, null, null, null);
        annotationServiceWrapper.sendAndVerifySaveConfigurationActivation(
                params, true, "SaveConfigurationActivationRequest.startTime must be specified");
    }

    @Test
    public void testSaveConfigurationActivationRejectEndTimeBeforeStartTime() {
        saveConfigForActivationTests("act-config-2", "cat1");
        final AnnotationTestBase.SaveConfigurationActivationParams params =
                new AnnotationTestBase.SaveConfigurationActivationParams(
                        null, "act-config-2",
                        ts(T1_SECONDS, T1_NANOS), ts(T0_SECONDS, T0_NANOS),
                        null, null, null, null);
        annotationServiceWrapper.sendAndVerifySaveConfigurationActivation(
                params, true, "endTime must be after startTime");
    }

    @Test
    public void testSaveConfigurationActivationRejectDuplicateAttributeKeys() {
        saveConfigForActivationTests("act-config-3", "cat1");
        final List<Attribute> attributes = List.of(
                Attribute.newBuilder().setName("key1").setValue("v1").build(),
                Attribute.newBuilder().setName("key1").setValue("v2").build()
        );
        final AnnotationTestBase.SaveConfigurationActivationParams params =
                new AnnotationTestBase.SaveConfigurationActivationParams(
                        null, "act-config-3", ts(T0_SECONDS, T0_NANOS), null,
                        null, null, attributes, null);
        annotationServiceWrapper.sendAndVerifySaveConfigurationActivation(
                params, true, "duplicate key");
    }

    @Test
    public void testSaveConfigurationActivationServerGeneratesId() {
        saveConfigForActivationTests("act-config-4", "cat1");
        // send with blank clientActivationId — server should generate UUID
        final AnnotationTestBase.SaveConfigurationActivationParams params =
                new AnnotationTestBase.SaveConfigurationActivationParams(
                        "", "act-config-4", ts(T0_SECONDS, T0_NANOS), null,
                        null, null, null, null);
        final String id = annotationServiceWrapper.sendAndVerifySaveConfigurationActivation(params, false, null);
        assertNotNull(id);
        assertFalse(id.isBlank());
        // verify the id is UUID-like (not blank, 36 chars)
        assertEquals(36, id.length());
    }

    @Test
    public void testSaveConfigurationActivationClientIdPreserved() {
        saveConfigForActivationTests("act-config-5", "cat1");
        final AnnotationTestBase.SaveConfigurationActivationParams params =
                new AnnotationTestBase.SaveConfigurationActivationParams(
                        "my-custom-id-001", "act-config-5", ts(T0_SECONDS, T0_NANOS), null,
                        null, null, null, null);
        final String id = annotationServiceWrapper.sendAndVerifySaveConfigurationActivation(params, false, null);
        assertEquals("my-custom-id-001", id);

        // verify in database
        final ConfigurationActivationDocument doc =
                mongoClient.findConfigurationActivationById("my-custom-id-001");
        assertNotNull(doc);
        assertEquals("act-config-5", doc.getConfigurationName());
        assertEquals("cat1", doc.getInternalCategory());
        assertNotNull(doc.getStartTime());
    }

    @Test
    public void testSaveConfigurationActivationCreateAndUpdate() {
        saveConfigForActivationTests("act-config-6", "cat1");
        final AnnotationTestBase.SaveConfigurationActivationParams createParams =
                new AnnotationTestBase.SaveConfigurationActivationParams(
                        "act-id-006", "act-config-6",
                        ts(T0_SECONDS, T0_NANOS), ts(T2_SECONDS, T2_NANOS),
                        "original description", null, null, "craigmcc");
        annotationServiceWrapper.sendAndVerifySaveConfigurationActivation(createParams, false, null);

        final ConfigurationActivationDocument doc = mongoClient.findConfigurationActivationById("act-id-006");
        assertNotNull(doc);
        assertNotNull(doc.getCreatedAt());
        assertNull(doc.getUpdatedAt());
        assertEquals("original description", doc.getDescription());

        // update
        final AnnotationTestBase.SaveConfigurationActivationParams updateParams =
                new AnnotationTestBase.SaveConfigurationActivationParams(
                        "act-id-006", "act-config-6",
                        ts(T0_SECONDS, T0_NANOS), ts(T2_SECONDS, T2_NANOS),
                        "updated description", null, null, "allenck");
        annotationServiceWrapper.sendAndVerifySaveConfigurationActivation(updateParams, false, null);

        final ConfigurationActivationDocument updatedDoc = mongoClient.findConfigurationActivationById("act-id-006");
        assertNotNull(updatedDoc);
        assertNotNull(updatedDoc.getCreatedAt());
        assertNotNull(updatedDoc.getUpdatedAt());
        assertEquals("updated description", updatedDoc.getDescription());
        assertEquals(doc.getCreatedAt(), updatedDoc.getCreatedAt());
    }

    @Test
    public void testSaveConfigurationActivationRejectOverlapSameConfigName() {
        saveConfigForActivationTests("act-overlap-1", "overlap-cat");
        // save activation for act-overlap-1: t0 to t2
        final AnnotationTestBase.SaveConfigurationActivationParams first =
                new AnnotationTestBase.SaveConfigurationActivationParams(
                        "overlap-id-1", "act-overlap-1",
                        ts(T0_SECONDS, T0_NANOS), ts(T2_SECONDS, T2_NANOS),
                        null, null, null, null);
        annotationServiceWrapper.sendAndVerifySaveConfigurationActivation(first, false, null);
        // verify document is in DB before sending overlapping request
        assertNotNull(mongoClient.findConfigurationActivationById("overlap-id-1"));

        // save overlapping activation for same config: t1 to t3 — should reject
        final AnnotationTestBase.SaveConfigurationActivationParams overlap =
                new AnnotationTestBase.SaveConfigurationActivationParams(
                        "overlap-id-2", "act-overlap-1",
                        ts(T1_SECONDS, T1_NANOS), ts(T3_SECONDS, T3_NANOS),
                        null, null, null, null);
        annotationServiceWrapper.sendAndVerifySaveConfigurationActivation(
                overlap, true, "overlapping activation exists");
    }

    @Test
    public void testSaveConfigurationActivationRejectOverlapSameCategory() {
        saveConfigForActivationTests("cat-config-A", "shared-cat");
        saveConfigForActivationTests("cat-config-B", "shared-cat");

        // save activation for cat-config-A: t0 to t2
        final AnnotationTestBase.SaveConfigurationActivationParams first =
                new AnnotationTestBase.SaveConfigurationActivationParams(
                        "cat-id-1", "cat-config-A",
                        ts(T0_SECONDS, T0_NANOS), ts(T2_SECONDS, T2_NANOS),
                        null, null, null, null);
        annotationServiceWrapper.sendAndVerifySaveConfigurationActivation(first, false, null);
        // verify document is in DB before sending overlapping request
        assertNotNull(mongoClient.findConfigurationActivationById("cat-id-1"));

        // save overlapping activation for cat-config-B with same category: t1 to t3 — should reject
        final AnnotationTestBase.SaveConfigurationActivationParams overlap =
                new AnnotationTestBase.SaveConfigurationActivationParams(
                        "cat-id-2", "cat-config-B",
                        ts(T1_SECONDS, T1_NANOS), ts(T3_SECONDS, T3_NANOS),
                        null, null, null, null);
        annotationServiceWrapper.sendAndVerifySaveConfigurationActivation(
                overlap, true, "overlapping activation exists");
    }

    @Test
    public void testSaveConfigurationActivationOpenEnded() {
        saveConfigForActivationTests("open-config-1", "open-cat");
        // save open-ended activation (no endTime)
        final AnnotationTestBase.SaveConfigurationActivationParams params =
                new AnnotationTestBase.SaveConfigurationActivationParams(
                        "open-id-1", "open-config-1",
                        ts(T0_SECONDS, T0_NANOS), null,
                        null, null, null, null);
        final String id = annotationServiceWrapper.sendAndVerifySaveConfigurationActivation(params, false, null);
        assertEquals("open-id-1", id);

        final ConfigurationActivationDocument doc = mongoClient.findConfigurationActivationById("open-id-1");
        assertNotNull(doc);
        assertNull(doc.getEndTime());
    }

    @Test
    public void testSaveConfigurationActivationRejectOverlapWithOpenEnded() {
        saveConfigForActivationTests("open-config-2", "open-cat-2");
        // save open-ended activation
        final AnnotationTestBase.SaveConfigurationActivationParams openEnded =
                new AnnotationTestBase.SaveConfigurationActivationParams(
                        "open-id-2", "open-config-2",
                        ts(T0_SECONDS, T0_NANOS), null,
                        null, null, null, null);
        annotationServiceWrapper.sendAndVerifySaveConfigurationActivation(openEnded, false, null);

        // save another activation for same config that would start after t0 — should overlap
        final AnnotationTestBase.SaveConfigurationActivationParams later =
                new AnnotationTestBase.SaveConfigurationActivationParams(
                        "open-id-3", "open-config-2",
                        ts(T1_SECONDS, T1_NANOS), ts(T3_SECONDS, T3_NANOS),
                        null, null, null, null);
        annotationServiceWrapper.sendAndVerifySaveConfigurationActivation(
                later, true, "overlapping activation");
    }

    // =========================================================================
    // getConfigurationActivation tests
    // =========================================================================

    private String createActivationForGetTests(
            String configName, String category, String clientId,
            long startSec, long startNano, long endSec, long endNano) {
        saveConfigForActivationTests(configName, category);
        final AnnotationTestBase.SaveConfigurationActivationParams params =
                new AnnotationTestBase.SaveConfigurationActivationParams(
                        clientId, configName,
                        ts(startSec, startNano), ts(endSec, endNano),
                        null, null, null, null);
        return annotationServiceWrapper.sendAndVerifySaveConfigurationActivation(params, false, null);
    }

    @Test
    public void testGetConfigurationActivationByIdSuccess() {
        createActivationForGetTests(
                "get-act-config-1", "get-cat-1", "get-act-id-1",
                T0_SECONDS, T0_NANOS, T1_SECONDS, T1_NANOS);

        final ConfigurationActivation result = annotationServiceWrapper
                .sendAndVerifyGetConfigurationActivationById("get-act-id-1", false, null);
        assertNotNull(result);
        assertEquals("get-act-id-1", result.getClientActivationId());
        assertEquals("get-act-config-1", result.getConfigurationName());
    }

    @Test
    public void testGetConfigurationActivationByCompositeKeySuccess() {
        createActivationForGetTests(
                "get-act-config-2", "get-cat-2", "get-act-id-2",
                T1_SECONDS, T1_NANOS, T2_SECONDS, T2_NANOS);

        final ConfigurationActivation result = annotationServiceWrapper
                .sendAndVerifyGetConfigurationActivationByCompositeKey(
                        "get-act-config-2", ts(T1_SECONDS, T1_NANOS), false, null);
        assertNotNull(result);
        assertEquals("get-act-config-2", result.getConfigurationName());
    }

    @Test
    public void testGetConfigurationActivationByIdRejectBlankId() {
        annotationServiceWrapper.sendAndVerifyGetConfigurationActivationById(
                "", true, "GetConfigurationActivationRequest.clientActivationId must not be blank");
    }

    @Test
    public void testGetConfigurationActivationByCompositeKeyRejectBlankName() {
        annotationServiceWrapper.sendAndVerifyGetConfigurationActivationByCompositeKey(
                "", ts(T0_SECONDS, T0_NANOS),
                true, "GetConfigurationActivationRequest.compositeKey.configurationName must not be blank");
    }

    @Test
    public void testGetConfigurationActivationByIdRejectNotFound() {
        annotationServiceWrapper.sendAndVerifyGetConfigurationActivationById(
                "no-such-id", true, "no ConfigurationActivation record found for: clientActivationId: no-such-id");
    }

    @Test
    public void testGetConfigurationActivationByCompositeKeyRejectNotFound() {
        annotationServiceWrapper.sendAndVerifyGetConfigurationActivationByCompositeKey(
                "no-such-config", ts(T0_SECONDS, T0_NANOS),
                true, "no ConfigurationActivation record found for:");
    }

    // =========================================================================
    // queryConfigurationActivations tests
    // =========================================================================

    private void createQueryActivationsScenario() {
        // config-QA1: category=qa-cat1
        // config-QA2: category=qa-cat2
        // config-QA3: category=qa-cat1
        saveConfigForActivationTests("config-QA1", "qa-cat1");
        saveConfigForActivationTests("config-QA2", "qa-cat2");
        saveConfigForActivationTests("config-QA3", "qa-cat1");

        // Activation QA1: config-QA1, t0-t1, tags=[qa-tag1], attrs={env=prod}
        final AnnotationTestBase.SaveConfigurationActivationParams qa1 =
                new AnnotationTestBase.SaveConfigurationActivationParams(
                        "qa-act-1", "config-QA1",
                        ts(T0_SECONDS, T0_NANOS), ts(T1_SECONDS, T1_NANOS),
                        null,
                        List.of("qa-tag1"),
                        List.of(Attribute.newBuilder().setName("env").setValue("prod").build()),
                        null);
        annotationServiceWrapper.sendAndVerifySaveConfigurationActivation(qa1, false, null);

        // Activation QA2: config-QA2, t1-t2, tags=[qa-tag2], attrs={env=dev}
        final AnnotationTestBase.SaveConfigurationActivationParams qa2 =
                new AnnotationTestBase.SaveConfigurationActivationParams(
                        "qa-act-2", "config-QA2",
                        ts(T1_SECONDS, T1_NANOS), ts(T2_SECONDS, T2_NANOS),
                        null,
                        List.of("qa-tag2"),
                        List.of(Attribute.newBuilder().setName("env").setValue("dev").build()),
                        null);
        annotationServiceWrapper.sendAndVerifySaveConfigurationActivation(qa2, false, null);

        // Activation QA3: config-QA3, t2-t3, tags=[qa-tag1,qa-tag2]
        final AnnotationTestBase.SaveConfigurationActivationParams qa3 =
                new AnnotationTestBase.SaveConfigurationActivationParams(
                        "qa-act-3", "config-QA3",
                        ts(T2_SECONDS, T2_NANOS), ts(T3_SECONDS, T3_NANOS),
                        null,
                        List.of("qa-tag1", "qa-tag2"),
                        null,
                        null);
        annotationServiceWrapper.sendAndVerifySaveConfigurationActivation(qa3, false, null);
    }

    @Test
    public void testQueryConfigurationActivationsRejectEmptyCriteria() {
        annotationServiceWrapper.sendAndVerifyQueryConfigurationActivations(
                List.of(), 0, null,
                true, "QueryConfigurationActivationsRequest.criteria must not be empty", 0);
    }

    @Test
    public void testQueryConfigurationActivationsByTimestamp() {
        createQueryActivationsScenario();
        // Query at T0_SECONDS + 1 second (within t0-t1 range) — should match QA1
        final Timestamp queryTs = ts(T0_SECONDS + 1, 0L);
        final QueryConfigurationActivationsRequest.QueryConfigurationActivationsCriterion criterion =
                QueryConfigurationActivationsRequest.QueryConfigurationActivationsCriterion.newBuilder()
                        .setTimestampCriterion(
                                QueryConfigurationActivationsRequest.QueryConfigurationActivationsCriterion.TimestampCriterion.newBuilder()
                                        .setTimestamp(queryTs)
                                        .build())
                        .build();
        final List<ConfigurationActivation> results = annotationServiceWrapper
                .sendAndVerifyQueryConfigurationActivations(List.of(criterion), 0, null, false, null, 1);
        assertEquals("qa-act-1", results.get(0).getClientActivationId());
    }

    @Test
    public void testQueryConfigurationActivationsByTimeRange() {
        createQueryActivationsScenario();
        // Query for t0-t2 range — should match QA1 and QA2
        final QueryConfigurationActivationsRequest.QueryConfigurationActivationsCriterion criterion =
                QueryConfigurationActivationsRequest.QueryConfigurationActivationsCriterion.newBuilder()
                        .setTimeRangeCriterion(
                                QueryConfigurationActivationsRequest.QueryConfigurationActivationsCriterion.TimeRangeCriterion.newBuilder()
                                        .setStartTime(ts(T0_SECONDS, T0_NANOS))
                                        .setEndTime(ts(T2_SECONDS, T2_NANOS))
                                        .build())
                        .build();
        annotationServiceWrapper.sendAndVerifyQueryConfigurationActivations(
                List.of(criterion), 0, null, false, null, 2);
    }

    @Test
    public void testQueryConfigurationActivationsByConfigurationName() {
        createQueryActivationsScenario();
        final QueryConfigurationActivationsRequest.QueryConfigurationActivationsCriterion criterion =
                QueryConfigurationActivationsRequest.QueryConfigurationActivationsCriterion.newBuilder()
                        .setConfigurationNameCriterion(
                                QueryConfigurationActivationsRequest.QueryConfigurationActivationsCriterion.ConfigurationNameCriterion.newBuilder()
                                        .addValues("config-QA1")
                                        .build())
                        .build();
        final List<ConfigurationActivation> results = annotationServiceWrapper
                .sendAndVerifyQueryConfigurationActivations(List.of(criterion), 0, null, false, null, 1);
        assertEquals("qa-act-1", results.get(0).getClientActivationId());
    }

    @Test
    public void testQueryConfigurationActivationsByClientActivationId() {
        createQueryActivationsScenario();
        final QueryConfigurationActivationsRequest.QueryConfigurationActivationsCriterion criterion =
                QueryConfigurationActivationsRequest.QueryConfigurationActivationsCriterion.newBuilder()
                        .setClientActivationIdCriterion(
                                QueryConfigurationActivationsRequest.QueryConfigurationActivationsCriterion.ClientActivationIdCriterion.newBuilder()
                                        .addValues("qa-act-2")
                                        .build())
                        .build();
        final List<ConfigurationActivation> results = annotationServiceWrapper
                .sendAndVerifyQueryConfigurationActivations(List.of(criterion), 0, null, false, null, 1);
        assertEquals("qa-act-2", results.get(0).getClientActivationId());
    }

    @Test
    public void testQueryConfigurationActivationsByCategory() {
        createQueryActivationsScenario();
        final QueryConfigurationActivationsRequest.QueryConfigurationActivationsCriterion criterion =
                QueryConfigurationActivationsRequest.QueryConfigurationActivationsCriterion.newBuilder()
                        .setCategoryCriterion(
                                QueryConfigurationActivationsRequest.QueryConfigurationActivationsCriterion.CategoryCriterion.newBuilder()
                                        .addValues("qa-cat1")
                                        .build())
                        .build();
        // QA1 (config-QA1, cat=qa-cat1) and QA3 (config-QA3, cat=qa-cat1)
        annotationServiceWrapper.sendAndVerifyQueryConfigurationActivations(
                List.of(criterion), 0, null, false, null, 2);
    }

    @Test
    public void testQueryConfigurationActivationsByTags() {
        createQueryActivationsScenario();
        final QueryConfigurationActivationsRequest.QueryConfigurationActivationsCriterion criterion =
                QueryConfigurationActivationsRequest.QueryConfigurationActivationsCriterion.newBuilder()
                        .setTagsCriterion(
                                QueryConfigurationActivationsRequest.QueryConfigurationActivationsCriterion.TagsCriterion.newBuilder()
                                        .addValues("qa-tag1")
                                        .build())
                        .build();
        // QA1 has qa-tag1, QA3 has qa-tag1 (after normalization)
        annotationServiceWrapper.sendAndVerifyQueryConfigurationActivations(
                List.of(criterion), 0, null, false, null, 2);
    }

    @Test
    public void testQueryConfigurationActivationsByAttributeKeyOnly() {
        createQueryActivationsScenario();
        final QueryConfigurationActivationsRequest.QueryConfigurationActivationsCriterion criterion =
                QueryConfigurationActivationsRequest.QueryConfigurationActivationsCriterion.newBuilder()
                        .setAttributesCriterion(
                                QueryConfigurationActivationsRequest.QueryConfigurationActivationsCriterion.AttributesCriterion.newBuilder()
                                        .setKey("env")
                                        .build())
                        .build();
        // QA1 (env=prod) and QA2 (env=dev) have env attribute
        annotationServiceWrapper.sendAndVerifyQueryConfigurationActivations(
                List.of(criterion), 0, null, false, null, 2);
    }

    @Test
    public void testQueryConfigurationActivationsByAttributeKeyAndValue() {
        createQueryActivationsScenario();
        final QueryConfigurationActivationsRequest.QueryConfigurationActivationsCriterion criterion =
                QueryConfigurationActivationsRequest.QueryConfigurationActivationsCriterion.newBuilder()
                        .setAttributesCriterion(
                                QueryConfigurationActivationsRequest.QueryConfigurationActivationsCriterion.AttributesCriterion.newBuilder()
                                        .setKey("env")
                                        .addValues("prod")
                                        .build())
                        .build();
        // only QA1 has env=prod
        final List<ConfigurationActivation> results = annotationServiceWrapper
                .sendAndVerifyQueryConfigurationActivations(List.of(criterion), 0, null, false, null, 1);
        assertEquals("qa-act-1", results.get(0).getClientActivationId());
    }

    @Test
    public void testQueryConfigurationActivationsEmptyResult() {
        createQueryActivationsScenario();
        final QueryConfigurationActivationsRequest.QueryConfigurationActivationsCriterion criterion =
                QueryConfigurationActivationsRequest.QueryConfigurationActivationsCriterion.newBuilder()
                        .setClientActivationIdCriterion(
                                QueryConfigurationActivationsRequest.QueryConfigurationActivationsCriterion.ClientActivationIdCriterion.newBuilder()
                                        .addValues("no-such-id")
                                        .build())
                        .build();
        annotationServiceWrapper.sendAndVerifyQueryConfigurationActivations(
                List.of(criterion), 0, null, false, null, 0);
    }

    @Test
    public void testQueryConfigurationActivationsRejectMissingTimestamp() {
        final QueryConfigurationActivationsRequest.QueryConfigurationActivationsCriterion criterion =
                QueryConfigurationActivationsRequest.QueryConfigurationActivationsCriterion.newBuilder()
                        .setTimestampCriterion(
                                QueryConfigurationActivationsRequest.QueryConfigurationActivationsCriterion.TimestampCriterion.newBuilder()
                                        .build())
                        .build();
        annotationServiceWrapper.sendAndVerifyQueryConfigurationActivations(
                List.of(criterion), 0, null,
                true, "TimestampCriterion.timestamp must be specified", 0);
    }

    @Test
    public void testQueryConfigurationActivationsRejectMissingTimeRangeStart() {
        final QueryConfigurationActivationsRequest.QueryConfigurationActivationsCriterion criterion =
                QueryConfigurationActivationsRequest.QueryConfigurationActivationsCriterion.newBuilder()
                        .setTimeRangeCriterion(
                                QueryConfigurationActivationsRequest.QueryConfigurationActivationsCriterion.TimeRangeCriterion.newBuilder()
                                        .setEndTime(ts(T2_SECONDS, T2_NANOS))
                                        .build())
                        .build();
        annotationServiceWrapper.sendAndVerifyQueryConfigurationActivations(
                List.of(criterion), 0, null,
                true, "TimeRangeCriterion.startTime must be specified", 0);
    }

    @Test
    public void testQueryConfigurationActivationsRejectEmptyConfigNameValues() {
        final QueryConfigurationActivationsRequest.QueryConfigurationActivationsCriterion criterion =
                QueryConfigurationActivationsRequest.QueryConfigurationActivationsCriterion.newBuilder()
                        .setConfigurationNameCriterion(
                                QueryConfigurationActivationsRequest.QueryConfigurationActivationsCriterion.ConfigurationNameCriterion.newBuilder()
                                        .build())
                        .build();
        annotationServiceWrapper.sendAndVerifyQueryConfigurationActivations(
                List.of(criterion), 0, null,
                true, "ConfigurationNameCriterion.values must not be empty", 0);
    }

    // =========================================================================
    // deleteConfigurationActivation tests
    // =========================================================================

    @Test
    public void testDeleteConfigurationActivationByIdSuccess() {
        saveConfigForActivationTests("del-act-config-1", "del-cat-1");
        final AnnotationTestBase.SaveConfigurationActivationParams params =
                new AnnotationTestBase.SaveConfigurationActivationParams(
                        "del-act-id-1", "del-act-config-1",
                        ts(T0_SECONDS, T0_NANOS), ts(T1_SECONDS, T1_NANOS),
                        null, null, null, null);
        annotationServiceWrapper.sendAndVerifySaveConfigurationActivation(params, false, null);

        final String deleted = annotationServiceWrapper.sendAndVerifyDeleteConfigurationActivationById(
                "del-act-id-1", false, null);
        assertEquals("del-act-id-1", deleted);
    }

    @Test
    public void testDeleteConfigurationActivationByCompositeKeySuccess() {
        saveConfigForActivationTests("del-act-config-2", "del-cat-2");
        final AnnotationTestBase.SaveConfigurationActivationParams params =
                new AnnotationTestBase.SaveConfigurationActivationParams(
                        "del-act-id-2", "del-act-config-2",
                        ts(T1_SECONDS, T1_NANOS), ts(T2_SECONDS, T2_NANOS),
                        null, null, null, null);
        annotationServiceWrapper.sendAndVerifySaveConfigurationActivation(params, false, null);

        final String deleted = annotationServiceWrapper.sendAndVerifyDeleteConfigurationActivationByCompositeKey(
                "del-act-config-2", ts(T1_SECONDS, T1_NANOS), false, null);
        // Response must carry the actual clientActivationId, not a synthetic composite-key string.
        assertEquals("del-act-id-2", deleted);
    }

    @Test
    public void testDeleteConfigurationActivationByIdRejectBlankId() {
        annotationServiceWrapper.sendAndVerifyDeleteConfigurationActivationById(
                "", true, "DeleteConfigurationActivationRequest.clientActivationId must not be blank");
    }

    @Test
    public void testDeleteConfigurationActivationByIdRejectNotFound() {
        annotationServiceWrapper.sendAndVerifyDeleteConfigurationActivationById(
                "no-such-id", true, "no ConfigurationActivation record found for: clientActivationId: no-such-id");
    }

    @Test
    public void testDeleteConfigurationActivationDoubleDeleteRejected() {
        saveConfigForActivationTests("del-act-config-3", "del-cat-3");
        final AnnotationTestBase.SaveConfigurationActivationParams params =
                new AnnotationTestBase.SaveConfigurationActivationParams(
                        "del-act-id-3", "del-act-config-3",
                        ts(T0_SECONDS, T0_NANOS), ts(T1_SECONDS, T1_NANOS),
                        null, null, null, null);
        annotationServiceWrapper.sendAndVerifySaveConfigurationActivation(params, false, null);
        annotationServiceWrapper.sendAndVerifyDeleteConfigurationActivationById("del-act-id-3", false, null);
        annotationServiceWrapper.sendAndVerifyDeleteConfigurationActivationById(
                "del-act-id-3", true, "no ConfigurationActivation record found for: clientActivationId: del-act-id-3");
    }

    // =========================================================================
    // getActiveConfigurations tests
    // =========================================================================

    private void createGetActiveConfigurationsScenario() {
        // active-config-1: category=active-cat1, active t0 to t2
        saveConfigForActivationTests("active-config-1", "active-cat1");
        final AnnotationTestBase.SaveConfigurationActivationParams a1 =
                new AnnotationTestBase.SaveConfigurationActivationParams(
                        "active-id-1", "active-config-1",
                        ts(T0_SECONDS, T0_NANOS), ts(T2_SECONDS, T2_NANOS),
                        null, null, null, null);
        annotationServiceWrapper.sendAndVerifySaveConfigurationActivation(a1, false, null);

        // active-config-2: category=active-cat2, active t1 to t3
        saveConfigForActivationTests("active-config-2", "active-cat2");
        final AnnotationTestBase.SaveConfigurationActivationParams a2 =
                new AnnotationTestBase.SaveConfigurationActivationParams(
                        "active-id-2", "active-config-2",
                        ts(T1_SECONDS, T1_NANOS), ts(T3_SECONDS, T3_NANOS),
                        null, null, null, null);
        annotationServiceWrapper.sendAndVerifySaveConfigurationActivation(a2, false, null);

        // active-config-3: category=active-cat3, active t2 to t4
        saveConfigForActivationTests("active-config-3", "active-cat3");
        final AnnotationTestBase.SaveConfigurationActivationParams a3 =
                new AnnotationTestBase.SaveConfigurationActivationParams(
                        "active-id-3", "active-config-3",
                        ts(T2_SECONDS, T2_NANOS), ts(T4_SECONDS, T4_NANOS),
                        null, null, null, null);
        annotationServiceWrapper.sendAndVerifySaveConfigurationActivation(a3, false, null);

        // active-config-4: category=active-cat4, open-ended from t3
        saveConfigForActivationTests("active-config-4", "active-cat4");
        final AnnotationTestBase.SaveConfigurationActivationParams a4 =
                new AnnotationTestBase.SaveConfigurationActivationParams(
                        "active-id-4", "active-config-4",
                        ts(T3_SECONDS, T3_NANOS), null,
                        null, null, null, null);
        annotationServiceWrapper.sendAndVerifySaveConfigurationActivation(a4, false, null);
    }

    @Test
    public void testGetActiveConfigurationsDefaultsToNow() {
        // Omitting the timestamp (passing null) should default to current server time and succeed.
        // No scenario data needed — just verifying no error is returned.
        annotationServiceWrapper.sendAndVerifyGetActiveConfigurations(
                null, false, null, 0);
    }

    @Test
    public void testGetActiveConfigurationsAtT0Plus1() {
        createGetActiveConfigurationsScenario();
        // at t0+1s: only active-config-1 is active (t0 to t2)
        final List<ConfigurationActivation> results = annotationServiceWrapper
                .sendAndVerifyGetActiveConfigurations(
                        ts(T0_SECONDS + 1, 0L), false, null, 1);
        assertEquals("active-id-1", results.get(0).getClientActivationId());
    }

    @Test
    public void testGetActiveConfigurationsAtT1Plus1() {
        createGetActiveConfigurationsScenario();
        // at t1+1s: active-config-1 (t0-t2) and active-config-2 (t1-t3) are active
        annotationServiceWrapper.sendAndVerifyGetActiveConfigurations(
                ts(T1_SECONDS + 1, 0L), false, null, 2);
    }

    @Test
    public void testGetActiveConfigurationsAtT2Plus1() {
        createGetActiveConfigurationsScenario();
        // at t2+1s: active-config-2 (t1-t3) and active-config-3 (t2-t4) are active
        annotationServiceWrapper.sendAndVerifyGetActiveConfigurations(
                ts(T2_SECONDS + 1, 0L), false, null, 2);
    }

    @Test
    public void testGetActiveConfigurationsAtT3Plus1() {
        createGetActiveConfigurationsScenario();
        // at t3+1s: active-config-3 (t2-t4) and active-config-4 (t3-open) are active
        annotationServiceWrapper.sendAndVerifyGetActiveConfigurations(
                ts(T3_SECONDS + 1, 0L), false, null, 2);
    }

    @Test
    public void testGetActiveConfigurationsAtT5() {
        createGetActiveConfigurationsScenario();
        // at t5 (after t4): only active-config-4 (t3-open) is active
        annotationServiceWrapper.sendAndVerifyGetActiveConfigurations(
                ts(T5_SECONDS, T5_NANOS), false, null, 1);
    }

    @Test
    public void testGetActiveConfigurationsNoneActive() {
        createGetActiveConfigurationsScenario();
        // before t0 there are no activations
        annotationServiceWrapper.sendAndVerifyGetActiveConfigurations(
                ts(T0_SECONDS - 1, 0L), false, null, 0);
    }

    // =========================================================================
    // ConfigurationActivation stub tests
    // =========================================================================

    @Test
    public void testPatchConfigurationActivationStub() {
        annotationServiceWrapper.sendAndVerifyPatchConfigurationActivationStub("some-id");
    }

    @Test
    public void testBulkSaveConfigurationActivationStub() {
        annotationServiceWrapper.sendAndVerifyBulkSaveConfigurationActivationStub();
    }

}
