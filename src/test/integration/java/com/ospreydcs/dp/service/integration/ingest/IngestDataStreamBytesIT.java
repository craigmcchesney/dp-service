package com.ospreydcs.dp.service.integration.ingest;

import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;
import com.ospreydcs.dp.service.ingest.IngestionTestBase;
import com.ospreydcs.dp.service.integration.GrpcIntegrationTestBase;
import org.junit.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class IngestDataStreamBytesIT extends GrpcIntegrationTestBase {

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() {
        super.tearDown();
    }

    @Test
    public void testIngestDataStreamBytes() {

        // register provider
        String providerId = null;
        {
            final String providerName = "Provider-1";
            final Map<String, String> attributeMap = Map.of("IOC", "IOC-2", "subsystem", "power");
            providerId = ingestionServiceWrapper.registerProvider(providerName, attributeMap);
        }

        // positive test case, successful ingestion
        {
            // create containers
            final List<IngestionTestBase.IngestionRequestParams> paramsList = new ArrayList<>();
            final List<IngestDataRequest> requestList = new ArrayList<>();

            // create 1st request
            {
                final String requestId = "request-1";
                final List<String> columnNames = Arrays.asList("PV_01");
                final List<List<Object>> values = Arrays.asList(Arrays.asList(1.01));
                final Instant instantNow = Instant.now();
                final IngestionTestBase.IngestionRequestParams params =
                        new IngestionTestBase.IngestionRequestParams(
                                providerId,
                                requestId,
                                null,
                                null,
                                instantNow.getEpochSecond(),
                                0L,
                                1_000_000L,
                                1,
                                columnNames,
                                IngestionTestBase.IngestionDataType.DOUBLE,
                                values,
                                null,
                                true, null, null);
                final IngestDataRequest request = IngestionTestBase.buildIngestionRequest(params);
                paramsList.add(params);
                requestList.add(request);
            }

            // create 2nd request
            {
                final String requestId = "request-2";
                final List<String> columnNames = Arrays.asList("PV_02");
                final List<List<Object>> values = Arrays.asList(Arrays.asList(2.02));
                final Instant instantNow = Instant.now();
                final IngestionTestBase.IngestionRequestParams params =
                        new IngestionTestBase.IngestionRequestParams(
                                providerId,
                                requestId,
                                null,
                                null,
                                instantNow.getEpochSecond(),
                                0L,
                                1_000_000L,
                                1,
                                columnNames,
                                IngestionTestBase.IngestionDataType.DOUBLE,
                                values,
                                null,
                                true, null, null);
                final IngestDataRequest request = IngestionTestBase.buildIngestionRequest(params);
                paramsList.add(params);
                requestList.add(request);
            }

            // send request and examine response
            final int numSerializedDataColumnsExpected = 1;
            ingestionServiceWrapper.sendAndVerifyIngestDataStream(
                    paramsList,
                    requestList,
                    numSerializedDataColumnsExpected,
                    false,
                    "");
        }

    }

}
