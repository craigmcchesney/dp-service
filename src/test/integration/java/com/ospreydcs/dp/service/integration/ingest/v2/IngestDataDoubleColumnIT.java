package com.ospreydcs.dp.service.integration.ingest.v2;

import com.ospreydcs.dp.grpc.v1.common.DoubleColumn;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;
import com.ospreydcs.dp.service.ingest.IngestionTestBase;
import com.ospreydcs.dp.service.integration.GrpcIntegrationTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class IngestDataDoubleColumnIT extends GrpcIntegrationTestBase {

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() {
        super.tearDown();
    }

    @Test
    public void doubleColumnTest() {

        String providerId;
        {
            // register ingestion provider
            final String providerName = String.valueOf(1);
            providerId = ingestionServiceWrapper.registerProvider(providerName, null);
        }

        {
            // positive unary ingestion test for DoubleColumn
            // assemble IngestionRequest
            String requestId = "request-8";
            String pvName = "pv_08";
            List<String> columnNames = Arrays.asList(pvName);
            Map<String, String> attributes = Map.of("subsystem", "vacuum", "sector", "42");
            String eventDescription = "calibration test";
            long firstSeconds = Instant.now().getEpochSecond();
            long firstNanos = 0L;
            long sampleIntervalNanos = 1_000_000L;
            int numSamples = 2;
            IngestionTestBase.IngestionRequestParams params =
                    new IngestionTestBase.IngestionRequestParams(
                            providerId,
                            requestId,
                            null,
                            null,
                            null,
                            null,
                            firstSeconds,
                            firstNanos,
                            sampleIntervalNanos,
                            numSamples,
                            columnNames,
                            IngestionTestBase.IngestionDataType.ARRAY_DOUBLE,
                            null, // don't set any column values, we're going to override
                            false);

            // specify explicit DoubleColumn data
            List<DoubleColumn> doubleColumns = new ArrayList<>();
            DoubleColumn.Builder doubleColumnBuilder = DoubleColumn.newBuilder();
            doubleColumnBuilder.setName(pvName);
            doubleColumnBuilder.addValues(12.34);
            doubleColumnBuilder.addValues(34.56);
            doubleColumns.add(doubleColumnBuilder.build());

            IngestDataRequest request =
                    IngestionTestBase.buildIngestionRequest(params, null, doubleColumns);

            // send request and examine response
            ingestionServiceWrapper.sendAndVerifyIngestData(params, request, 0);
        }
    }


}
