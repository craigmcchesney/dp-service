package com.ospreydcs.dp.service.integration.ingest;

import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataRequest;
import com.ospreydcs.dp.grpc.v1.ingestion.IngestDataResponse;
import com.ospreydcs.dp.service.ingest.IngestionTestBase;
import com.ospreydcs.dp.service.integration.GrpcIntegrationTestBase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class IngestDataBidiStreamExplicitTimestampListIT extends GrpcIntegrationTestBase {

    // static variables
    private static final Logger logger = LogManager.getLogger();

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() {
        super.tearDown();
    }

    @Test
    public void explicitTimestampListTest() {

        final long startSeconds = configMgr().getConfigLong(CFG_KEY_START_SECONDS, DEFAULT_START_SECONDS);
        final long startNanos = 0L;

        // register ingestion provider
        final String providerName = String.valueOf(1);
        final String providerId = ingestionServiceWrapper.registerProvider(providerName, null);

        {
            // ingest data with explicit timestamp list

            final List<String> pvNames = Arrays.asList(
                    "pv_01", "pv_02", "pv_03", "pv_04", "pv_05");

            // create list with list of data values for each column, same list will be used for all requests
            final List<List<Object>> values = new ArrayList<>();
            final int numSamples = 5;
            for (int pvIndex = 0 ; pvIndex < pvNames.size() ; ++pvIndex) {
                final List<Object> pvValueList = new ArrayList<>();
                for (int valueIndex = 0 ; valueIndex < numSamples ; ++valueIndex) {
                    double pvValue = pvIndex + (valueIndex / numSamples);
                    pvValueList.add(pvValue);
                }
                values.add(pvValueList);
            }

            // send sequence of 5 ingestion requests, one per second, each with data for 5 pvs (created above)
            for (int requestIndex = 0 ; requestIndex < 5 ; ++requestIndex) {

                final String requestId = "request-" + requestIndex;

                final List<Long> timestampsSecondsList = new ArrayList<>();
                final List<Long> timestampNanosList = new ArrayList<>();

                final long requestSeconds = startSeconds + requestIndex;
                final long requestNanos = startNanos;
                final long samplePeriod = 200_000_000L;
                for (int timestampIndex = 0 ; timestampIndex < 5 ; ++timestampIndex) {
                    final long timestampNanos = timestampIndex * samplePeriod;
                    timestampsSecondsList.add(requestSeconds);
                    timestampNanosList.add(timestampNanos);
                }

                // create ingestion request params
                final IngestionTestBase.IngestionRequestParams params =
                        new IngestionTestBase.IngestionRequestParams(
                                providerId,
                                requestId,
                                timestampsSecondsList,
                                timestampNanosList,
                                requestSeconds,
                                requestNanos,
                                0L, // 5 values per second
                                numSamples, // each DataColumn must contain 5 DataValues
                                pvNames,
                                IngestionTestBase.IngestionDataType.DOUBLE,
                                values, null, false, null);

                // build ingestion request
                final IngestDataRequest request = IngestionTestBase.buildIngestionRequest(params);
                final List<IngestDataRequest> requestList = Arrays.asList(request);

                // send request
                final List<IngestDataResponse> responseList = ingestionServiceWrapper.sendIngestDataBidiStream(requestList);

                // verify ingestion
                final List<IngestionTestBase.IngestionRequestParams> paramsList = Arrays.asList(params);
                ingestionServiceWrapper.verifyIngestionHandling(paramsList, requestList, responseList, 0);
            }
        }
    }

}
