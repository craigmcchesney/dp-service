package com.ospreydcs.dp.service.query.server;

import com.ospreydcs.dp.grpc.v1.common.RejectDetails;
import com.ospreydcs.dp.grpc.v1.common.ResponseType;
import com.ospreydcs.dp.grpc.v1.query.DpQueryServiceGrpc;
import com.ospreydcs.dp.grpc.v1.query.QueryDataByTimeRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryDataResponse;
import com.ospreydcs.dp.service.query.QueryTestBase;
import com.ospreydcs.dp.service.query.handler.QueryHandlerBase;
import com.ospreydcs.dp.service.query.handler.QueryHandlerInterface;
import com.ospreydcs.dp.service.query.service.QueryServiceImpl;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.MethodSorters;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.mock;

@RunWith(JUnit4.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class QueryGrpcTest extends QueryTestBase {

    protected static class TestQueryHandler extends QueryHandlerBase implements QueryHandlerInterface {

        @Override
        public boolean init() {
            System.out.println("handler.init");
            return true;
        }

        @Override
        public boolean fini() {
            System.out.println("handler.fini");
            return true;
        }

        @Override
        public boolean start() {
            System.out.println("handler.start");
            return true;
        }

        @Override
        public boolean stop() {
            System.out.println("handler.fini");
            return true;
        }
    }

    protected static class TestQueryClient {

        // must use async stub for streaming api
        protected static DpQueryServiceGrpc.DpQueryServiceStub asyncStub;

        public TestQueryClient(Channel channel) {
            asyncStub = DpQueryServiceGrpc.newStub(channel);
        }

        protected List<QueryDataResponse> sendQueryDataByTimeRequest(
                QueryDataByTimeRequest request, int numResponsesExpected) {

            System.out.println("sendQueryDataByTimeRequest responses expected: " + numResponsesExpected);

            List<QueryDataResponse> responseList = new ArrayList<>();
            final CountDownLatch finishLatch = new CountDownLatch(1);

            // create observer for api response stream
            StreamObserver<QueryDataResponse> responseObserver = new StreamObserver<QueryDataResponse>() {

                @Override
                public void onNext(QueryDataResponse queryDataResponse) {
                    System.out.println("sendQueryDataByTimeRequest.responseObserver.onNext");
                    responseList.add(queryDataResponse);
                }

                @Override
                public void onError(Throwable throwable) {
                    System.out.println("sendQueryDataByTimeRequest.responseObserver.onError");
                    Status status = Status.fromThrowable(throwable);
                    finishLatch.countDown();
                }

                @Override
                public void onCompleted() {
                    System.out.println("sendQueryDataByTimeRequest.responseObserver.Completed");
                    finishLatch.countDown();
                }
            };

            // send api request
            asyncStub.queryDataByTime(request, responseObserver);

            // wait for completion of api response stream via finishLatch notification
            try {
                finishLatch.await(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                fail("InterruptedException waiting for finishLatch");
            }

            return responseList;
        }
    }

    /**
     * This rule manages automatic graceful shutdown for the registered servers and channels at the
     * end of test.
     */
    @ClassRule
    public static final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

    private static QueryServiceImpl serviceImpl;

    private static TestQueryClient client;

    @BeforeClass
    public static void setUp() throws Exception {
        QueryHandlerInterface handler = new TestQueryHandler();
        QueryServiceImpl impl = new QueryServiceImpl();
        if (!impl.init(handler)) {
            fail("impl.init failed");
        }
        serviceImpl = mock(QueryServiceImpl.class, delegatesTo(impl));

        // Generate a unique in-process server name.
        String serverName = InProcessServerBuilder.generateName();

        // Create a server, add service, start, and register for automatic graceful shutdown.
        grpcCleanup.register(InProcessServerBuilder
                .forName(serverName).directExecutor().addService(serviceImpl).build().start());

        // Create a client channel and register for automatic graceful shutdown.
        ManagedChannel channel = grpcCleanup.register(
                InProcessChannelBuilder.forName(serverName).directExecutor().build());

        client = new TestQueryClient(channel);
    }

    @AfterClass
    public static void tearDown() {
        serviceImpl = null;
        client = null;
    }

    /**
     * Test validation failure, that columnName not specified.
     */
    @Test
    public void test01ValidateRequestUnspecifiedColumnName() {

        // create request with unspecified column name
        String columnName = null;
        Long nowSeconds = Instant.now().getEpochSecond();
        QueryDataByTimeRequestParams params = new QueryDataByTimeRequestParams(
                null,
                nowSeconds,
                0L,
                nowSeconds + 1,
                0L);
        QueryDataByTimeRequest request = buildQueryDataByTimeRequest(params);

        // send request
        List<QueryDataResponse> responseList = client.sendQueryDataByTimeRequest(request, 1);

        // examine response
        assertTrue("size mismatch between lists of requests and responses",
                responseList.size() == 1);
        QueryDataResponse response = responseList.get(0);
        assertTrue("responseType not set",
                response.getResponseType() == ResponseType.REJECT_RESPONSE);
        assertTrue("response time not set",
                response.getResponseTime().getEpochSeconds() > 0);
        assertTrue("response details not set",
                response.hasRejectDetails());
        assertTrue("reject reason not set",
                response.getRejectDetails().getRejectReason() == RejectDetails.RejectReason.INVALID_REQUEST_REASON);
        assertTrue(
                "reject message not set",
                response.getRejectDetails().getMessage().equals("columnName must be specified"));
    }

}