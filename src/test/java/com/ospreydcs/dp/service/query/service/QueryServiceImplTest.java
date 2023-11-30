package com.ospreydcs.dp.service.query.service;

import com.ospreydcs.dp.grpc.v1.common.RejectDetails;
import com.ospreydcs.dp.grpc.v1.common.ResponseType;
import com.ospreydcs.dp.grpc.v1.common.Timestamp;
import com.ospreydcs.dp.grpc.v1.query.QueryDataByTimeRequest;
import com.ospreydcs.dp.grpc.v1.query.QueryDataResponse;
import com.ospreydcs.dp.service.query.QueryTestBase;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.time.Instant;
import java.util.Date;

import static org.junit.Assert.assertTrue;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class QueryServiceImplTest extends QueryTestBase {

    private static final QueryServiceImpl serviceImpl = new QueryServiceImpl();

    @Test
    public void test01DateFromTimestamp() {

        long epochSeconds = 1691438936L;
        long nanos = 999000000L;

        // create a grpc timestamp, and the convert to java date
        final Timestamp.Builder timestampBuilder = Timestamp.newBuilder();
        timestampBuilder.setEpochSeconds(epochSeconds);
        timestampBuilder.setNanoseconds(nanos);
        Timestamp timestamp = timestampBuilder.build();
        Date dateFromTimestamp = serviceImpl.dateFromTimestamp(timestamp);

        // create a java instant, and use to create java date
        Instant instant = Instant.ofEpochSecond(epochSeconds, nanos);
        Date dateFromInstant = Date.from(instant);

        // check that the two dates are equal
        assertTrue("dateFromTimestamp date mismatch with date from instant", dateFromTimestamp.equals(dateFromInstant));
    }

    @Test
    public void test02QueryResponseReject() {

        // create request
        String columnName = null;
        Long nowSeconds = Instant.now().getEpochSecond();
        QueryDataByTimeRequestParams params = new QueryDataByTimeRequestParams(
                null,
                nowSeconds,
                0L,
                nowSeconds + 1,
                0L);
        QueryDataByTimeRequest request = buildQueryDataByTimeRequest(params);

        final String msg = "test";
        QueryDataResponse response =
                serviceImpl.queryResponseReject(request, msg, RejectDetails.RejectReason.INVALID_REQUEST_REASON);

        // check response contains message and reason
        assertTrue("responseType not set",
                response.getResponseType() == ResponseType.REJECT_RESPONSE);
        assertTrue("response time not set",
                response.getResponseTime().getEpochSeconds() > 0);
        assertTrue("response details not set",
                response.hasRejectDetails());
        assertTrue("reject reason not set",
                response.getRejectDetails().getRejectReason() == RejectDetails.RejectReason.INVALID_REQUEST_REASON);
        assertTrue("reject message not set",
                response.getRejectDetails().getMessage().equals(msg));
    }

}
