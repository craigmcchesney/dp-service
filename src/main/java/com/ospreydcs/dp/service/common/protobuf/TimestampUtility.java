package com.ospreydcs.dp.service.common.protobuf;

import com.ospreydcs.dp.grpc.v1.common.Timestamp;

import java.time.Instant;
import java.util.Date;

public class TimestampUtility {


    public static Timestamp timestampFromSeconds(long epochSecs, long nanos) {
        return Timestamp.newBuilder().setEpochSeconds(epochSecs).setNanoseconds(nanos).build();
    }

    public static Date dateFromTimestamp(Timestamp timestamp) {
        final Instant timestampInstant = Instant.ofEpochSecond(
                timestamp.getEpochSeconds(), timestamp.getNanoseconds());
        return Date.from(timestampInstant);
    }

    public static Timestamp getTimestampFromInstant(Instant instant) {
        Timestamp timestamp =
                Timestamp.newBuilder()
                        .setEpochSeconds(instant.getEpochSecond())
                        .setNanoseconds(instant.getNano())
                        .build();
        return timestamp;
    }

    public static Timestamp getTimestampNow() {
        Instant instantNow = Instant.now();
        return getTimestampFromInstant(instantNow);
    }

}
