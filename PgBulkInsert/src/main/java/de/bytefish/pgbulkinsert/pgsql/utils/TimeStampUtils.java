// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.pgsql.utils;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;

public class TimeStampUtils {

    private TimeStampUtils() {

    }

    public static int toPgDays(LocalDate date)
    {
        // Adjust TimeZone Offset:
        LocalDateTime dateTime = date.atStartOfDay();
        // pg time 0 is 2000-01-01 00:00:00:
        long secs = toPgSecs(getSecondsSinceJavaEpoch(dateTime));
        // Needs Days:
        return (int) TimeUnit.SECONDS.toDays(secs);
    }

    public static Long toPgSecs(LocalDateTime dateTime) {
        // pg time 0 is 2000-01-01 00:00:00:
        long secs = toPgSecs(getSecondsSinceJavaEpoch(dateTime));
        // Needs Microseconds:
        return TimeUnit.SECONDS.toMicros(secs);
    }

    private static long getSecondsSinceJavaEpoch(LocalDateTime localDateTime) {
        // Adjust TimeZone Offset:
        OffsetDateTime zdt = localDateTime.atOffset(ZoneOffset.UTC);
        // Get the Epoch Milliseconds:
        long milliseconds = zdt.toInstant().toEpochMilli();
        // Turn into Seconds:
        return TimeUnit.MILLISECONDS.toSeconds(milliseconds);
    }

    /**
     * Converts the given java seconds to postgresql seconds. The conversion is valid for any year 100 BC onwards.
     *
     * from /org/postgresql/jdbc2/TimestampUtils.java
     *
     * @param seconds Postgresql seconds.
     * @return Java seconds.
     */
    @SuppressWarnings("checkstyle:magicnumber")
    private static long toPgSecs(final long seconds) {
        long secs = seconds;
        // java epoc to postgres epoc
        secs -= 946684800L;

        // Julian/Greagorian calendar cutoff point
        if (secs < -13165977600L) { // October 15, 1582 -> October 4, 1582
            secs -= 86400 * 10;
            if (secs < -15773356800L) { // 1500-03-01 -> 1500-02-28
                int years = (int) ((secs + 15773356800L) / -3155823050L);
                years++;
                years -= years / 4;
                secs += years * 86400;
            }
        }

        return secs;
    }
}
