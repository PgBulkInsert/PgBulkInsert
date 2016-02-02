// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.de.bytefish.pgbulkinsert.pgsql;


import de.bytefish.pgbulkinsert.de.bytefish.pgbulkinsert.exceptions.BinaryWriteFailedException;
import de.bytefish.pgbulkinsert.de.bytefish.pgbulkinsert.functional.Action0;

import java.io.*;
import java.math.BigInteger;
import java.time.*;
import java.util.concurrent.TimeUnit;

public class PgBinaryWriter implements AutoCloseable {

    /** The ByteBuffer to write the output. */
    private transient DataOutputStream buffer;

    public PgBinaryWriter() {
    }

    public void open(final OutputStream out) {
        buffer = new DataOutputStream(new BufferedOutputStream(out));

        writeHeader();
    }

    private void writeHeader() {
        try {

            // 11 bytes required header
            buffer.writeBytes("PGCOPY\n\377\r\n\0");
            // 32 bit integer indicating no OID
            buffer.writeInt(0);
            // 32 bit header extension area length
            buffer.writeInt(0);

        } catch(Exception e) {
            throw new BinaryWriteFailedException(e);
        }
    }

    private <T> void write(Action0 action) {
        try {

            action.invoke();

            buffer.writeShort(-1);
        } catch (Exception e) {
            throw new BinaryWriteFailedException(e);
        }
    }

    public void startRow(int numColumns) {
        try {
            buffer.writeShort(numColumns);
        } catch(Exception e) {
            throw new BinaryWriteFailedException(e);
        }
    }

    public void write(final Boolean value) {
        write(() -> {
            buffer.writeInt(1);
            if (value) {
                buffer.writeByte(1);
            } else {
                buffer.writeByte(0);
            }
        });
    }

    public void write(final BigInteger value) throws IOException {
        write(() -> {
            Long longValue = value.longValue();

            buffer.writeInt(8);
            buffer.writeLong(longValue);
        });
    }

    public void write(final Double value) throws IOException {
        write(() -> {
            buffer.writeInt(8);
            buffer.writeDouble(value);
        });
    }

    public void write(final Float value) throws IOException {
        write(() -> {
            buffer.writeInt(4);
            buffer.writeFloat(value);
        });
    }

    public void write(final Integer value) throws IOException {
        write(() -> {
            buffer.writeInt(4);
            buffer.writeInt(value);
        });
    }

    public void write(final Short value) throws IOException {
        write(() -> {
            buffer.writeInt(2);
            buffer.writeShort(value);
        });
    }

    public void write(final byte value) throws IOException {
        write(() -> {
            buffer.writeInt(1);
            buffer.writeInt(value);
        });
    }

    public void write(final Long value) throws IOException {
        write(() -> {
            buffer.writeInt(8);
            buffer.writeLong(value);
        });
    }

    public void write(final String value) throws IOException {
        write(() -> {
            final byte[] utf8Bytes = value.getBytes("UTF-8");

            buffer.writeInt(utf8Bytes.length);
            buffer.write(utf8Bytes);
        });
    }

    public void write(final LocalDateTime value) throws IOException {
        write(() -> {
            buffer.writeInt(8);
            buffer.writeLong(toPgSecs(value));
        });
    }

    private static long toPgSecs(LocalDateTime dateTime) {
        // Adjust TimeZone Offset:
        OffsetDateTime zdt = dateTime.atOffset(ZoneOffset.UTC);
        // Get the Epoch Millisecodns:
        long milliseconds = zdt.toInstant().toEpochMilli();
        // pg time 0 is 2000-01-01 00:00:00:
        long secs = toPgSecs(TimeUnit.MILLISECONDS.toSeconds(milliseconds));
        // Needs Microseconds:
        return TimeUnit.SECONDS.toMicros(secs);
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


    @Override
    public void close() {
        try {
            buffer.flush();
            buffer.close();
        } catch(Exception e) {
            // is this ok?
        }
    }
}
