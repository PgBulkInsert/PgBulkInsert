// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.de.bytefish.pgbulkinsert.pgsql;


import de.bytefish.pgbulkinsert.de.bytefish.pgbulkinsert.exceptions.BinaryWriteFailedException;
import de.bytefish.pgbulkinsert.de.bytefish.pgbulkinsert.pgsql.converter.LocalDateConverter;
import de.bytefish.pgbulkinsert.de.bytefish.pgbulkinsert.pgsql.converter.LocalDateTimeConverter;
import de.bytefish.pgbulkinsert.de.bytefish.pgbulkinsert.pgsql.handlers.*;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Map;

public class PgBinaryWriter implements AutoCloseable {

    /** The ByteBuffer to write the output. */
    private transient DataOutputStream buffer;

    // We don't want to resolve the handlers at runtime, so make them explicitly available:
    private IValueHandler<Boolean> booleanValueHandler;
    private IValueHandler<BigInteger> bigIntegerValueHandler;
    private IValueHandler<Byte> byteValueHandler;
    private IValueHandler<Double> doubleValueHandler;
    private IValueHandler<Float> floatValueHandler;
    private IValueHandler<LocalDate> localDateValueHandler;
    private IValueHandler<LocalDateTime> localDateTimeValueHandler;
    private IValueHandler<Integer> integerValueHandler;
    private IValueHandler<Short> shortValueHandler;
    private IValueHandler<Long> longValueHandler;
    private IValueHandler<String> stringValueHandler;

    public PgBinaryWriter() {

        // Initialize sane defaults:
        bigIntegerValueHandler = new BigIntegerHandler();
        booleanValueHandler = new BooleanHandler();
        byteValueHandler = new ByteHandler();
        doubleValueHandler = new DoubleHandler();
        floatValueHandler = new FloatHandler();
        localDateValueHandler = new LocalDateHandler(new LocalDateConverter());
        localDateTimeValueHandler = new LocalDateTimeHandler(new LocalDateTimeConverter());
        integerValueHandler = new IntegerHandler();
        shortValueHandler = new ShortHandler();
        longValueHandler = new LongHandler();
        stringValueHandler = new StringHandler();
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

    public void startRow(int numColumns) {
        try {
            buffer.writeShort(numColumns);
        } catch(Exception e) {
            throw new BinaryWriteFailedException(e);
        }
    }

    public void write(final Boolean value) {
        booleanValueHandler.handle(buffer, value);
    }

    public void write(final BigInteger value) {
        bigIntegerValueHandler.handle(buffer, value);
    }

    public void write(final Byte value) {
        byteValueHandler.handle(buffer, value);
    }

    public void write(final Double value) {
        doubleValueHandler.handle(buffer, value);
    }

    public void write(final Float value) {
        floatValueHandler.handle(buffer, value);
    }

    public void write(final Integer value) {
        integerValueHandler.handle(buffer, value);
    }

    public void write(final Short value) {
        shortValueHandler.handle(buffer, value);
    }

    public void write(final Long value) {
        longValueHandler.handle(buffer, value);
    }

    public void write(final LocalDate value) {
        localDateValueHandler.handle(buffer, value);
    }

    public void write(final LocalDateTime value) {
        localDateTimeValueHandler.handle(buffer, value);
    }

    public void write(final String value) {
        stringValueHandler.handle(buffer, value);
    }

    @Override
    public void close() {
        try {
            buffer.writeShort(-1); // EOF

            buffer.flush();
            buffer.close();
        } catch(Exception e) {
            // is this ok?
        }
    }
}
