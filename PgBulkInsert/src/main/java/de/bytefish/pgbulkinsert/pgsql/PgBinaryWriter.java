// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.pgsql;

import de.bytefish.pgbulkinsert.exceptions.BinaryWriteFailedException;
import de.bytefish.pgbulkinsert.pgsql.handlers.IValueHandler;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;

import org.postgresql.copy.CopyIn;
import org.postgresql.copy.PGCopyOutputStream;

public class PgBinaryWriter implements AutoCloseable {

    private transient DataOutputStream buffer;
    
    private final int bufferSize;

    public PgBinaryWriter(int bufferSize) {
    	this.bufferSize = bufferSize;
    }

    public void open(final CopyIn copyIn) {
        buffer = new DataOutputStream(new BufferedOutputStream(new PGCopyOutputStream(copyIn, 1), bufferSize));

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

    public <TTargetType> void write(final IValueHandler<TTargetType> handler, final TTargetType value) {
        handler.handle(buffer, value);
    }

    @Override
    public void close() {
        try {
            buffer.writeShort(-1);

            buffer.flush();
            buffer.close();
        } catch(Exception e) {
            throw new BinaryWriteFailedException(e);
        }
    }
}
