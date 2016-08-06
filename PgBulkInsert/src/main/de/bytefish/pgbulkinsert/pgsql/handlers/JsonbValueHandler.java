// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.pgsql.handlers;

import de.bytefish.pgbulkinsert.pgsql.constants.DataType;
import de.bytefish.pgbulkinsert.pgsql.constants.ObjectIdentifier;

import java.io.DataOutputStream;

public class JsonbValueHandler extends BaseValueHandler<String> {

    private final int jsonbProtocolVersion;

    public JsonbValueHandler() {
        this(1);
    }

    public JsonbValueHandler(int jsonbProtocolVersion) {
        this.jsonbProtocolVersion = jsonbProtocolVersion;
    }

    @Override
    protected void internalHandle(DataOutputStream buffer, final String value) throws Exception {

        byte[] utf8Bytes = value.getBytes("UTF-8");

        // Write the Length of the Data to Copy:
        buffer.writeInt(utf8Bytes.length + 1);
        // Write the Jsonb Protocol Version:
        buffer.writeByte(jsonbProtocolVersion);
        // Copy the Data:
        buffer.write(utf8Bytes);
    }

    @Override
    public DataType getDataType() {
        return DataType.Jsonb;
    }
}
