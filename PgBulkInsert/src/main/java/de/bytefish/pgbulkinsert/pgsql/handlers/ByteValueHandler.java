// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.pgsql.handlers;

import de.bytefish.pgbulkinsert.pgsql.constants.DataType;

import java.io.DataOutputStream;

public class ByteValueHandler extends BaseValueHandler<Byte> {

    @Override
    protected void internalHandle(DataOutputStream buffer, final Byte value) throws Exception {
        buffer.writeInt(1);
        buffer.writeInt(value);
    }

    @Override
    public DataType getDataType() {
        return DataType.Char;
    }
}
