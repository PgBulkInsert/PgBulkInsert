// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.pgsql.handlers;

import de.bytefish.pgbulkinsert.pgsql.constants.DataType;

import java.io.DataOutputStream;

public class ShortValueHandler extends BaseValueHandler<Short> {

    @Override
    protected void internalHandle(DataOutputStream buffer, final Short value) throws Exception {
        buffer.writeInt(2);
        buffer.writeShort(value);
    }

    @Override
    public DataType getDataType() {
        return DataType.Int2;
    }
}
