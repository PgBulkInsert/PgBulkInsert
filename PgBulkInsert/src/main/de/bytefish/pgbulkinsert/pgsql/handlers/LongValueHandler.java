// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.pgsql.handlers;

import de.bytefish.pgbulkinsert.pgsql.constants.DataType;

import java.io.DataOutputStream;

public class LongValueHandler extends BaseValueHandler<Long> {

    @Override
    protected void internalHandle(DataOutputStream buffer, final Long value) throws Exception {
        buffer.writeInt(8);
        buffer.writeLong(value);
    }

    @Override
    public DataType getDataType() {
        return DataType.Int8;
    }
}
