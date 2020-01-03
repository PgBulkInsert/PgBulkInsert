// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.pgsql.handlers;

import java.io.DataOutputStream;
import java.util.UUID;

public class UUIDValueHandler extends BaseValueHandler<UUID> {

    private static final int SIZE = 2 * Long.BYTES;

    @Override
    protected void internalHandle(DataOutputStream buffer, final UUID value) throws Exception {
        buffer.writeInt(SIZE);

        buffer.writeLong(value.getMostSignificantBits());
        buffer.writeLong(value.getLeastSignificantBits());
    }
}
