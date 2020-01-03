// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.pgsql.handlers;

import java.io.DataOutputStream;

public class DoubleValueHandler<T extends Number> extends BaseValueHandler<T> {

    private static final int SIZE = Double.BYTES;

    @Override
    protected void internalHandle(DataOutputStream buffer, final T value) throws Exception {
        buffer.writeInt(SIZE);
        buffer.writeDouble(value.doubleValue());
    }
}
