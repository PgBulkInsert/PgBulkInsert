// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.de.bytefish.pgbulkinsert.pgsql.handlers;

import de.bytefish.pgbulkinsert.de.bytefish.pgbulkinsert.exceptions.BinaryWriteFailedException;

import java.io.DataOutputStream;

public abstract class BaseValueHandler<T> implements IValueHandler<T> {

    @Override
    public void handle(DataOutputStream buffer, final T value) {
        try {
            if (value == null) {
                buffer.writeInt(-1);
                return;
            }
            internalHandle(buffer, value);
        } catch (Exception e) {
            throw new BinaryWriteFailedException(e);
        }
    }

    protected abstract void internalHandle(DataOutputStream buffer, final T value) throws Exception;
}
