// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.pgsql.handlers;

import java.io.DataOutputStream;
import java.lang.reflect.Type;

public class ShortValueHandler extends BaseValueHandler<Short> {

    @Override
    protected void internalHandle(DataOutputStream buffer, final Short value) throws Exception {
        buffer.writeInt(2);
        buffer.writeShort(value);
    }

    @Override
    public Type getTargetType() {
        return Short.class;
    }
}
