// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.pgsql.handlers;

import java.io.DataOutputStream;
import java.lang.reflect.Type;

public class BooleanValueHandler extends BaseValueHandler<Boolean> {

    @Override
    protected void internalHandle(DataOutputStream buffer, final Boolean value) throws Exception {
        buffer.writeInt(1);
        if (value) {
            buffer.writeByte(1);
        } else {
            buffer.writeByte(0);
        }
    }

    @Override
    public Type getTargetType() {
        return Boolean.class;
    }
}
