// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.pgsql.handlers;

import java.io.DataOutputStream;
import java.lang.reflect.Type;

public class ByteArrayValueHandler extends BaseValueHandler<Byte[]> {

    @Override
    protected void internalHandle(DataOutputStream buffer, final Byte[] value) throws Exception {
        buffer.writeInt(value.length);
        for(Byte b : value) {
            buffer.writeByte(b);
        }
    }

    @Override
    public Type getTargetType() {
        return Byte[].class;
    }
}
