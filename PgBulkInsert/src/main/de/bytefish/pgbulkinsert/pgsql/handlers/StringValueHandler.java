// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.pgsql.handlers;

import java.io.DataOutputStream;
import java.lang.reflect.Type;

public class StringValueHandler extends BaseValueHandler<String> {

    @Override
    protected void internalHandle(DataOutputStream buffer, final String value) throws Exception {
        byte[] utf8Bytes = value.getBytes("UTF-8");

        buffer.writeInt(utf8Bytes.length);
        buffer.write(utf8Bytes);
    }

    @Override
    public Type getTargetType() {
        return String.class;
    }
}
