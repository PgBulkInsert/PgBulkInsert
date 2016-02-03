// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.de.bytefish.pgbulkinsert.pgsql.handlers;

import java.io.DataOutputStream;

public class IntegerHandler extends BaseValueHandler<Integer> {

    @Override
    protected void internalHandle(DataOutputStream buffer, final Integer value) throws Exception {
        buffer.writeInt(4);
        buffer.writeInt(value);
    }
}
