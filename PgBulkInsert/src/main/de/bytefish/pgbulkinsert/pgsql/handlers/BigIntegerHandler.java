// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.de.bytefish.pgbulkinsert.pgsql.handlers;

import java.io.DataOutputStream;
import java.math.BigInteger;

public class BigIntegerHandler extends BaseValueHandler<BigInteger> {

    @Override
    protected void internalHandle(DataOutputStream buffer, final BigInteger value) throws Exception {
        Long longValue = value.longValue();

        buffer.writeInt(8);
        buffer.writeLong(longValue);
    }
}
