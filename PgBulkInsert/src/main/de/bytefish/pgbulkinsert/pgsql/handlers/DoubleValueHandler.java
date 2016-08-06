// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.pgsql.handlers;

import de.bytefish.pgbulkinsert.pgsql.constants.DataType;

import java.io.DataOutputStream;
import java.lang.reflect.Type;

public class DoubleValueHandler extends BaseValueHandler<Double> {

    @Override
    protected void internalHandle(DataOutputStream buffer, final Double value) throws Exception {
        buffer.writeInt(8);
        buffer.writeDouble(value);
    }

    @Override
    public DataType getDataType() {
        return DataType.DoublePrecision;
    }
}
