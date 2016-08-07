// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.pgsql.handlers;

import de.bytefish.pgbulkinsert.pgsql.constants.DataType;
import de.bytefish.pgbulkinsert.pgsql.handlers.utils.GeometricUtils;
import de.bytefish.pgbulkinsert.pgsql.model.geometric.Point;

import java.io.DataOutputStream;

public class PointValueHandler extends BaseValueHandler<Point> {

    @Override
    protected void internalHandle(DataOutputStream buffer, final Point value) throws Exception {
        buffer.writeInt(16);

        GeometricUtils.writePoint(buffer, value);
    }

    @Override
    public DataType getDataType() {
        return DataType.Point;
    }
}