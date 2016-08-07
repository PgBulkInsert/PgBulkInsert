// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.pgsql.handlers;

import de.bytefish.pgbulkinsert.pgsql.constants.DataType;
import de.bytefish.pgbulkinsert.pgsql.handlers.utils.GeometricUtils;
import de.bytefish.pgbulkinsert.pgsql.model.geometric.Circle;

import java.io.DataOutputStream;

public class CircleValueHandler extends BaseValueHandler<Circle> {

    @Override
    protected void internalHandle(DataOutputStream buffer, final Circle value) throws Exception {
        buffer.writeInt(24);
        // First encode the Center Point:
        GeometricUtils.writePoint(buffer, value.getCenter());
        // ... and then the Radius:
        buffer.writeDouble(value.getRadius());
    }

    @Override
    public DataType getDataType() {
        return DataType.Circle;
    }
}