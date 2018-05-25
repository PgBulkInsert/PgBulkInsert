// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.pgsql.handlers;

import de.bytefish.pgbulkinsert.pgsql.handlers.utils.GeometricUtils;
import de.bytefish.pgbulkinsert.pgsql.model.geometric.Box;

import java.io.DataOutputStream;

public class BoxValueHandler extends BaseValueHandler<Box> {

    @Override
    protected void internalHandle(DataOutputStream buffer, final Box value) throws Exception {
        buffer.writeInt(32);

        GeometricUtils.writePoint(buffer, value.getHigh());
        GeometricUtils.writePoint(buffer, value.getLow());
    }
}