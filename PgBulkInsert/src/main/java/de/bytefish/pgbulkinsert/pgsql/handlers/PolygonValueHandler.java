// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.pgsql.handlers;

import de.bytefish.pgbulkinsert.pgsql.handlers.utils.GeometricUtils;
import de.bytefish.pgbulkinsert.pgsql.model.geometric.Point;
import de.bytefish.pgbulkinsert.pgsql.model.geometric.Polygon;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.DataOutputStream;

public class PolygonValueHandler extends BaseValueHandler<Polygon> {

    @Override
    protected void internalHandle(DataOutputStream buffer, final Polygon value) throws Exception {
        // The total number of bytes to write:
        int totalBytesToWrite = 4 + 16 * value.size();

        // The Number of Bytes to follow:
        buffer.writeInt(totalBytesToWrite);

        // Write Points:
        buffer.writeInt(value.getPoints().size());

        // Write each Point in List:
        for (Point p : value.getPoints()) {
            GeometricUtils.writePoint(buffer, p);
        }

    }

    @Override
    public int getLength(Polygon value) {
        throw new NotImplementedException();
    }
}