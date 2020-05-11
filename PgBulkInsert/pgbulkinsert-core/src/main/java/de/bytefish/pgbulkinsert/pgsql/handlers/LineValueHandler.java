// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.pgsql.handlers;

import de.bytefish.pgbulkinsert.pgsql.model.geometric.Line;

import java.io.DataOutputStream;

public class LineValueHandler extends BaseValueHandler<Line> {

    @Override
    protected void internalHandle(DataOutputStream buffer, final Line value) throws Exception {
        buffer.writeInt(24);

        buffer.writeDouble(value.getA());
        buffer.writeDouble(value.getB());
        buffer.writeDouble(value.getC());
    }

    @Override
    public int getLength(Line value) {
        return 24;
    }
}