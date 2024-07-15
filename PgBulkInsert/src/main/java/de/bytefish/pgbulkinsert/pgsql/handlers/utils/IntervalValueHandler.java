// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.pgsql.handlers.utils;

import de.bytefish.pgbulkinsert.pgsql.handlers.BaseValueHandler;
import de.bytefish.pgbulkinsert.pgsql.model.geometric.Box;
import de.bytefish.pgbulkinsert.pgsql.model.interval.Interval;

import java.io.DataOutputStream;

public class IntervalValueHandler extends BaseValueHandler<Interval> {

    @Override
    protected void internalHandle(DataOutputStream buffer, final Interval value) throws Exception {
        buffer.writeInt(16);

        buffer.writeLong(value.getTime());
        buffer.writeInt(value.getDays());
        buffer.writeInt(value.getMonths());
    }

    @Override
    public int getLength(Interval value) {
        return 16;
    }
}