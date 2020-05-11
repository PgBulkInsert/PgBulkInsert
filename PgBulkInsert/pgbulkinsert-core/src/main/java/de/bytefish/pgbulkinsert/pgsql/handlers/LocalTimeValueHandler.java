// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.pgsql.handlers;

import de.bytefish.pgbulkinsert.pgsql.converter.IValueConverter;
import de.bytefish.pgbulkinsert.pgsql.converter.LocalDateConverter;
import de.bytefish.pgbulkinsert.pgsql.utils.TimeStampUtils;

import java.io.DataOutputStream;
import java.time.LocalDate;
import java.time.LocalTime;

public class LocalTimeValueHandler extends BaseValueHandler<LocalTime> {

    public LocalTimeValueHandler() {

    }

    @Override
    protected void internalHandle(DataOutputStream buffer, final LocalTime value) throws Exception {
        buffer.writeInt(8);
        buffer.writeLong(TimeStampUtils.toMicroseconds(value));
    }

    @Override
    public int getLength(LocalTime value) {
        return 8;
    }
}
