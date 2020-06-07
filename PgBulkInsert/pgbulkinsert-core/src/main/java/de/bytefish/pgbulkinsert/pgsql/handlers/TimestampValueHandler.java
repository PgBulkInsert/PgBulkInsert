// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.pgsql.handlers;

import de.bytefish.pgbulkinsert.pgsql.converter.IValueConverter;
import de.bytefish.pgbulkinsert.pgsql.converter.LocalDateTimeConverter;

import java.io.DataOutputStream;
import java.sql.Timestamp;
import java.time.LocalDateTime;

public class TimestampValueHandler extends BaseValueHandler<Timestamp> {

    private IValueConverter<LocalDateTime, Long> dateTimeConverter;

    public TimestampValueHandler() {
        this(new LocalDateTimeConverter());
    }

    public TimestampValueHandler(IValueConverter<LocalDateTime, Long> dateTimeConverter) {
        this.dateTimeConverter = dateTimeConverter;
    }

    @Override
    protected void internalHandle(DataOutputStream buffer, final Timestamp value) throws Exception {
        buffer.writeInt(8);
        buffer.writeLong(dateTimeConverter.convert(value.toLocalDateTime()));
    }

    @Override
    public int getLength(Timestamp value) {
        return 8;
    }
}