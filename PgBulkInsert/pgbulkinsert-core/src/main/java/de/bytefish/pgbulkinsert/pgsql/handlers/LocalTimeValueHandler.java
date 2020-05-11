// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.pgsql.handlers;

import de.bytefish.pgbulkinsert.pgsql.converter.IValueConverter;
import de.bytefish.pgbulkinsert.pgsql.converter.LocalDateConverter;
import de.bytefish.pgbulkinsert.pgsql.converter.LocalTimeConverter;

import java.io.DataOutputStream;
import java.time.LocalDate;
import java.time.LocalTime;

public class LocalTimeValueHandler extends BaseValueHandler<LocalTime> {

    private IValueConverter<LocalTime, Long> timeConverter;

    public LocalTimeValueHandler() {
        this(new LocalTimeConverter());
    }

    public LocalTimeValueHandler(IValueConverter<LocalTime, Long> timeConverter) {
        this.timeConverter = timeConverter;
    }

    @Override
    protected void internalHandle(DataOutputStream buffer, final LocalTime value) throws Exception {
        buffer.writeInt(8);
        buffer.writeLong(timeConverter.convert(value));
    }

    @Override
    public int getLength(LocalTime value) {
        return 8;
    }
}
