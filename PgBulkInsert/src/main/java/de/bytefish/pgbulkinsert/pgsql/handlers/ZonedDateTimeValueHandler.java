package de.bytefish.pgbulkinsert.pgsql.handlers;

import de.bytefish.pgbulkinsert.pgsql.converter.IValueConverter;
import de.bytefish.pgbulkinsert.pgsql.converter.LocalDateTimeConverter;

import java.io.DataOutputStream;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

public class ZonedDateTimeValueHandler extends BaseValueHandler<ZonedDateTime> {
    private IValueConverter<ZonedDateTime, Long> dateTimeConverter;

    public ZonedDateTimeValueHandler() {
        this(new ToUTCStripTimezone());
    }

    public ZonedDateTimeValueHandler(IValueConverter<ZonedDateTime, Long> dateTimeConverter) {
        this.dateTimeConverter = dateTimeConverter;
    }

    @Override
    protected void internalHandle(DataOutputStream buffer, ZonedDateTime value) throws Exception {
        buffer.writeInt(8);
        buffer.writeLong(dateTimeConverter.convert(value));
    }

    private static final class ToUTCStripTimezone implements IValueConverter<ZonedDateTime, Long> {
        private final IValueConverter<LocalDateTime, Long> converter = new LocalDateTimeConverter();

        @Override
        public Long convert(final ZonedDateTime value) {
            return converter.convert(value.withZoneSameInstant(ZoneOffset.UTC).toLocalDateTime());
        }
    }
}