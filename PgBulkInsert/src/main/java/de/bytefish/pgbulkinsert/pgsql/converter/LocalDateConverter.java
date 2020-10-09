// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.pgsql.converter;

import de.bytefish.pgbulkinsert.pgsql.utils.TimeStampUtils;

import java.time.LocalDate;

public class LocalDateConverter implements IValueConverter<LocalDate, Integer> {

    @Override
    public Integer convert(final LocalDate date) {
        return TimeStampUtils.toPgDays(date);
    }

}
