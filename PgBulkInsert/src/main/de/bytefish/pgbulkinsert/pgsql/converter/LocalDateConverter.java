// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.de.bytefish.pgbulkinsert.pgsql.converter;

import de.bytefish.pgbulkinsert.de.bytefish.pgbulkinsert.pgsql.utils.TimeStampUtils;

import java.time.LocalDate;

public class LocalDateConverter implements IValueConverter<LocalDate, Long> {

    @Override
    public Long convert(final LocalDate date) {
        return TimeStampUtils.toPgSecs(date);
    }

}
