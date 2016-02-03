// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.de.bytefish.pgbulkinsert.pgsql.handlers;

import de.bytefish.pgbulkinsert.de.bytefish.pgbulkinsert.pgsql.converter.IValueConverter;
import de.bytefish.pgbulkinsert.de.bytefish.pgbulkinsert.pgsql.converter.LocalDateConverter;

import java.io.DataOutputStream;
import java.time.LocalDate;

public class LocalDateHandler extends BaseValueHandler<LocalDate> {

    private IValueConverter<LocalDate, Long> dateTimeConverter;

    public LocalDateHandler() {
        this(new LocalDateConverter());
    }

    public LocalDateHandler(IValueConverter<LocalDate, Long> dateTimeConverter) {

        this.dateTimeConverter = dateTimeConverter;
    }

    @Override
    protected void internalHandle(DataOutputStream buffer, final LocalDate value) throws Exception {
        buffer.writeInt(8);
        buffer.writeLong(dateTimeConverter.convert(value));
    }

}
