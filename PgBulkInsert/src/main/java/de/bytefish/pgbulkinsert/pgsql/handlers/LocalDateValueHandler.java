// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.pgsql.handlers;

import de.bytefish.pgbulkinsert.pgsql.converter.IValueConverter;
import de.bytefish.pgbulkinsert.pgsql.converter.LocalDateConverter;

import java.io.DataOutputStream;
import java.time.LocalDate;

public class LocalDateValueHandler extends BaseValueHandler<LocalDate> {

    private IValueConverter<LocalDate, Integer> dateConverter;

    public LocalDateValueHandler() {
        this(new LocalDateConverter());
    }

    public LocalDateValueHandler(IValueConverter<LocalDate, Integer> dateTimeConverter) {

        this.dateConverter = dateTimeConverter;
    }

    @Override
    protected void internalHandle(DataOutputStream buffer, final LocalDate value) throws Exception {
        buffer.writeInt(4);
        buffer.writeInt(dateConverter.convert(value));
    }
}
