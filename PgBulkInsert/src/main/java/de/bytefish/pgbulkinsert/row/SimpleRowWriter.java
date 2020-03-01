/*******************************************************************************
 *  Copyright (c) 2019, 2020 lucendar.com.
 *  All rights reserved.
 *
 *  Contributors:
 *     KwanKin Yau (alphax@vip.163.com) - initial API and implementation
 *******************************************************************************/
package de.bytefish.pgbulkinsert.row;

import de.bytefish.pgbulkinsert.pgsql.handlers.IValueHandlerProvider;

import java.util.Map;

public class SimpleRowWriter extends RowWriter<SimpleRow> {

    public SimpleRowWriter(Table table, IValueHandlerProvider valueHandlerProvider) {
        super(table, valueHandlerProvider);
    }

    public SimpleRowWriter(Table table) {
        super(table);
    }

    @Override
    protected SimpleRow createRow(IValueHandlerProvider provider, Map<String, Integer> lookup) {
        return new SimpleRow(provider, lookup);
    }
}
