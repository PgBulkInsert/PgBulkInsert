// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.model;

import java.util.function.BiConsumer;

import de.bytefish.pgbulkinsert.pgsql.PgBinaryWriter;

public class ColumnDefinition<TEntity>
{
    private final String columnName;

    private final BiConsumer<PgBinaryWriter, TEntity> write;

    public ColumnDefinition(String columnName, BiConsumer<PgBinaryWriter, TEntity> write) {
        this.columnName = columnName;
        this.write = write;
    }

    public String getColumnName() {
        return columnName;
    }

    public BiConsumer<PgBinaryWriter, TEntity> getWrite() {
        return write;
    }

    @Override
    public String toString()
    {
        return String.format("ColumnDefinition (ColumnName = {%1$s}, Serialize = {%2$s})", columnName, write);
    }
}