// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.de.bytefish.pgbulkinsert;

import de.bytefish.pgbulkinsert.de.bytefish.pgbulkinsert.exceptions.SaveEntityFailedException;
import de.bytefish.pgbulkinsert.de.bytefish.pgbulkinsert.functional.Action2;
import de.bytefish.pgbulkinsert.de.bytefish.pgbulkinsert.functional.Func2;
import de.bytefish.pgbulkinsert.de.bytefish.pgbulkinsert.pgsql.PgBinaryWriter;
import de.bytefish.pgbulkinsert.de.bytefish.pgbulkinsert.util.StringUtils;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;
import org.postgresql.copy.PGCopyOutputStream;

import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class PgBulkInsert<TEntity> {

    private class TableDefinition {

        private String schema;

        private String tableName;

        public TableDefinition(String tableName) {
            this("", tableName);
        }

        public TableDefinition(String schema, String tableName) {
            this.schema = schema;
            this.tableName = tableName;
        }

        public String getSchema() {
            return schema;
        }

        public String getTableName() {
            return tableName;
        }

        public String GetFullQualifiedTableName() {
            if (StringUtils.isNullOrWhiteSpace(schema)) {
                return tableName;
            }
            return String.format("%1$s.%2$s", schema, tableName);
        }

        @Override
        public String toString() {
            return String.format("TableDefinition (Schema = {%1$s}, TableName = {%2$s})", schema, tableName);
        }
    }

    private class ColumnDefinition
    {
        private String columnName;

        private Action2<PgBinaryWriter, TEntity> write;

        public ColumnDefinition(String columnName, Action2<PgBinaryWriter, TEntity> write) {
            this.columnName = columnName;
            this.write = write;
        }

        public String getColumnName() {
            return columnName;
        }

        public Action2<PgBinaryWriter, TEntity> getWrite() {
            return write;
        }

        @Override
        public String toString()
        {
            return String.format("ColumnDefinition (ColumnName = {%1$s}, Serialize = {%2$s})", columnName, write);
        }
    }

    private TableDefinition table;

    private List<ColumnDefinition> columns;

    public PgBulkInsert(String schemaName, String tableName)
    {
        table = new TableDefinition(schemaName, tableName);
        columns = new ArrayList<>();
    }

    public void saveAll(PGConnection connection, Stream<TEntity> entities) throws SQLException {

        CopyManager cpManager = connection.getCopyAPI();
        CopyIn copyIn = cpManager.copyIn(getCopyCommand());

        int columnCount = columns.size();

        try (PgBinaryWriter bw = new PgBinaryWriter()) {

            // Wrap the CopyOutputStream in our own Writer:
            bw.open(new PGCopyOutputStream(copyIn));

            // Insert Each Column:
            entities.forEach(entity -> {
                // Start a New Row:
                bw.startRow(columnCount);

                columns.forEach(column -> {
                    try {
                        column.getWrite().invoke(bw, entity);
                    } catch (Exception e) {
                        throw new SaveEntityFailedException(e);
                    }
                });
            });
        }
    }

    protected void MapBoolean(String columnName, Func2<TEntity, Boolean> propertyGetter)
    {
        AddColumn(columnName, (binaryWriter, entity) -> {
            binaryWriter.write(propertyGetter.invoke(entity));
        });
    }

    protected void MapByte(String columnName, Func2<TEntity, Byte> propertyGetter)
    {
        AddColumn(columnName, (binaryWriter, entity) -> {
            binaryWriter.write(propertyGetter.invoke(entity));
        });
    }

    protected void MapSmallInt(String columnName, Func2<TEntity, Short> propertyGetter)
    {
        AddColumn(columnName, (binaryWriter, entity) -> {
            binaryWriter.write(propertyGetter.invoke(entity));
        });
    }

    protected void MapInteger(String columnName, Func2<TEntity, Integer> propertyGetter)
    {
        AddColumn(columnName, (binaryWriter, entity) -> {
            binaryWriter.write(propertyGetter.invoke(entity));
        });
    }

    protected void MapBigInt(String columnName, Func2<TEntity, Long> propertyGetter)
    {
        AddColumn(columnName, (binaryWriter, entity) -> {
            binaryWriter.write(propertyGetter.invoke(entity));
        });
    }

    protected void MapReal(String columnName, Func2<TEntity, Float> propertyGetter)
    {
        AddColumn(columnName, (binaryWriter, entity) -> {
            binaryWriter.write(propertyGetter.invoke(entity));
        });
    }

    protected void MapDouble(String columnName, Func2<TEntity, Double> propertyGetter)
    {
        AddColumn(columnName, (binaryWriter, entity) -> {
            binaryWriter.write(propertyGetter.invoke(entity));
        });
    }

    protected void MapDate(String columnName, Func2<TEntity, LocalDate> propertyGetter)
    {
        AddColumn(columnName, (binaryWriter, entity) -> {
            binaryWriter.write(propertyGetter.invoke(entity));
        });
    }

    protected void MapInet4Addr(String columnName, Func2<TEntity, Inet4Address> propertyGetter)
    {
        AddColumn(columnName, (binaryWriter, entity) -> {
            binaryWriter.write(propertyGetter.invoke(entity));
        });
    }

    protected void MapInet6Addr(String columnName, Func2<TEntity, Inet6Address> propertyGetter)
    {
        AddColumn(columnName, (binaryWriter, entity) -> {
            binaryWriter.write(propertyGetter.invoke(entity));
        });
    }

    protected void MapTimeStamp(String columnName, Func2<TEntity, LocalDateTime> propertyGetter)
    {
        AddColumn(columnName, (binaryWriter, entity) -> {
            binaryWriter.write(propertyGetter.invoke(entity));
        });
    }

    protected void MapString(String columnName, Func2<TEntity, String> propertyGetter)
    {
        AddColumn(columnName, (binaryWriter, entity) -> {
            binaryWriter.write(propertyGetter.invoke(entity));
        });
    }

    private PgBulkInsert<TEntity> AddColumn(String columnName, Action2<PgBinaryWriter, TEntity> action)
    {
        columns.add(new ColumnDefinition(columnName, action));

        return this;
    }

    private String getCopyCommand()
    {
        String commaSeparatedColumns = columns.stream()
                .map(x -> x.columnName)
                .collect(Collectors.joining(", "));

        return String.format("COPY %1$s(%2$s) FROM STDIN BINARY",
                table.GetFullQualifiedTableName(),
                commaSeparatedColumns);
    }
}
