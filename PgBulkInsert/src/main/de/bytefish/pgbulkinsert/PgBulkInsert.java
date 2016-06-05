// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.de.bytefish.pgbulkinsert;

import de.bytefish.pgbulkinsert.de.bytefish.pgbulkinsert.exceptions.SaveEntityFailedException;
import de.bytefish.pgbulkinsert.de.bytefish.pgbulkinsert.functional.Action2;
import de.bytefish.pgbulkinsert.de.bytefish.pgbulkinsert.functional.Func2;
import de.bytefish.pgbulkinsert.de.bytefish.pgbulkinsert.pgsql.PgBinaryWriter;
import de.bytefish.pgbulkinsert.de.bytefish.pgbulkinsert.pgsql.constants.PgTypes;
import de.bytefish.pgbulkinsert.de.bytefish.pgbulkinsert.pgsql.handlers.CollectionValueHandler;
import de.bytefish.pgbulkinsert.de.bytefish.pgbulkinsert.pgsql.handlers.IValueHandler;
import de.bytefish.pgbulkinsert.de.bytefish.pgbulkinsert.pgsql.handlers.IValueHandlerProvider;
import de.bytefish.pgbulkinsert.de.bytefish.pgbulkinsert.pgsql.handlers.ValueHandlerProvider;
import de.bytefish.pgbulkinsert.de.bytefish.pgbulkinsert.util.StringUtils;

import org.postgresql.PGConnection;
import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;
import org.postgresql.copy.PGCopyOutputStream;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
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

    private IValueHandlerProvider provider;

    private TableDefinition table;

    private List<ColumnDefinition> columns;

    public PgBulkInsert(String schemaName, String tableName)
    {
        this(new ValueHandlerProvider(), schemaName, tableName);
    }

    public PgBulkInsert(IValueHandlerProvider provider, String schemaName, String tableName)
    {
        this.provider = provider;
        this.table = new TableDefinition(schemaName, tableName);
        this.columns = new ArrayList<>();
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

                // Iterate over each column mapping:
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

    protected <TElementType, TCollectionType extends Collection<TElementType>> void mapCollection(String columnName, Class<TElementType> type, Func2<TEntity, TCollectionType> propertyGetter) {

        final IValueHandler<TElementType> valueHandler = provider.resolve(type);
        final int valueOID = PgTypes.mapFrom(type);

        map(columnName, new CollectionValueHandler<>(type, valueOID, valueHandler), propertyGetter);
    }

    protected <TProperty> void map(String columnName, Class<TProperty> type, Func2<TEntity, TProperty> propertyGetter)
    {
        final IValueHandler<TProperty> valueHandler = provider.resolve(type);

        map(columnName, valueHandler, propertyGetter);
    }

    protected <TProperty> void map(String columnName, IValueHandler<TProperty> valueHandler, Func2<TEntity, TProperty> propertyGetter) {
        addColumn(columnName, (binaryWriter, entity) -> {
            binaryWriter.write(valueHandler, propertyGetter.invoke(entity));
        });
    }

    protected void mapBoolean(String columnName, Func2<TEntity, Boolean> propertyGetter)
    {
       map(columnName, Boolean.class, propertyGetter);
    }

    protected void mapByte(String columnName, Func2<TEntity, Byte> propertyGetter)
    {
        map(columnName, Byte.class, propertyGetter);
    }

    protected void mapSmallInt(String columnName, Func2<TEntity, Short> propertyGetter)
    {

        map(columnName, Short.class, propertyGetter);
    }

    protected void mapInteger(String columnName, Func2<TEntity, Integer> propertyGetter)
    {
        map(columnName, Integer.class, propertyGetter);
    }

    protected void mapBigInt(String columnName, Func2<TEntity, Long> propertyGetter)
    {
        map(columnName, Long.class, propertyGetter);
    }

    protected void mapReal(String columnName, Func2<TEntity, Float> propertyGetter)
    {
        map(columnName, Float.class, propertyGetter);
    }

    protected void mapDouble(String columnName, Func2<TEntity, Double> propertyGetter)
    {
        map(columnName, Double.class, propertyGetter);
    }

    protected void mapDate(String columnName, Func2<TEntity, LocalDate> propertyGetter)
    {
        map(columnName, LocalDate.class, propertyGetter);
    }

    protected void mapInet4Addr(String columnName, Func2<TEntity, Inet4Address> propertyGetter)
    {
        map(columnName, Inet4Address.class, propertyGetter);
    }

    protected void mapInet6Addr(String columnName, Func2<TEntity, Inet6Address> propertyGetter)
    {
        map(columnName, Inet6Address.class, propertyGetter);
    }

    protected void mapTimeStamp(String columnName, Func2<TEntity, LocalDateTime> propertyGetter)
    {
        map(columnName, LocalDateTime.class, propertyGetter);
    }

    protected void mapString(String columnName, Func2<TEntity, String> propertyGetter)
    {
        map(columnName, String.class, propertyGetter);
    }

    protected void mapUUID(String columnName, Func2<TEntity, UUID> propertyGetter) {
        map(columnName, UUID.class, propertyGetter);
    }

    protected void mapByteArray(String columnName, Func2<TEntity, Byte[]> propertyGetter) {
        map(columnName, Byte[].class, propertyGetter);
    }

    protected <TCollectionType extends Collection<Boolean>> void mapBooleanArray(String columnName, Func2<TEntity, TCollectionType> propertyGetter) {
        mapCollection(columnName, Boolean.class, propertyGetter);
    }

    protected <TCollectionType extends Collection<Short>> void mapShortArray(String columnName, Func2<TEntity, TCollectionType> propertyGetter) {
        mapCollection(columnName, Short.class, propertyGetter);
    }

    protected <TCollectionType extends Collection<Integer>> void mapIntegerArray(String columnName, Func2<TEntity, TCollectionType> propertyGetter) {
        mapCollection(columnName, Integer.class, propertyGetter);
    }

    protected <TCollectionType extends Collection<Long>> void mapLongArray(String columnName, Func2<TEntity, TCollectionType> propertyGetter) {
        mapCollection(columnName, Long.class, propertyGetter);
    }

    protected <TCollectionType extends Collection<String>> void mapStringArray(String columnName, Func2<TEntity, TCollectionType> propertyGetter) {
        mapCollection(columnName, String.class, propertyGetter);
    }

    protected <TCollectionType extends Collection<Float>> void mapFloatArray(String columnName, Func2<TEntity, TCollectionType> propertyGetter) {
        mapCollection(columnName, Float.class, propertyGetter);
    }

    protected <TCollectionType extends Collection<Double>> void mapDoubleArray(String columnName, Func2<TEntity, TCollectionType> propertyGetter) {
        mapCollection(columnName, Double.class, propertyGetter);
    }

    protected <TCollectionType extends Collection<UUID>> void mapUUIDArray(String columnName, Func2<TEntity, TCollectionType> propertyGetter) {
        mapCollection(columnName, UUID.class, propertyGetter);
    }

    protected <TCollectionType extends Collection<Inet4Address>> void mapInet4Array(String columnName, Func2<TEntity, TCollectionType> propertyGetter) {
        mapCollection(columnName, Inet4Address.class, propertyGetter);
    }

    protected <TCollectionType extends Collection<Inet6Address>> void mapInet6Array(String columnName, Func2<TEntity, TCollectionType> propertyGetter) {
        mapCollection(columnName, Inet6Address.class, propertyGetter);
    }

    private PgBulkInsert<TEntity> addColumn(String columnName, Action2<PgBinaryWriter, TEntity> action)
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
