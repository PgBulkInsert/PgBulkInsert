// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert;

import de.bytefish.pgbulkinsert.exceptions.SaveEntityFailedException;
import de.bytefish.pgbulkinsert.functional.Action2;
import de.bytefish.pgbulkinsert.functional.Func2;
import de.bytefish.pgbulkinsert.model.ColumnDefinition;
import de.bytefish.pgbulkinsert.model.TableDefinition;
import de.bytefish.pgbulkinsert.pgsql.PgBinaryWriter;
import de.bytefish.pgbulkinsert.pgsql.constants.DataType;
import de.bytefish.pgbulkinsert.pgsql.constants.ObjectIdentifier;
import de.bytefish.pgbulkinsert.pgsql.handlers.CollectionValueHandler;
import de.bytefish.pgbulkinsert.pgsql.handlers.IValueHandler;
import de.bytefish.pgbulkinsert.pgsql.handlers.IValueHandlerProvider;
import de.bytefish.pgbulkinsert.pgsql.handlers.ValueHandlerProvider;
import de.bytefish.pgbulkinsert.pgsql.model.geometric.*;
import de.bytefish.pgbulkinsert.pgsql.model.network.MacAddress;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;
import org.postgresql.copy.PGCopyOutputStream;

import java.math.BigDecimal;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class PgBulkInsert<TEntity> implements IPgBulkInsert<TEntity> {

    private IValueHandlerProvider provider;


    private TableDefinition table;

    private List<ColumnDefinition<TEntity>> columns;

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

        try (PgBinaryWriter bw = new PgBinaryWriter()) {

            // Wrap the CopyOutputStream in our own Writer:
            bw.open(new PGCopyOutputStream(copyIn));

            // Insert Each Column:
            entities.forEach(entity -> this.saveEntity(bw, entity));
        }
    }

    private void saveEntity(PgBinaryWriter bw, TEntity entity) throws SaveEntityFailedException {
        synchronized (bw) {
            // Start a New Row:
            bw.startRow(columns.size());

            // Iterate over each column mapping:
            columns.forEach(column -> {
                try {
                    column.getWrite().invoke(bw, entity);
                } catch (Exception e) {
                    throw new SaveEntityFailedException(e);
                }
            });
        }
    }

    protected <TElementType, TCollectionType extends Collection<TElementType>> void mapCollection(String columnName, DataType dataType, Func2<TEntity, TCollectionType> propertyGetter) {

        final IValueHandler<TElementType> valueHandler = provider.resolve(dataType);
        final int valueOID = ObjectIdentifier.mapFrom(dataType);

        map(columnName, new CollectionValueHandler<>(valueOID, valueHandler), propertyGetter);
    }

    protected <TProperty> void map(String columnName, DataType dataType, Func2<TEntity, TProperty> propertyGetter)
    {
        final IValueHandler<TProperty> valueHandler = provider.resolve(dataType);

        map(columnName, valueHandler, propertyGetter);
    }

    protected <TProperty> void map(String columnName, IValueHandler<TProperty> valueHandler, Func2<TEntity, TProperty> propertyGetter) {
        addColumn(columnName, (binaryWriter, entity) -> {
            binaryWriter.write(valueHandler, propertyGetter.invoke(entity));
        });
    }

    protected void mapBoolean(String columnName, Func2<TEntity, Boolean> propertyGetter)
    {
       map(columnName, DataType.Boolean, propertyGetter);
    }

    protected void mapByte(String columnName, Func2<TEntity, Byte> propertyGetter)
    {
        map(columnName, DataType.Char, propertyGetter);
    }

    protected void mapSmallInt(String columnName, Func2<TEntity, Short> propertyGetter)
    {

        map(columnName, DataType.Int2, propertyGetter);
    }

    protected void mapInteger(String columnName, Func2<TEntity, Integer> propertyGetter)
    {
        map(columnName, DataType.Int4, propertyGetter);
    }

    protected void mapNumeric(String columnName, Func2<TEntity, BigDecimal> propertyGetter)
    {
        map(columnName, DataType.Numeric, propertyGetter);
    }

    protected void mapLong(String columnName, Func2<TEntity, Long> propertyGetter)
    {
        map(columnName, DataType.Int8, propertyGetter);
    }

    protected void mapReal(String columnName, Func2<TEntity, Float> propertyGetter)
    {
        map(columnName, DataType.SinglePrecision, propertyGetter);
    }

    protected void mapDouble(String columnName, Func2<TEntity, Double> propertyGetter)
    {
        map(columnName, DataType.DoublePrecision, propertyGetter);
    }

    protected void mapDate(String columnName, Func2<TEntity, LocalDate> propertyGetter)
    {
        map(columnName, DataType.Date, propertyGetter);
    }

    protected void mapInet4Addr(String columnName, Func2<TEntity, Inet4Address> propertyGetter)
    {
        map(columnName, DataType.Inet4, propertyGetter);
    }

    protected void mapInet6Addr(String columnName, Func2<TEntity, Inet6Address> propertyGetter)
    {
        map(columnName, DataType.Inet6, propertyGetter);
    }

    protected void mapTimeStamp(String columnName, Func2<TEntity, LocalDateTime> propertyGetter)
    {
        map(columnName, DataType.Timestamp, propertyGetter);
    }

    protected void mapString(String columnName, Func2<TEntity, String> propertyGetter)
    {
        map(columnName, DataType.Text, propertyGetter);
    }

    protected void mapUUID(String columnName, Func2<TEntity, UUID> propertyGetter) {
        map(columnName, DataType.Uuid, propertyGetter);
    }

    protected void mapByteArray(String columnName, Func2<TEntity, Byte[]> propertyGetter) {
        map(columnName, DataType.Bytea, propertyGetter);
    }

    protected void mapJsonb(String columnName, Func2<TEntity, String> propertyGetter) {
        map(columnName, DataType.Jsonb, propertyGetter);
    }

    protected void mapHstore(String columnName, Func2<TEntity, Map<String, String>> propertyGetter) {
        map(columnName, DataType.Hstore, propertyGetter);
    }

    protected void mapPoint(String columnName, Func2<TEntity, Point> propertyGetter) {
        map(columnName, DataType.Point, propertyGetter);
    }

    protected void mapBox(String columnName, Func2<TEntity, Box> propertyGetter) {
        map(columnName, DataType.Box, propertyGetter);
    }

    protected void mapPath(String columnName, Func2<TEntity, Path> propertyGetter) {
        map(columnName, DataType.Path, propertyGetter);
    }

    protected void mapPolygon(String columnName, Func2<TEntity, Polygon> propertyGetter) {
        map(columnName, DataType.Polygon, propertyGetter);
    }

    protected void mapLine(String columnName, Func2<TEntity, Line> propertyGetter) {
        map(columnName, DataType.Line, propertyGetter);
    }

    protected void mapLineSegment(String columnName, Func2<TEntity, LineSegment> propertyGetter) {
        map(columnName, DataType.LineSegment, propertyGetter);
    }

    protected void mapCircle(String columnName, Func2<TEntity, Circle> propertyGetter) {
        map(columnName, DataType.Circle, propertyGetter);
    }

    protected void mapMacAddress(String columnName, Func2<TEntity, MacAddress> propertyGetter) {
        map(columnName, DataType.MacAddress, propertyGetter);
    }

    protected <TCollectionType extends Collection<Boolean>> void mapBooleanArray(String columnName, Func2<TEntity, TCollectionType> propertyGetter) {
        mapCollection(columnName, DataType.Boolean, propertyGetter);
    }

    protected <TCollectionType extends Collection<Short>> void mapShortArray(String columnName, Func2<TEntity, TCollectionType> propertyGetter) {
        mapCollection(columnName, DataType.Int2, propertyGetter);
    }

    protected <TCollectionType extends Collection<Integer>> void mapIntegerArray(String columnName, Func2<TEntity, TCollectionType> propertyGetter) {
        mapCollection(columnName, DataType.Int4, propertyGetter);
    }

    protected <TCollectionType extends Collection<Long>> void mapLongArray(String columnName, Func2<TEntity, TCollectionType> propertyGetter) {
        mapCollection(columnName, DataType.Int8, propertyGetter);
    }

    protected <TCollectionType extends Collection<String>> void mapStringArray(String columnName, Func2<TEntity, TCollectionType> propertyGetter) {
        mapCollection(columnName, DataType.Text, propertyGetter);
    }

    protected <TCollectionType extends Collection<Float>> void mapFloatArray(String columnName, Func2<TEntity, TCollectionType> propertyGetter) {
        mapCollection(columnName, DataType.SinglePrecision, propertyGetter);
    }

    protected <TCollectionType extends Collection<Double>> void mapDoubleArray(String columnName, Func2<TEntity, TCollectionType> propertyGetter) {
        mapCollection(columnName, DataType.DoublePrecision, propertyGetter);
    }

    protected <TCollectionType extends Collection<BigDecimal>> void mapNumericArray(String columnName, Func2<TEntity, TCollectionType> propertyGetter) {
        mapCollection(columnName, DataType.Numeric, propertyGetter);
    }

    protected <TCollectionType extends Collection<UUID>> void mapUUIDArray(String columnName, Func2<TEntity, TCollectionType> propertyGetter) {
        mapCollection(columnName, DataType.Uuid, propertyGetter);
    }

    protected <TCollectionType extends Collection<Inet4Address>> void mapInet4Array(String columnName, Func2<TEntity, TCollectionType> propertyGetter) {
        mapCollection(columnName, DataType.Inet4, propertyGetter);
    }

    protected <TCollectionType extends Collection<Inet6Address>> void mapInet6Array(String columnName, Func2<TEntity, TCollectionType> propertyGetter) {
        mapCollection(columnName, DataType.Inet6, propertyGetter);
    }

    private PgBulkInsert<TEntity> addColumn(String columnName, Action2<PgBinaryWriter, TEntity> action)
    {
        columns.add(new ColumnDefinition(columnName, action));

        return this;
    }

    private String getCopyCommand()
    {
        String commaSeparatedColumns = columns.stream()
                .map(x -> x.getColumnName())
                .collect(Collectors.joining(", "));

        return String.format("COPY %1$s(%2$s) FROM STDIN BINARY",
                table.GetFullQualifiedTableName(),
                commaSeparatedColumns);
    }
}
