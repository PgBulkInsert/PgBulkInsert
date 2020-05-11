// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.mapping;

import de.bytefish.pgbulkinsert.function.ToBooleanFunction;
import de.bytefish.pgbulkinsert.function.ToFloatFunction;
import de.bytefish.pgbulkinsert.model.ColumnDefinition;
import de.bytefish.pgbulkinsert.model.TableDefinition;
import de.bytefish.pgbulkinsert.pgsql.PgBinaryWriter;
import de.bytefish.pgbulkinsert.pgsql.constants.DataType;
import de.bytefish.pgbulkinsert.pgsql.constants.ObjectIdentifier;
import de.bytefish.pgbulkinsert.pgsql.handlers.*;
import de.bytefish.pgbulkinsert.pgsql.model.geometric.*;
import de.bytefish.pgbulkinsert.pgsql.model.network.MacAddress;
import de.bytefish.pgbulkinsert.pgsql.model.range.Range;
import de.bytefish.pgbulkinsert.util.PostgreSqlUtils;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;

public abstract class AbstractMapping<TEntity> {

    protected boolean usePostgresQuoting;

    protected final IValueHandlerProvider provider;

    protected final TableDefinition table;

    protected final List<ColumnDefinition<TEntity>> columns;

    protected AbstractMapping(String schemaName, String tableName) {
        this(new ValueHandlerProvider(), schemaName, tableName, false);
    }

    protected AbstractMapping(String schemaName, String tableName, boolean usePostgresQuoting) {
        this(new ValueHandlerProvider(), schemaName, tableName, usePostgresQuoting);
    }

    protected AbstractMapping(IValueHandlerProvider provider, String schemaName, String tableName, boolean usePostgresQuoting) {
        this.provider = provider;
        this.table = new TableDefinition(schemaName, tableName);
        this.usePostgresQuoting = usePostgresQuoting;
        this.columns = new ArrayList<>();
    }

    protected void usePostgresQuoting(boolean enabled) {
        this.usePostgresQuoting = enabled;
    }

    protected <TElementType, TCollectionType extends Collection<TElementType>> void mapCollection(String columnName, DataType dataType, Function<TEntity, TCollectionType> propertyGetter) {

        final IValueHandler<TElementType> valueHandler = provider.resolve(dataType);
        final int valueOID = ObjectIdentifier.mapFrom(dataType);

        map(columnName, new CollectionValueHandler<>(valueOID, valueHandler), propertyGetter);
    }

    protected <TProperty> void map(String columnName, DataType dataType, Function<TEntity, TProperty> propertyGetter) {
        final IValueHandler<TProperty> valueHandler = provider.resolve(dataType);

        map(columnName, valueHandler, propertyGetter);
    }

    protected <TProperty> void map(String columnName, IValueHandler<TProperty> valueHandler, Function<TEntity, TProperty> propertyGetter) {
        addColumn(columnName, (binaryWriter, entity) -> {
            binaryWriter.write(valueHandler, propertyGetter.apply(entity));
        });
    }

    // region Numeric

    protected void mapBoolean(String columnName, Function<TEntity, Boolean> propertyGetter) {
        map(columnName, DataType.Boolean, propertyGetter);
    }

    protected void mapBoolean(String columnName, ToBooleanFunction<TEntity> propertyGetter) {
        addColumn(columnName, (binaryWriter, entity) -> {
            binaryWriter.writeBoolean(propertyGetter.applyAsBoolean(entity));
        });
    }

    protected void mapByte(String columnName, Function<TEntity, Number> propertyGetter) {
        map(columnName, DataType.Char, propertyGetter);
    }

    protected void mapByte(String columnName, ToIntFunction<TEntity> propertyGetter) {
        addColumn(columnName, (binaryWriter, entity) -> {
            binaryWriter.writeByte(propertyGetter.applyAsInt(entity));
        });
    }

    protected void mapShort(String columnName, Function<TEntity, Number> propertyGetter) {
        map(columnName, DataType.Int2, propertyGetter);
    }

    protected void mapShort(String columnName, ToIntFunction<TEntity> propertyGetter) {
        addColumn(columnName, (binaryWriter, entity) -> {
            binaryWriter.writeShort(propertyGetter.applyAsInt(entity));
        });
    }

    protected void mapInteger(String columnName, Function<TEntity, Number> propertyGetter) {
        map(columnName, DataType.Int4, propertyGetter);
    }

    protected void mapInteger(String columnName, ToIntFunction<TEntity> propertyGetter) {
        addColumn(columnName, (binaryWriter, entity) -> {
            binaryWriter.writeInt(propertyGetter.applyAsInt(entity));
        });
    }

    protected void mapNumeric(String columnName, Function<TEntity, Number> propertyGetter) {
        map(columnName, DataType.Numeric, propertyGetter);
    }

    protected void mapLong(String columnName, Function<TEntity, Number> propertyGetter) {
        map(columnName, DataType.Int8, propertyGetter);
    }

    protected void mapLong(String columnName, ToLongFunction<TEntity> propertyGetter) {
        addColumn(columnName, (binaryWriter, entity) -> {
            binaryWriter.writeLong(propertyGetter.applyAsLong(entity));
        });
    }

    protected void mapFloat(String columnName, Function<TEntity, Number> propertyGetter) {
        map(columnName, DataType.SinglePrecision, propertyGetter);
    }

    protected void mapFloat(String columnName, ToFloatFunction<TEntity> propertyGetter) {
        addColumn(columnName, (binaryWriter, entity) -> {
            binaryWriter.writeFloat(propertyGetter.applyAsFloat(entity));
        });
    }

    protected void mapDouble(String columnName, Function<TEntity, Number> propertyGetter) {
        map(columnName, DataType.DoublePrecision, propertyGetter);
    }

    protected void mapDouble(String columnName, ToDoubleFunction<TEntity> propertyGetter) {
        addColumn(columnName, (binaryWriter, entity) -> {
            binaryWriter.writeDouble(propertyGetter.applyAsDouble(entity));
        });
    }

    // endregion

    // region Network
    protected void mapInet4Addr(String columnName, Function<TEntity, Inet4Address> propertyGetter) {
        map(columnName, DataType.Inet4, propertyGetter);
    }

    protected void mapInet6Addr(String columnName, Function<TEntity, Inet6Address> propertyGetter) {
        map(columnName, DataType.Inet6, propertyGetter);
    }

    protected void mapMacAddress(String columnName, Function<TEntity, MacAddress> propertyGetter) {
        map(columnName, DataType.MacAddress, propertyGetter);
    }

    // endregion

    // region Temporal

    protected void mapDate(String columnName, Function<TEntity, LocalDate> propertyGetter) {
        map(columnName, DataType.Date, propertyGetter);
    }

    protected void mapTime(String columnName, Function<TEntity, LocalTime> propertyGetter) {
        map(columnName, DataType.Time, propertyGetter);
    }

    protected void mapTimeStamp(String columnName, Function<TEntity, LocalDateTime> propertyGetter) {
        map(columnName, DataType.Timestamp, propertyGetter);
    }

    protected void mapTimeStampTz(String columnName, Function<TEntity, ZonedDateTime> propertyGetter) {
        map(columnName, DataType.TimestampTz, propertyGetter);
    }

    // endregion

    // region Text

    protected void mapText(String columnName, Function<TEntity, String> propertyGetter) {
        map(columnName, DataType.Text, propertyGetter);
    }

    protected void mapVarChar(String columnName, Function<TEntity, String> propertyGetter) {
        map(columnName, DataType.Text, propertyGetter);
    }

    // engregion

    // region UUID

    protected void mapUUID(String columnName, Function<TEntity, UUID> propertyGetter) {
        map(columnName, DataType.Uuid, propertyGetter);
    }

    // endregion

    // region JSON

    protected void mapJsonb(String columnName, Function<TEntity, String> propertyGetter) {
        map(columnName, DataType.Jsonb, propertyGetter);
    }

    // endregion

    // region hstore

    protected void mapHstore(String columnName, Function<TEntity, Map<String, String>> propertyGetter) {
        map(columnName, DataType.Hstore, propertyGetter);
    }

    // endregion

    // region Geo

    protected void mapPoint(String columnName, Function<TEntity, Point> propertyGetter) {
        map(columnName, DataType.Point, propertyGetter);
    }

    protected void mapBox(String columnName, Function<TEntity, Box> propertyGetter) {
        map(columnName, DataType.Box, propertyGetter);
    }

    protected void mapPath(String columnName, Function<TEntity, Path> propertyGetter) {
        map(columnName, DataType.Path, propertyGetter);
    }

    protected void mapPolygon(String columnName, Function<TEntity, Polygon> propertyGetter) {
        map(columnName, DataType.Polygon, propertyGetter);
    }

    protected void mapLine(String columnName, Function<TEntity, Line> propertyGetter) {
        map(columnName, DataType.Line, propertyGetter);
    }

    protected void mapLineSegment(String columnName, Function<TEntity, LineSegment> propertyGetter) {
        map(columnName, DataType.LineSegment, propertyGetter);
    }

    protected void mapCircle(String columnName, Function<TEntity, Circle> propertyGetter) {
        map(columnName, DataType.Circle, propertyGetter);
    }

    // endregion

    // region Arrays

    protected void mapBooleanArray(String columnName, Function<TEntity, Collection<Boolean>> propertyGetter) {
        mapCollection(columnName, DataType.Boolean, propertyGetter);
    }

    protected void mapByteArray(String columnName, Function<TEntity, byte[]> propertyGetter) {
        map(columnName, DataType.Bytea, propertyGetter);
    }

    protected <T extends Number> void mapShortArray(String columnName, Function<TEntity, Collection<T>> propertyGetter) {
        mapCollection(columnName, DataType.Int2, propertyGetter);
    }

    protected <T extends Number> void mapIntegerArray(String columnName, Function<TEntity, Collection<T>> propertyGetter) {
        mapCollection(columnName, DataType.Int4, propertyGetter);
    }

    protected <T extends Number> void mapLongArray(String columnName, Function<TEntity, Collection<T>> propertyGetter) {
        mapCollection(columnName, DataType.Int8, propertyGetter);
    }

    protected void mapTextArray(String columnName, Function<TEntity, Collection<String>> propertyGetter) {
        mapCollection(columnName, DataType.Text, propertyGetter);
    }

    protected void mapVarCharArray(String columnName, Function<TEntity, Collection<String>> propertyGetter) {
        mapCollection(columnName, DataType.VarChar, propertyGetter);
    }

    protected <T extends Number> void mapFloatArray(String columnName, Function<TEntity, Collection<T>> propertyGetter) {
        mapCollection(columnName, DataType.SinglePrecision, propertyGetter);
    }

    protected <T extends Number> void mapDoubleArray(String columnName, Function<TEntity, Collection<T>> propertyGetter) {
        mapCollection(columnName, DataType.DoublePrecision, propertyGetter);
    }

    protected <T extends Number> void mapNumericArray(String columnName, Function<TEntity, Collection<T>> propertyGetter) {
        mapCollection(columnName, DataType.Numeric, propertyGetter);
    }

    protected void mapUUIDArray(String columnName, Function<TEntity, Collection<UUID>> propertyGetter) {
        mapCollection(columnName, DataType.Uuid, propertyGetter);
    }

    protected void mapInet4Array(String columnName, Function<TEntity, Collection<Inet4Address>> propertyGetter) {
        mapCollection(columnName, DataType.Inet4, propertyGetter);
    }

    protected void mapInet6Array(String columnName, Function<TEntity, Collection<Inet6Address>> propertyGetter) {
        mapCollection(columnName, DataType.Inet6, propertyGetter);
    }

    // endregion

    // region Ranges

    protected <TElementType> void mapRange(String columnName, DataType dataType, Function<TEntity, Range<TElementType>> propertyGetter) {
        final IValueHandler<TElementType> valueHandler = provider.resolve(dataType);

        map(columnName, new RangeValueHandler<>(valueHandler), propertyGetter);
    }

    protected void mapTsRange(String columnName, Function<TEntity, Range<LocalDateTime>> propertyGetter) {
        map(columnName, DataType.TsRange, propertyGetter);
    }

    protected void mapTsTzRange(String columnName, Function<TEntity, Range<ZonedDateTime>> propertyGetter) {
        map(columnName, DataType.TsTzRange, propertyGetter);
    }

    protected void mapInt4Range(String columnName, Function<TEntity, Range<Integer>> propertyGetter) {
        map(columnName, DataType.Int4Range, propertyGetter);
    }

    protected void mapInt8Range(String columnName, Function<TEntity, Range<Long>> propertyGetter) {
        map(columnName, DataType.Int8Range, propertyGetter);
    }

    protected void mapNumRange(String columnName, Function<TEntity, Range<Number>> propertyGetter) {
        map(columnName, DataType.NumRange, propertyGetter);
    }

    protected void mapDateRange(String columnName, Function<TEntity, Range<LocalDate>> propertyGetter) {
        map(columnName, DataType.DateRange, propertyGetter);
    }

    // endregion

    private void addColumn(String columnName, BiConsumer<PgBinaryWriter, TEntity> action) {
        columns.add(new ColumnDefinition<>(columnName, action));
    }

    public List<ColumnDefinition<TEntity>> getColumns() {
        return columns;
    }

    public String getCopyCommand() {
        String commaSeparatedColumns = columns.stream()
                .map(x -> x.getColumnName())
                .map(x -> usePostgresQuoting ? PostgreSqlUtils.quoteIdentifier(x) : x)
                .collect(Collectors.joining(", "));

        return String.format("COPY %1$s(%2$s) FROM STDIN BINARY",
                table.GetFullyQualifiedTableName(usePostgresQuoting),
                commaSeparatedColumns);
    }
}
