package de.bytefish.pgbulkinsert.row;

import de.bytefish.pgbulkinsert.pgsql.PgBinaryWriter;
import de.bytefish.pgbulkinsert.pgsql.constants.DataType;
import de.bytefish.pgbulkinsert.pgsql.constants.ObjectIdentifier;
import de.bytefish.pgbulkinsert.pgsql.handlers.CollectionValueHandler;
import de.bytefish.pgbulkinsert.pgsql.handlers.IValueHandler;
import de.bytefish.pgbulkinsert.pgsql.handlers.ValueHandlerProvider;
import de.bytefish.pgbulkinsert.pgsql.model.geometric.*;
import de.bytefish.pgbulkinsert.pgsql.model.network.MacAddress;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

public class SimpleRow {

    private final ValueHandlerProvider provider;
    private final Map<String, Integer> lookup;
    private final Map<Integer, Consumer<PgBinaryWriter>> actions;

    @SuppressWarnings("unchecked")
    public SimpleRow(ValueHandlerProvider provider, Map<String, Integer> lookup) {
        this.provider = provider;
        this.lookup = lookup;
        this.actions = new HashMap<>();
    }

    public <TTargetType> void setValue(String columnName, DataType type, TTargetType value) {
        final int ordinal = lookup.get(columnName);

        setValue(ordinal, type, value);
    }

    public <TTargetType> void setValue(int ordinal, DataType type, TTargetType value) {

        final IValueHandler<TTargetType> handler = provider.resolve(type);

        actions.put(ordinal, (writer) -> writer.write(handler, value));
    }

    public <TTargetType> void setValue(String columnName, IValueHandler<TTargetType> handler, TTargetType value) {
        final int ordinal = lookup.get(columnName);

        setValue(ordinal, handler, value);
    }

    public <TTargetType> void setValue(int ordinal, IValueHandler<TTargetType> handler, TTargetType value) {
        actions.put(ordinal, (writer) -> writer.write(handler, value));
    }

    public <TElementType, TCollectionType extends Collection<TElementType>> void setCollection(String columnName, DataType type, TCollectionType value) {

        final int ordinal = lookup.get(columnName);

        setCollection(ordinal, type, value);
    }


    public <TElementType, TCollectionType extends Collection<TElementType>> void setCollection(int ordinal, DataType type, TCollectionType value) {
        final CollectionValueHandler<TElementType, TCollectionType> handler = new CollectionValueHandler<>(ObjectIdentifier.mapFrom(type), provider.resolve(type));

        actions.put(ordinal, (writer) -> writer.write(handler, value));
    }


    public void writeRow(PgBinaryWriter writer) {
        for(int ordinalIdx = 0; ordinalIdx < lookup.keySet().size(); ordinalIdx++) {

            // If this Ordinal wasn't set, we assume a NULL:
            if(!actions.containsKey(ordinalIdx)) {
                    writer.writeNull();

                    continue;
            }

            actions.get(ordinalIdx).accept(writer);
        }
    }

    public void setBoolean(String columnName, boolean value) {
        setValue(columnName, DataType.Boolean, value);
    }

    public void setBoolean(int ordinal, boolean value) {
        setValue(ordinal, DataType.Boolean, value);
    }

    public void setByte(String columnName, byte value) {
        setValue(columnName, DataType.Char, value);
    }

    public void setByte(int ordinal, byte value) {
        setValue(ordinal, DataType.Char, value);
    }

    public void setShort(String columnName, short value) {
        setValue(columnName, DataType.Int2, value);
    }

    public void setShort(int ordinal, short value) {
        setValue(ordinal, DataType.Int2, value);
    }

    public void setInteger(String columnName, int value) {
        setValue(columnName, DataType.Int4, value);
    }

    public void setInteger(int ordinal, int value) {
        setValue(ordinal, DataType.Int4, value);
    }

    public void setNumeric(String columnName, Number value) {
        setValue(columnName, DataType.Numeric, value);
    }

    public void setNumeric(int ordinal, Number value) {
        setValue(ordinal, DataType.Numeric, value);
    }

    public void setLong(String columnName, long value) {
        setValue(columnName, DataType.Int8, value);
    }

    public void setLong(int ordinal, long value) {
        setValue(ordinal, DataType.Int8, value);
    }

    public void setFloat(String columnName, float value) {
        setValue(columnName, DataType.SinglePrecision, value);
    }

    public void setFloat(int ordinal, float value) {
        setValue(ordinal, DataType.SinglePrecision, value);
    }

    public void setDouble(String columnName, double value) {
        setValue(columnName, DataType.DoublePrecision, value);
    }

    public void setDate(String columnName, LocalDate value) {
        setValue(columnName, DataType.Date, value);
    }

    public void setDate(int ordinal, LocalDate value) {
        setValue(ordinal, DataType.Date, value);
    }

    public void setInet6Addr(String columnName, Inet6Address value) {
        setValue(columnName, DataType.Inet6, value);
    }

    public void setInet6Addr(int ordinal, Inet6Address value) {
        setValue(ordinal, DataType.Inet6, value);
    }

    public void setInet4Addr(String columnName, Inet4Address value) {
        setValue(columnName, DataType.Inet4, value);
    }

    public void setInet4Addr(int ordinal, Inet4Address value) {
        setValue(ordinal, DataType.Inet4, value);
    }

    public void setTimeStamp(String columnName, LocalDateTime value) {
        setValue(columnName, DataType.Timestamp, value);
    }

    public void setTimeStamp(int ordinal, LocalDateTime value) {
        setValue(ordinal, DataType.Timestamp, value);
    }

    public void setTimeStampTz(String columnName, ZonedDateTime value) {
        setValue(columnName, DataType.TimestampTz, value);
    }

    public void setTimeStampTz(int ordinal, ZonedDateTime value) {
        setValue(ordinal, DataType.TimestampTz, value);
    }

    public void setText(String columnName, String value) {
        setValue(columnName, DataType.Text, value);
    }

    public void setText(int ordinal, String value) {
        setValue(ordinal, DataType.Text, value);
    }

    public void setVarChar(String columnName, String value) {
        setValue(columnName, DataType.Text, value);
    }

    public void setVarChar(int ordinal, String value) {
        setValue(ordinal, DataType.Text, value);
    }

    public void setUUID(String columnName, UUID value) {
        setValue(columnName, DataType.Uuid, value);
    }

    public void setUUID(int ordinal, UUID value) {
        setValue(ordinal, DataType.Uuid, value);
    }

    public void setByteArray(String columnName, byte[] value) {
        setValue(columnName, DataType.Bytea, value);
    }

    public void setByteArray(int ordinal, byte[] value) {
        setValue(ordinal, DataType.Bytea, value);
    }

    public void setJsonb(String columnName, String value) {
        setValue(columnName, DataType.Jsonb, value);
    }

    public void setJsonb(int ordinal, String value) {
        setValue(ordinal, DataType.Jsonb, value);
    }

    public void setHstore(String columnName, Map<String, String> value) {
        setValue(columnName, DataType.Hstore, value);
    }

    public void setHstore(int ordinal, Map<String, String> value) {
        setValue(ordinal, DataType.Hstore, value);
    }

    public void setPoint(String columnName, Point value) {
        setValue(columnName, DataType.Point, value);
    }

    public void setPoint(int ordinal, Point value) {
        setValue(ordinal, DataType.Point, value);
    }

    public void setBox(String columnName, Box value) {
        setValue(columnName, DataType.Box, value);
    }

    public void setBox(int ordinal, Box value) {
        setValue(ordinal, DataType.Box, value);
    }

    public void setPath(String columnName, Path value) {
        setValue(columnName, DataType.Path, value);
    }

    public void setPath(int ordinal, Path value) {
        setValue(ordinal, DataType.Path, value);
    }

    public void setPolygon(String columnName, Polygon value) {
        setValue(columnName, DataType.Polygon, value);
    }

    public void setPolygon(int ordinal, Polygon value) {
        setValue(ordinal, DataType.Polygon, value);
    }

    public void setLine(String columnName, Line value) {
        setValue(columnName, DataType.Line, value);
    }

    public void setLine(int ordinal, Line value) {
        setValue(ordinal, DataType.Line, value);
    }

    public void setLineSegment(String columnName, LineSegment value) {
        setValue(columnName, DataType.LineSegment, value);
    }

    public void setLineSegment(int ordinal, LineSegment value) {
        setValue(ordinal, DataType.LineSegment, value);
    }

    public void setCircle(String columnName, Circle value) {
        setValue(columnName, DataType.Circle, value);
    }

    public void setCircle(int ordinal, Circle value) {
        setValue(ordinal, DataType.Circle, value);
    }

    public void setMacAddress(String columnName, MacAddress value) {
        setValue(columnName, DataType.MacAddress, value);
    }

    public void setMacAddress(int ordinal, MacAddress value) {
        setValue(ordinal, DataType.MacAddress, value);
    }

    public void setBooleanArray(String columnName, Collection<Boolean> value) {
        setCollection(columnName, DataType.Boolean, value);
    }

    public void setBooleanArray(int ordinal, Collection<Boolean> value) {
        setCollection(ordinal, DataType.Boolean, value);
    }

    public <T extends Number> void setShortArray(String columnName, Collection<T> value) {
        setCollection(columnName, DataType.Int2, value);
    }

    public <T extends Number> void setShortArray(int ordinal, Collection<T> value) {
        setCollection(ordinal, DataType.Int2, value);
    }

    public <T extends Number> void setIntegerArray(String columnName, Collection<T> value) {
        setCollection(columnName, DataType.Int4, value);
    }

    public <T extends Number> void setIntegerArray(int ordinal, Collection<T> value) {
        setCollection(ordinal, DataType.Int4, value);
    }

    public <T extends Number> void setLongArray(String columnName, Collection<T> value) {
        setCollection(columnName, DataType.Int8, value);
    }

    public <T extends Number> void setLongArray(int ordinal, Collection<T> value) {
        setCollection(ordinal, DataType.Int8, value);
    }

    public void setTextArray(String columnName, Collection<String> value) {
        setCollection(columnName, DataType.Text, value);
    }

    public void setTextArray(int ordinal, Collection<String> value) {
        setCollection(ordinal, DataType.Text, value);
    }

    public void setVarCharArray(String columnName, Collection<String> value) {
        setCollection(columnName, DataType.VarChar, value);
    }

    public void setVarCharArray(int ordinal, Collection<String> value) {
        setCollection(ordinal, DataType.VarChar, value);
    }

    public <T extends Number> void setFloatArray(String columnName, Collection<T> value) {
        setCollection(columnName, DataType.SinglePrecision, value);
    }

    public <T extends Number> void setFloatArray(int ordinal, Collection<T> value) {
        setCollection(ordinal, DataType.SinglePrecision, value);
    }

    public <T extends Number> void setDoubleArray(String columnName, Collection<T> value) {
        setCollection(columnName, DataType.DoublePrecision, value);
    }

    public <T extends Number> void setDoubleArray(int ordinal, Collection<T> value) {
        setCollection(ordinal, DataType.DoublePrecision, value);
    }

    public <T extends Number> void setNumericArray(String columnName, Collection<T> value) {
        setCollection(columnName, DataType.Numeric, value);
    }

    public <T extends Number> void setNumericArray(int ordinal, Collection<T> value) {
        setCollection(ordinal, DataType.Numeric, value);
    }

    public void setUUIDArray(String columnName, Collection<UUID> value) {
        setCollection(columnName, DataType.Uuid, value);
    }

    public void setUUIDArray(int ordinal, Collection<UUID> value) {
        setCollection(ordinal, DataType.Uuid, value);
    }

    public void setInet4Array(String columnName, Collection<Inet4Address> value) {
        setCollection(columnName, DataType.Inet4, value);
    }

    public void setInet4Array(int ordinal, Collection<Inet4Address> value) {
        setCollection(ordinal, DataType.Inet4, value);
    }

    public void setInet6Array(String columnName, Collection<Inet6Address> value) {
        setCollection(columnName, DataType.Inet6, value);
    }

    public void setInet6Array(int ordinal, Collection<Inet6Address> value) {
        setCollection(ordinal, DataType.Inet6, value);
    }
}
