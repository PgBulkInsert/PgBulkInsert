// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.pgsql.handlers;

import de.bytefish.pgbulkinsert.exceptions.ValueHandlerAlreadyRegisteredException;
import de.bytefish.pgbulkinsert.exceptions.ValueHandlerNotRegisteredException;
import de.bytefish.pgbulkinsert.pgsql.constants.DataType;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class ValueHandlerProvider implements IValueHandlerProvider {

    private Map<DataType, ValueHandler> valueHandlers;

    public ValueHandlerProvider() {
        valueHandlers = new HashMap<>();

        add(DataType.Boolean, new BooleanValueHandler());
        add(DataType.Char, new ByteValueHandler<>());
        add(DataType.Numeric, new BigDecimalValueHandler<>());
        add(DataType.DoublePrecision, new DoubleValueHandler<>());
        add(DataType.SinglePrecision, new FloatValueHandler<>());
        add(DataType.Date, new LocalDateValueHandler());
        add(DataType.Timestamp, new LocalDateTimeValueHandler());
        add(DataType.Int2, new ShortValueHandler<>());
        add(DataType.Int4, new IntegerValueHandler<>());
        add(DataType.Int8, new LongValueHandler<>());
        add(DataType.Text, new StringValueHandler());
        add(DataType.VarChar, new StringValueHandler());
        add(DataType.Inet4, new Inet4AddressValueHandler());
        add(DataType.Inet6, new Inet6AddressValueHandler());
        add(DataType.Uuid, new UUIDValueHandler());
        add(DataType.Bytea, new ByteArrayValueHandler());
        add(DataType.Jsonb, new JsonbValueHandler());
        add(DataType.Hstore, new HstoreValueHandler());
        add(DataType.Point, new PointValueHandler());
        add(DataType.Box, new BoxValueHandler());
        add(DataType.Line, new LineValueHandler());
        add(DataType.LineSegment, new LineSegmentValueHandler());
        add(DataType.Path, new PathValueHandler());
        add(DataType.Polygon, new PolygonValueHandler());
        add(DataType.Circle, new CircleValueHandler());
        add(DataType.MacAddress, new MacAddressValueHandler());
    }

    public <TTargetType> ValueHandlerProvider add(DataType targetType, IValueHandler<TTargetType> valueHandler) {
        if(valueHandlers.containsKey(targetType)) {
            throw new ValueHandlerAlreadyRegisteredException(String.format("TargetType '%s' has already been registered", targetType));
        }

        valueHandlers.put(targetType, valueHandler);

        return this;
    }

    @Override
    public <TTargetType> IValueHandler<TTargetType> resolve(DataType dataType) {
        if(!valueHandlers.containsKey(dataType)) {
            throw new ValueHandlerNotRegisteredException(String.format("DataType '%s' has not been registered", dataType));
        }
        return (IValueHandler<TTargetType>) valueHandlers.get(dataType);
    }


    @Override
    public String toString() {

        String valueHandlersString =
                valueHandlers.entrySet()
                        .stream()
                        .map(e -> e.getValue().toString())
                        .collect(Collectors.joining(", "));

        return "ValueHandlerProvider{" +
                "valueHandlers=[" + valueHandlersString + "]" +
                '}';
    }
}
