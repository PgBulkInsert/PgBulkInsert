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

        add(new BooleanValueHandler());
        add(new ByteValueHandler());
        add(new DoubleValueHandler());
        add(new FloatValueHandler());
        add(new LocalDateValueHandler());
        add(new LocalDateTimeValueHandler());
        add(new IntegerValueHandler());
        add(new ShortValueHandler());
        add(new LongValueHandler());
        add(new StringValueHandler());
        add(new Inet4AddressValueHandler());
        add(new Inet6AddressValueHandler());
        add(new UUIDValueHandler());
        add(new ByteArrayValueHandler());
        add(new JsonbValueHandler());
        add(new HstoreValueHandler());
        add(new PointValueHandler());
        add(new BoxValueHandler());
        add(new LineValueHandler());
        add(new LineSegmentValueHandler());
        add(new PathValueHandler());
        add(new PolygonValueHandler());
        add(new CircleValueHandler());
        add(new MacAddressValueHandler());
    }

    public <TTargetType> ValueHandlerProvider add(IValueHandler<TTargetType> valueHandler) {
        DataType targetType = valueHandler.getDataType();

        if(valueHandlers.containsKey(targetType)) {
            throw new ValueHandlerAlreadyRegisteredException(String.format("TargetType '%s' has already been registered", targetType));
        }

        valueHandlers.put(valueHandler.getDataType(), valueHandler);

        return this;
    }

    @Override
    public <TTargetType> IValueHandler<TTargetType> resolve(DataType dataType) {
        if(!valueHandlers.containsKey(dataType)) {
            throw new ValueHandlerNotRegisteredException(String.format("DataType '%s' has not been registered", dataType));
        }
        return (IValueHandler<TTargetType>) valueHandlers.get(dataType);
    }

    public <TTargetType> ValueHandlerProvider override(IValueHandler<TTargetType> valueHandler) {

        valueHandlers.put(valueHandler.getDataType(), valueHandler);

        return this;
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
