// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.de.bytefish.pgbulkinsert.pgsql.handlers;

import de.bytefish.pgbulkinsert.de.bytefish.pgbulkinsert.exceptions.ValueHandlerAlreadyRegisteredException;
import de.bytefish.pgbulkinsert.de.bytefish.pgbulkinsert.exceptions.ValueHandlerNotRegisteredException;
import de.bytefish.pgbulkinsert.de.bytefish.pgbulkinsert.pgsql.converter.LocalDateConverter;
import de.bytefish.pgbulkinsert.de.bytefish.pgbulkinsert.pgsql.converter.LocalDateTimeConverter;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class ValueHandlerProvider implements IValueHandlerProvider {

    private Map<Type, ValueHandler> valueHandlers;

    public ValueHandlerProvider() {
        valueHandlers = new HashMap<>();

        add(new BooleanValueHandler());
        add(new ByteValueHandler());
        add(new DoubleValueHandler());
        add(new FloatValueHandler());
        add(new LocalDateValueHandler(new LocalDateConverter()));
        add(new LocalDateTimeValueHandler(new LocalDateTimeConverter()));
        add(new IntegerValueHandler());
        add(new ShortValueHandler());
        add(new LongValueHandler());
        add(new StringValueHandler());
        add(new Inet4AddressValueHandler());
        add(new Inet6AddressValueHandler());
        add(new UUIDValueHandler());
        add(new ByteArrayValueHandler());
    }

    public <TTargetType> ValueHandlerProvider add(IValueHandler<TTargetType> valueHandler) {
        Type targetType = valueHandler.getTargetType();

        if(valueHandlers.containsKey(targetType)) {
            throw new ValueHandlerAlreadyRegisteredException(String.format("TargetType '%s' has already been registered", targetType));
        }

        valueHandlers.put(valueHandler.getTargetType(), valueHandler);

        return this;
    }

    @Override
    public <TTargetType> IValueHandler<TTargetType> resolve(Type targetType) {
        if(!valueHandlers.containsKey(targetType)) {
            throw new ValueHandlerNotRegisteredException(String.format("TargetType '%s' has not been registered", targetType));
        }
        return (IValueHandler<TTargetType>) valueHandlers.get(targetType);
    }

    public <TTargetType> ValueHandlerProvider override(IValueHandler<TTargetType> valueHandler) {

        valueHandlers.put(valueHandler.getTargetType(), valueHandler);

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
