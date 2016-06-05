// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.de.bytefish.pgbulkinsert.pgsql.constants;

import de.bytefish.pgbulkinsert.de.bytefish.pgbulkinsert.util.JavaUtils;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.util.AbstractMap;
import java.util.Map;
import java.util.UUID;

// https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.h
public class PgTypes {

    public static int Boolean = 16;

    public static int Bytea = 17;

    public static int Char = 18;

    public static int Int8 = 20;

    public static int Int2 = 21;

    public static int Int4 = 23;

    public static int Text = 25;

    public static int SinglePrecision = 700;

    public static int DoublePrecision = 701;

    public static int Cash = 790;

    public static int Money = 791;

    public static int MacAddress = 829;

    public static int Inet = 869;

    public static int Cidr = 650;

    public static int Unknown =	705;

    public static int Uuid = 2950;

    private static Map<Class, Integer> mapping = JavaUtils.initializeMap(
            new AbstractMap.SimpleEntry<>(Byte.class, Char),
            new AbstractMap.SimpleEntry<>(Byte[].class, Bytea),
            new AbstractMap.SimpleEntry<>(Boolean.class, Boolean),
            new AbstractMap.SimpleEntry<>(Long.class, Int8),
            new AbstractMap.SimpleEntry<>(Short.class, Int2),
            new AbstractMap.SimpleEntry<>(Integer.class, Int4),
            new AbstractMap.SimpleEntry<>(String.class, Text),
            new AbstractMap.SimpleEntry<>(Float.class, SinglePrecision),
            new AbstractMap.SimpleEntry<>(Double.class, DoublePrecision),
            new AbstractMap.SimpleEntry<>(Inet4Address.class, Inet),
            new AbstractMap.SimpleEntry<>(Inet6Address.class, Inet),
            new AbstractMap.SimpleEntry<>(UUID.class, Uuid)
    );

    public static int mapFrom(Class type) {
        if(mapping.containsKey(type)) {
            return mapping.get(type);
        }
        return Unknown;
    }
}
