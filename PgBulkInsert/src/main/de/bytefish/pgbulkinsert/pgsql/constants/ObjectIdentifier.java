// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.pgsql.constants;

import de.bytefish.pgbulkinsert.util.JavaUtils;

import java.util.AbstractMap;
import java.util.Map;


// https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.h
public class ObjectIdentifier {

    public static int Boolean = 16;

    public static int Bytea = 17;

    public static int Char = 18;

    public static int Int8 = 20;

    public static int Int2 = 21;

    public static int Int4 = 23;

    public static int Text = 25;

    public static int Jsonb = 114;

    public static int SinglePrecision = 700;

    public static int DoublePrecision = 701;

    public static int Cash = 790;

    public static int Money = 791;

    public static int MacAddress = 829;

    public static int Inet = 869;

    public static int Cidr = 650;

    public static int Unknown =	705;

    public static int Date = 1082;

    public static int Timestamp = 1114;

    public static int Uuid = 2950;

    private static Map<DataType, Integer> mapping = JavaUtils.initializeMap(
            new AbstractMap.SimpleEntry<>(DataType.Char, Char),
            new AbstractMap.SimpleEntry<>(DataType.Bytea, Bytea),
            new AbstractMap.SimpleEntry<>(DataType.Boolean, Boolean),
            new AbstractMap.SimpleEntry<>(DataType.Int2, Int2),
            new AbstractMap.SimpleEntry<>(DataType.Int4, Int4),
            new AbstractMap.SimpleEntry<>(DataType.Int8, Int8),
            new AbstractMap.SimpleEntry<>(DataType.Text, Text),
            new AbstractMap.SimpleEntry<>(DataType.Jsonb, Jsonb),
            new AbstractMap.SimpleEntry<>(DataType.SinglePrecision, SinglePrecision),
            new AbstractMap.SimpleEntry<>(DataType.DoublePrecision, DoublePrecision),
            new AbstractMap.SimpleEntry<>(DataType.Inet4, Inet),
            new AbstractMap.SimpleEntry<>(DataType.Inet6, Inet),
            new AbstractMap.SimpleEntry<>(DataType.Uuid, Uuid),
            new AbstractMap.SimpleEntry<>(DataType.Timestamp, Timestamp),
            new AbstractMap.SimpleEntry<>(DataType.Date, Date)
    );

    public static int mapFrom(DataType type) {
        if(mapping.containsKey(type)) {
            return mapping.get(type);
        }
        return Unknown;
    }
}
