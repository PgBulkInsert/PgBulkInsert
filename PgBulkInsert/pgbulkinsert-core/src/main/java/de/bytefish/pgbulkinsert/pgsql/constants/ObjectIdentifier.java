// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.pgsql.constants;

import java.util.HashMap;
import java.util.Map;


// https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.h
public class ObjectIdentifier {

    // region OID 1 - 99

    // boolean, 'true'/'false'
    public static int Boolean = 16;

    // variable-length string, binary values escaped
    public static int Bytea = 17;

    // single character
    public static int Char = 18;

    // 63-byte type for storing system identifiers
    public static int Name = 19;

    // ~18 digit integer, 8-byte storage
    public static int Int8 = 20;

    // -32 thousand to 32 thousand, 2-byte storage
    public static int Int2 = 21;

    // -2 billion to 2 billion integer, 4-byte storage
    public static int Int4 = 23;

    // variable-length string, no limit specified
    public static int Text = 25;

    // object identifier(oid), maximum 4 billion
    public static int Oid = 26;

    // (block, offset), physical location of tuple
    public static int Tid = 27;

    // transaction id
    public static int Xid = 28;

    // command identifier type, sequence in transaction id
    public static int Cid = 29;


    // endregion

    // region OID 100 - 199

    // JSON
    public static int Jsonb = 114;

    // XML content
    public static int Xml = 115;

    // endregion

    // region OID 600 - 699

    // geometric point '(x, y)'
    public static int Point = 600;

    // geometric line segment '(pt1, pt2)'
    public static int LineSegment = 601;

    // geometric path '(pt1,...)'
    public static int Path = 602;

    // geometric box '(lower left, upper right)'
    public static int Box = 603;

    // geometric polygon '(pt1, ...)'
    public static int Polygon = 604;

    // geometric line
    public static int Line = 628;

    // endregion

    // region OID 700 - 799

    // single-precision floating point number, 4-byte storage
    public static int SinglePrecision = 700;

    // double-precision floating point number, 8-byte storage
    public static int DoublePrecision = 701;

    // absolute, limited-range date and time (Unix system time)
    public static int AbsTime = 702;

    // relative, limited-range time interval (Unix delta time)
    public static int RelTime = 703;

    // (abstime, abstime), time interval
    public static int TInterval = 704;

    // unknown
    public static int Unknown =	705;

    // geometric circle '(center, radius)'
    public static int Circle =	705;

    // monetary amounts, $d,ddd.cc
    public static int Cash = 790;

    // money
    public static int Money = 791;

    // endregion

    // region OID 800 - 899

    // XX:XX:XX:XX:XX:XX, MAC address
    public static int MacAddress = 829;

    // IP address/netmask, host address, netmask optional
    public static int Inet = 869;

    // network IP address/netmask, network address
    public static int Cidr = 650;

    // XX:XX:XX:XX:XX:XX:XX:XX, MAC address
    public static int MacAddress8 = 774;

    // endregion

    // region OIDS 1000 - 1099

    // char(length), blank-padded string, fixed storage length
    public static int CharLength = 1042;

    // varchar(length), non-blank-padded string, variable storage length
    public static int VarCharLength = 1043;

    // Date
    public static int Date = 1082;

    // Time Of Day
    public static int Time = 1082;

    // endregion

    // region OIDS 1100 - 1199

    // date and time
    public static int Timestamp = 1114;

    // date and time with time zone
    public static int TimestampTz = 1184;

    // Interval
    public static int Interval = 1186;

    // endregion

    // region OIDS 1200 - 1299

    // time of day with time zone
    public static int TimeTz = 1266;

    // endregion

    // region OIDS 1500 - 1599

    // fixed-length bit string
    public static int Bit = 1560;

    // variable-length bit string
    public static int VarBit = 1562;

    // endregion

    // region OIDS 1700 - 1799

    public static int Numeric = 1700;

    // endregion

    // region UUID

    public static int Uuid = 2950;

    // endregion

    // region Pseudo-Types

    public static int Record = 2249;

    // endregion

    private static Map<DataType, Integer> mapping = buildLookupTable();

    private static Map<DataType, Integer> buildLookupTable() {

        final Map<DataType, Integer> mapping = new HashMap<>();

        mapping.put(DataType.Boolean, Boolean);
        mapping.put(DataType.Bytea, Bytea);
        mapping.put(DataType.Char, Char);
        mapping.put(DataType.Name, Name);
        mapping.put(DataType.Int8, Int8);
        mapping.put(DataType.Int2, Int2);
        mapping.put(DataType.Int4, Int4);
        mapping.put(DataType.Text, Text);
        mapping.put(DataType.Oid, Oid);
        mapping.put(DataType.Tid, Tid);
        mapping.put(DataType.Xid, Xid);
        mapping.put(DataType.Cid, Cid);
        mapping.put(DataType.Jsonb, Jsonb);
        mapping.put(DataType.Xml, Xml);
        mapping.put(DataType.Point, Point);
        mapping.put(DataType.LineSegment, LineSegment);
        mapping.put(DataType.Path, Path);
        mapping.put(DataType.Box, Box);
        mapping.put(DataType.Polygon, Polygon);
        mapping.put(DataType.Line, Line);
        mapping.put(DataType.SinglePrecision, SinglePrecision);
        mapping.put(DataType.DoublePrecision, DoublePrecision);
        mapping.put(DataType.AbsTime, AbsTime);
        mapping.put(DataType.RelTime, RelTime);
        mapping.put(DataType.TInterval, TInterval);
        mapping.put(DataType.Unknown, Unknown);
        mapping.put(DataType.Circle, Circle);
        mapping.put(DataType.Cash, Cash);
        mapping.put(DataType.Money, Money);
        mapping.put(DataType.MacAddress, MacAddress);
        mapping.put(DataType.Inet4, Inet);
        mapping.put(DataType.Inet6, Inet);
        mapping.put(DataType.Cidr, Cidr);
        mapping.put(DataType.MacAddress8, MacAddress8);
        mapping.put(DataType.CharLength, CharLength);
        mapping.put(DataType.VarChar, VarCharLength);
        mapping.put(DataType.Date, Date);
        mapping.put(DataType.Time, Time);
        mapping.put(DataType.Timestamp, Timestamp);
        mapping.put(DataType.TimestampTz, TimestampTz);
        mapping.put(DataType.Interval, Interval);
        mapping.put(DataType.TimeTz, TimeTz);
        mapping.put(DataType.Bit, Bit);
        mapping.put(DataType.VarBit, VarBit);
        mapping.put(DataType.Numeric, Numeric);
        mapping.put(DataType.Uuid, Uuid);
        mapping.put(DataType.Record, Record);

        return mapping;
    }

    public static int mapFrom(DataType type) {
        if(mapping.containsKey(type)) {
            return mapping.get(type);
        }
        return Unknown;
    }
}
