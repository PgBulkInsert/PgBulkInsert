package de.bytefish.pgbulkinsert.jpa.mappings;

import de.bytefish.pgbulkinsert.pgsql.constants.DataType;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

public class PostgresTypeMapping implements IPostgresTypeMapping {

    private final Map<Type, DataType> mappings;

    public PostgresTypeMapping() {
        this.mappings = getDefaultMappings();
    }

    public PostgresTypeMapping(Map<Type, DataType> typeMappings) {
        this.mappings = getDefaultMappings();

        for (Map.Entry<Type, DataType> entry : typeMappings.entrySet()) {
            mappings.put(entry.getKey(), entry.getValue());
        }
    }

    public Map<Type, DataType> getMappings() {
        return mappings;
    }

    private Map<Type, DataType> getDefaultMappings() {

        Map<Type, DataType> mappings = new HashMap<>();

        mappings.put(String.class, DataType.Text);

        mappings.put(LocalDate.class, DataType.Date);
        mappings.put(LocalDateTime.class, DataType.Timestamp);
        mappings.put(ZonedDateTime.class, DataType.TimestampTz);

        mappings.put(boolean.class, DataType.Boolean);
        mappings.put(Boolean.class, DataType.Boolean);

        mappings.put(char.class, DataType.Char);
        mappings.put(Character.class, DataType.Char);

        mappings.put(byte.class, DataType.Char);
        mappings.put(Byte.class, DataType.Char);

        mappings.put(short.class, DataType.Int2);
        mappings.put(Short.class, DataType.Int2);

        mappings.put(int.class, DataType.Int4);
        mappings.put(Integer.class, DataType.Int4);

        mappings.put(long.class, DataType.Int8);
        mappings.put(Long.class, DataType.Int8);

        mappings.put(double.class, DataType.DoublePrecision);
        mappings.put(Double.class, DataType.DoublePrecision);

        mappings.put(float.class, DataType.SinglePrecision);
        mappings.put(Float.class, DataType.SinglePrecision);

        mappings.put(Inet4Address.class, DataType.Inet4);
        mappings.put(Inet6Address.class, DataType.Inet6);

        mappings.put(byte[].class, DataType.Bytea);
        mappings.put(Byte[].class, DataType.Bytea);

        mappings.put(BigDecimal.class, DataType.Numeric);

        return mappings;
    }

    @Override
    public DataType getDataType(Type type) {

        if(!mappings.containsKey(type)) {
            String errorMessage = String.format("Java Type %s is not supported yet.", type);

            throw new IllegalArgumentException(errorMessage);
        }

        return mappings.get(type);
    }
}
