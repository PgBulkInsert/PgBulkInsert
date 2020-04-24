// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.jpa;

import de.bytefish.pgbulkinsert.mapping.AbstractMapping;
import de.bytefish.pgbulkinsert.pgsql.handlers.IValueHandlerProvider;

import javax.persistence.Column;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Table;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.function.Function;

public class JpaMapping<TEntity> extends AbstractMapping<TEntity> {

    public JpaMapping(Class<TEntity> entityClass) {
        super(getSchemaName(entityClass), getTableName(entityClass));

        mapFields(entityClass);
    }

    private void mapFields(Class<TEntity> entityClass) {
        try {
            internalMapFields(entityClass);
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void internalMapFields(Class<TEntity> entityClass) throws Exception {

        for (Field f : entityClass.getDeclaredFields()) {
            // Is this Field an Enum?
            Enumerated enumerated = f.getAnnotation(Enumerated.class);

            // Get the Column to match in Postgres:
            Column column = f.getAnnotation(Column.class);

            if(column != null) {
                // Access the Property to resolve Type and Name:
                PropertyDescriptor pd =  new PropertyDescriptor(f.getName(), entityClass);

                String columnName = column.name();
                Type fieldType = f.getType();
                Method fieldGetter = pd.getReadMethod();

                if(enumerated != null) {
                    mapEnum(columnName, fieldType, fieldGetter);
                } else {
                    mapField(columnName, fieldType, fieldGetter);
                }
            }
        }
    }
    private void mapEnum(String columnName, Enumerated enumerated, Method fieldGetter) {

        if(enumerated.value() == EnumType.ORDINAL) {
            mapShort(columnName, new Function<TEntity, Number>() {
                @Override
                public Short apply(TEntity tEntity) {
                    Enum<?> enumeration =  (Enum<?>) internalInvoke(fieldGetter, tEntity);

                    return (short) enumeration.ordinal();
                }
            });
        } else if(enumerated.value() == EnumType.STRING) {
            mapText(columnName, new Function<TEntity, String>() {
                @Override
                public String apply(TEntity tEntity) {
                    Enum<?> enumeration =  (Enum<?>) internalInvoke(fieldGetter, tEntity);

                    return enumeration.name();
                }
            });
        }
    }

    private void mapField(String columnName, Type fieldType, Method fieldGetter) {

        if(fieldType.equals(String.class)) {
            mapText(columnName, tEntity -> (String) internalInvoke(fieldGetter, tEntity));

        } else  if(fieldType.equals(boolean.class) || fieldType.equals(Boolean.class)) {
            mapBoolean(columnName, new Function<TEntity, Boolean>() {
                @Override
                public Boolean apply(TEntity tEntity) {
                    return (Boolean) internalInvoke(fieldGetter, tEntity);
                }
            });
        } else if(fieldType.equals(byte.class) || fieldType.equals(Byte.class)) {
            mapByte(columnName, new Function<TEntity, Number>() {
                @Override
                public Number apply(TEntity tEntity) {
                    return (Byte) internalInvoke(fieldGetter, tEntity);
                }
            });
        } else if(fieldType.equals(short.class) || fieldType.equals(Short.class)) {
            mapShort(columnName, new Function<TEntity, Number>() {
                @Override
                public Number apply(TEntity tEntity) {
                    return (Short) internalInvoke(fieldGetter, tEntity);
                }
            });
        } else if(fieldType.equals(int.class) || fieldType.equals(Integer.class)) {
            mapInteger(columnName, new Function<TEntity, Number>() {
                @Override
                public Number apply(TEntity tEntity) {
                    return (Integer) internalInvoke(fieldGetter, tEntity);
                }
            });
        } else if(fieldType.equals(long.class) || fieldType.equals(Long.class)) {
            mapLong(columnName, new Function<TEntity, Number>() {
                @Override
                public Number apply(TEntity tEntity) {
                    return (Long) internalInvoke(fieldGetter, tEntity);
                }
            });
        } else if(fieldType.equals(float.class) || fieldType.equals(Float.class)) {
            mapFloat(columnName, new Function<TEntity, Number>() {
                @Override
                public Number apply(TEntity tEntity) {
                    return (Float) internalInvoke(fieldGetter, tEntity);
                }
            });
        } else if(fieldType.equals(double.class) || fieldType.equals(Double.class)) {
            mapFloat(columnName, new Function<TEntity, Number>() {
                @Override
                public Number apply(TEntity tEntity) {
                    return (Double) internalInvoke(fieldGetter, tEntity);
                }
            });
        } else if(fieldType.equals(byte[].class) || fieldType.equals(Byte[].class)) {
            mapByteArray(columnName, new Function<TEntity, byte[]>() {
                @Override
                public byte[] apply(TEntity tEntity) {
                    return (byte[]) internalInvoke(fieldGetter, tEntity);
                }
            });
        }
    }

    private Object internalInvoke(Method method, TEntity obj) {
        try {
           return method.invoke(obj);
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

    private JpaMapping(String schemaName, String tableName) {
        super(schemaName, tableName);
    }

    private JpaMapping(String schemaName, String tableName, boolean usePostgresQuoting) {
        super(schemaName, tableName, usePostgresQuoting);
    }

    private JpaMapping(IValueHandlerProvider provider, String schemaName, String tableName, boolean usePostgresQuoting) {
        super(provider, schemaName, tableName, usePostgresQuoting);
    }

    public static <T> String getTableName(Class<T> entityClass) {
        Table table = entityClass.getAnnotation(Table.class);

        return (table == null) ? "" : table.name();
    }

    public static <T> String getSchemaName(Class<T> entityClass) {

        Table table = entityClass.getAnnotation(Table.class);

        return (table == null) ? "" : table.schema();
    }

}
