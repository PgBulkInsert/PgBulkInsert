// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.jpa;

import de.bytefish.pgbulkinsert.mapping.AbstractMapping;
import de.bytefish.pgbulkinsert.pgsql.handlers.IValueHandlerProvider;

import javax.persistence.Column;
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
            // Get the Column to match in Postgres:
            Column column = f.getAnnotation(Column.class);

            if(column != null) {
                // Access the Property to resolve Type and Name:
                PropertyDescriptor pd =  new PropertyDescriptor(f.getName(), entityClass);

                String columnName = column.name();
                Type fieldType = f.getType();
                Method fieldGetter = pd.getReadMethod();

                mapField(columnName, fieldType, fieldGetter);
            }
        }
    }

    private void mapField(String columnName, Type fieldType, Method fieldGetter) {

        if(fieldType.equals(String.class)) {
            mapText(columnName, tEntity -> (String) internalInvoke(fieldGetter, tEntity));

            return;
        }

        if(fieldType.equals(boolean.class) || fieldType.equals(Boolean.class)) {
            mapBoolean(columnName, new Function<TEntity, Boolean>() {
                @Override
                public Boolean apply(TEntity tEntity) {
                    return (Boolean) internalInvoke(fieldGetter, tEntity);
                }
            });

            return;
        }

        if(fieldType.equals(byte.class) || fieldType.equals(Byte.class)) {
            mapByte(columnName, new Function<TEntity, Number>() {
                @Override
                public Number apply(TEntity tEntity) {
                    return (Byte) internalInvoke(fieldGetter, tEntity);
                }
            });

            return;
        }

        if(fieldType.equals(short.class) || fieldType.equals(Short.class)) {
            mapShort(columnName, new Function<TEntity, Number>() {
                @Override
                public Number apply(TEntity tEntity) {
                    return (Short) internalInvoke(fieldGetter, tEntity);
                }
            });

            return;
        }

        if(fieldType.equals(int.class) || fieldType.equals(Integer.class)) {
            mapInteger(columnName, new Function<TEntity, Number>() {
                @Override
                public Number apply(TEntity tEntity) {
                    return (Integer) internalInvoke(fieldGetter, tEntity);
                }
            });

            return;
        }

        if(fieldType.equals(long.class) || fieldType.equals(Long.class)) {
            mapLong(columnName, new Function<TEntity, Number>() {
                @Override
                public Number apply(TEntity tEntity) {
                    return (Long) internalInvoke(fieldGetter, tEntity);
                }
            });

            return;
        }

        if(fieldType.equals(float.class) || fieldType.equals(Float.class)) {
            mapFloat(columnName, new Function<TEntity, Number>() {
                @Override
                public Number apply(TEntity tEntity) {
                    return (Float) internalInvoke(fieldGetter, tEntity);
                }
            });

            return;
        }

        if(fieldType.equals(double.class) || fieldType.equals(Double.class)) {
            mapFloat(columnName, new Function<TEntity, Number>() {
                @Override
                public Number apply(TEntity tEntity) {
                    return (Double) internalInvoke(fieldGetter, tEntity);
                }
            });

            return;
        }

        //throw new RuntimeException("Could not map type " + fieldType.getTypeName())
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
