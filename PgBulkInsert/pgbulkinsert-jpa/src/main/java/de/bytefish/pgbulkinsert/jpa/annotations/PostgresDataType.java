package de.bytefish.pgbulkinsert.jpa.annotations;

import de.bytefish.pgbulkinsert.pgsql.constants.DataType;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.METHOD})
public @interface PostgresDataType {
    String columnName();
    DataType dataType();
}
