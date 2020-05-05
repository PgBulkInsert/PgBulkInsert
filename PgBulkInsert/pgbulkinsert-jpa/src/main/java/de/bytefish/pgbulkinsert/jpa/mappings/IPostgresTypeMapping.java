package de.bytefish.pgbulkinsert.jpa.mappings;

import de.bytefish.pgbulkinsert.pgsql.constants.DataType;

import java.lang.reflect.Type;

public interface IPostgresTypeMapping {

    DataType getDataType(final Type type);

}
