package de.bytefish.pgbulkinsert.row;

import de.bytefish.pgbulkinsert.exceptions.BinaryWriteFailedException;
import de.bytefish.pgbulkinsert.pgsql.PgBinaryWriter;
import de.bytefish.pgbulkinsert.pgsql.handlers.ValueHandlerProvider;
import de.bytefish.pgbulkinsert.util.PostgreSqlUtils;
import de.bytefish.pgbulkinsert.util.StringUtils;
import org.postgresql.PGConnection;
import org.postgresql.copy.PGCopyOutputStream;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SimpleRowWriter implements  AutoCloseable {

    public static class Table {

        private final String schema;
        private final String table;
        private final String[] columns;

        public Table(String table, String... columns) {
            this(null, table, columns);
        }

        public Table(String schema, String table, String... columns) {
            this.schema = schema;
            this.table = table;
            this.columns = columns;
        }

        public String getSchema() {
            return schema;
        }

        public String getTable() {
            return table;
        }

        public String[] getColumns() {
            return columns;
        }

        public String getFullyQualifiedTableName(boolean usePostgresQuoting) {
            return PostgreSqlUtils.getFullyQualifiedTableName(schema, table, usePostgresQuoting);
        }

        public String getCopyCommand(boolean usePostgresQuoting) {

            String commaSeparatedColumns = Arrays.stream(columns)
                    .map(x -> usePostgresQuoting ? PostgreSqlUtils.quoteIdentifier(x) : x)
                    .collect(Collectors.joining(", "));

            return String.format("COPY %1$s(%2$s) FROM STDIN BINARY",
                    getFullyQualifiedTableName(usePostgresQuoting),
                    commaSeparatedColumns);
        }
    }

    private final Table table;
    private final PgBinaryWriter writer;
    private final ValueHandlerProvider provider;
    private final Map<String, Integer> lookup;

    private Function<String, String> nullCharacterHandler;
    private boolean isOpened;
    private boolean isClosed;

    public SimpleRowWriter(final Table table, final PGConnection connection) throws SQLException {
        this(table, connection, false);
    }

    public SimpleRowWriter(final Table table, final PGConnection connection, final boolean usePostgresQuoting) throws SQLException {
        this.table = table;
        this.isClosed = false;
        this.isOpened = false;
        this.nullCharacterHandler = (val) -> val;

        this.provider = new ValueHandlerProvider();
        this.lookup = new HashMap<>();

        for (int ordinal = 0; ordinal < table.columns.length; ordinal++) {
            lookup.put(table.columns[ordinal], ordinal);
        }

        this.writer = new PgBinaryWriter(new PGCopyOutputStream(connection, table.getCopyCommand(usePostgresQuoting), 1));

        isClosed = false;
        isOpened = true;
    }

    public synchronized void startRow(Consumer<SimpleRow> consumer) {

        // We try to write a Row, but the underlying Stream to PostgreSQL has not
        // been opened yet. We should not proceed and throw an Exception:
        if(!isOpened) {
            throw new BinaryWriteFailedException("The SimpleRowWriter has not been opened");
        }

        // We try to write a Row, but the underlying Stream to PostgreSQL has already
        // been closed. We should not proceed and throw an Exception:
        if(isClosed) {
            throw new BinaryWriteFailedException("The PGCopyOutputStream has already been closed");
        }

        try {

            writer.startRow(table.columns.length);

            SimpleRow row = new SimpleRow(provider, lookup, nullCharacterHandler);

            consumer.accept(row);

            row.writeRow(writer);

        } catch(Exception e) {

            try {
                close();
            } catch(Exception ex) {
                // There is nothing more we can do ...
            }

            throw e;
        }
    }

    @Override
    public void close()  {

        // This stream shouldn't be reused, so let's store a flag here:
        isOpened = false;
        isClosed = true;

        writer.close();
    }

    public void enableNullCharacterHandler() {
        this.nullCharacterHandler = (val) -> StringUtils.removeNullCharacter(val);
    }

    public void setNullCharacterHandler(Function<String, String> nullCharacterHandler) {
        this.nullCharacterHandler = nullCharacterHandler;
    }
}
