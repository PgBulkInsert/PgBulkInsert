package de.bytefish.pgbulkinsert.row;

import de.bytefish.pgbulkinsert.pgsql.PgBinaryWriter;
import de.bytefish.pgbulkinsert.pgsql.handlers.IValueHandlerProvider;
import de.bytefish.pgbulkinsert.pgsql.handlers.ValueHandlerProvider;
import de.bytefish.pgbulkinsert.util.StringUtils;
import org.postgresql.PGConnection;
import org.postgresql.copy.PGCopyOutputStream;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class SimpleRowWriter {

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

        public String GetFullyQualifiedTableName() {
            if (StringUtils.isNullOrWhiteSpace(schema)) {
                return table;
            }
            return String.format("%1$s.%2$s", schema, table);
        }
    }

    private final Table table;
    private final PgBinaryWriter writer;
    private final IValueHandlerProvider provider;
    private final Map<String, Integer> lookup;

    public SimpleRowWriter(Table table, IValueHandlerProvider valueHandlerProvider) {
        this.writer = new PgBinaryWriter();
        this.table = table;
        this.provider = valueHandlerProvider;

        this.lookup = new HashMap<>();

        for (int ordinal = 0; ordinal < table.columns.length; ordinal++) {
            lookup.put(table.columns[ordinal], ordinal);
        }
    }

    public SimpleRowWriter(Table table) {
        this(table, new ValueHandlerProvider());
    }

    public void open(PGConnection connection) throws SQLException  {
        writer.open(new PGCopyOutputStream(connection, getCopyCommand(table), 1));
    }

    public synchronized void startRow(Consumer<SimpleRow> consumer) {

        writer.startRow(table.columns.length);

        SimpleRow row = new SimpleRow(provider, lookup);

        consumer.accept(row);

        row.writeRow(writer);
    }

    public void close() throws SQLException  {
        writer.close();
    }

    private static String getCopyCommand(Table table) {
        String commaSeparatedColumns = Arrays.stream(table.columns)
                .collect(Collectors.joining(", "));

        return String.format("COPY %1$s(%2$s) FROM STDIN BINARY",
                table.GetFullyQualifiedTableName(),
                commaSeparatedColumns);
    }
}
