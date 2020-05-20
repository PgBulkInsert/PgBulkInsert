package de.bytefish.pgbulkinsert.test.jpa.annotations;

import de.bytefish.pgbulkinsert.PgBulkInsert;
import de.bytefish.pgbulkinsert.jpa.JpaMapping;
import de.bytefish.pgbulkinsert.jpa.annotations.PostgresDataType;
import de.bytefish.pgbulkinsert.pgsql.constants.DataType;
import de.bytefish.pgbulkinsert.test.utils.TransactionalTestBase;
import de.bytefish.pgbulkinsert.util.PostgreSqlUtils;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Assert;
import org.junit.Test;
import org.postgresql.PGConnection;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

public class PostgresDataTypeTests extends TransactionalTestBase {

    @Entity
    @Table(name = "unit_test", schema = "public")
    public static class SampleEntity {

        @Nullable
        @Id
        @Column(name = "id")
        private Long id;

        @Nullable
        @Column(name = "int_field")
        @PostgresDataType(columnName = "int_field", dataType = DataType.Int4)
        private Short shortField;

        @Nullable
        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        @Nullable
        public Short getShortField() {
            return shortField;
        }

        public void setShortField(Short shortField) {
            this.shortField = shortField;
        }
    }

    @Override
    protected void onSetUpInTransaction() throws Exception {
        createTable();
    }

    @Test
    public void writeShortAsIntValueTest() throws SQLException {

        SampleEntity s = new SampleEntity();

        s.setId(1L);
        s.setShortField((short) 33);

        // Create the JpaMapping:
        JpaMapping<SampleEntity> mapping = new JpaMapping<>(SampleEntity.class);

        // Create the Bulk Inserter:
        PgBulkInsert<SampleEntity> bulkInsert = new PgBulkInsert<>(mapping);

        PGConnection conn = PostgreSqlUtils.getPGConnection(connection);

        bulkInsert.saveAll(conn, Arrays.asList(s));

        ResultSet rs = getAll();

        while (rs.next()) {

            int v1 = rs.getInt("int_field");

            Assert.assertEquals(33, v1);
        }
    }

    private boolean createTable() throws SQLException {

        String sqlStatement = "CREATE TABLE public.unit_test" +
                "            (\n" +
                "                id int8,\n" +
                "                int_field int4\n" +
                "            );";

        Statement statement = connection.createStatement();

        return statement.execute(sqlStatement);
    }

    private ResultSet getAll() throws SQLException {
        String sqlStatement = "SELECT * FROM public.unit_test";

        Statement statement = connection.createStatement();

        return statement.executeQuery(sqlStatement);
    }
}
