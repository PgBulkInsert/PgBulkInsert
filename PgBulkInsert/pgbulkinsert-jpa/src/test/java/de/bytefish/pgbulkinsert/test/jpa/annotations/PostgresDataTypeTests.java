package de.bytefish.pgbulkinsert.test.jpa.annotations;

import de.bytefish.pgbulkinsert.PgBulkInsert;
import de.bytefish.pgbulkinsert.jpa.JpaMapping;
import de.bytefish.pgbulkinsert.jpa.annotations.PostgresDataType;
import de.bytefish.pgbulkinsert.pgsql.constants.DataType;
import de.bytefish.pgbulkinsert.test.utils.TransactionalTestBase;
import de.bytefish.pgbulkinsert.util.PostgreSqlUtils;
import org.junit.Assert;
import org.junit.Test;
import org.postgresql.PGConnection;

import javax.persistence.*;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class PostgresDataTypeTests extends TransactionalTestBase {

    @Entity
    @Table(name = "unit_test", schema = "sample")
    public class SampleEntity {

        @Id
        @Column(name = "id")
        private Long id;

        @Column(name = "int_field")
        @PostgresDataType(columnName = "int_field", dataType = DataType.Int4)
        private Short shortField;

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

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

        String sqlStatement = "CREATE TABLE sample.unit_test" +
                "            (\n" +
                "                id int8,\n" +
                "                int_field int4\n" +
                "            );";

        Statement statement = connection.createStatement();

        return statement.execute(sqlStatement);
    }

    private ResultSet getAll() throws SQLException {
        String sqlStatement = "SELECT * FROM sample.unit_test";

        Statement statement = connection.createStatement();

        return statement.executeQuery(sqlStatement);
    }

    private int getRowCount() throws SQLException {

        Statement s = connection.createStatement();

        ResultSet r = s.executeQuery(String.format("SELECT COUNT(*) AS rowcount FROM sample.unit_test"));
        r.next();
        int count = r.getInt("rowcount");
        r.close();

        return count;
    }
}
