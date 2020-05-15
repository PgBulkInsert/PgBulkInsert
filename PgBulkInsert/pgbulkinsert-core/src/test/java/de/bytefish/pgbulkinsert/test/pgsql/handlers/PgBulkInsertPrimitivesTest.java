package de.bytefish.pgbulkinsert.test.pgsql.handlers;

import de.bytefish.pgbulkinsert.PgBulkInsert;
import de.bytefish.pgbulkinsert.mapping.AbstractMapping;
import de.bytefish.pgbulkinsert.test.utils.TransactionalTestBase;
import de.bytefish.pgbulkinsert.util.PostgreSqlUtils;
import org.junit.Assert;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class PgBulkInsertPrimitivesTest extends TransactionalTestBase {

	private static class SampleEntity {

		public int col_integer;
		public float col_float;
		public double col_double;
		public long col_long;
		public short col_short;
		public byte[] col_bytearray;
		public boolean col_boolean;


		public int getCol_integer() {
			return col_integer;
		}
		public float getCol_float() {
			return col_float;
		}
		public double getCol_double() {
			return col_double;
		}
		public long getCol_long() {
			return col_long;
		}
		public short getCol_short() {
			return col_short;
		}
		public byte[] getCol_bytearray() {
			return col_bytearray;
		}
		public boolean isCol_boolean() {
			return col_boolean;
		}

	}

    @Override
    protected void onSetUpInTransaction() throws Exception {
        createTable();
    }

    @Override
    protected void onSetUpBeforeTransaction() throws Exception {

    }

    private class SampleEntityMapping extends AbstractMapping<SampleEntity> {

        public SampleEntityMapping() {
            super(schema, "unit_test");

            mapBoolean("col_boolean", SampleEntity::isCol_boolean);
            mapInteger("col_integer", SampleEntity::getCol_integer);
            mapShort("col_smallint", SampleEntity::getCol_short);
            mapLong("col_long", SampleEntity::getCol_long);
            mapByteArray("col_bytea", SampleEntity::getCol_bytearray);
            mapFloat("col_real", SampleEntity::getCol_float);
            mapDouble("col_double", SampleEntity::getCol_double);
        }
    }

    @Test
    public void saveAll_boolean_Test() throws SQLException {

        // This list will be inserted.
        List<SampleEntity> entities = new ArrayList<>();

        // Create the Entity to insert:
        SampleEntity entity = new SampleEntity();
        entity.col_boolean = true;

        entities.add(entity);

        PgBulkInsert<SampleEntity> pgBulkInsert = new PgBulkInsert<>(new SampleEntityMapping());

        pgBulkInsert.saveAll(PostgreSqlUtils.getPGConnection(connection), entities.stream());

        ResultSet rs = getAll();

        while (rs.next()) {
            boolean v = rs.getBoolean("col_boolean");

            Assert.assertEquals(true, v);
        }
    }

    @Test
    public void saveAll_Short_Test() throws SQLException {

        // This list will be inserted.
        List<SampleEntity> entities = new ArrayList<>();

        // Create the Entity to insert:
        SampleEntity entity = new SampleEntity();
        entity.col_short = 1;

        entities.add(entity);

        PgBulkInsert<SampleEntity> pgBulkInsert = new PgBulkInsert<>(new SampleEntityMapping());

        pgBulkInsert.saveAll(PostgreSqlUtils.getPGConnection(connection), entities.stream());

        ResultSet rs = getAll();

        while (rs.next()) {
            short v = rs.getShort("col_smallint");

            Assert.assertEquals(1, v);
        }
    }

    @Test
    public void saveAll_Integer_Test() throws SQLException {

        // This list will be inserted.
        List<SampleEntity> entities = new ArrayList<>();

        // Create the Entity to insert:
        SampleEntity entity = new SampleEntity();
        entity.col_integer = 1;

        entities.add(entity);

        PgBulkInsert<SampleEntity> pgBulkInsert = new PgBulkInsert<>(new SampleEntityMapping());

        pgBulkInsert.saveAll(PostgreSqlUtils.getPGConnection(connection), entities.stream());

        ResultSet rs = getAll();

        while (rs.next()) {
            int v = rs.getInt("col_integer");

            Assert.assertEquals(1, v);
        }
    }

    @Test
    public void saveAll_Single_Precision_Test() throws SQLException {

        // This list will be inserted.
        List<SampleEntity> entities = new ArrayList<>();

        // Create the Entity to insert:
        SampleEntity entity = new SampleEntity();
        entity.col_float = 2.0001f;

        entities.add(entity);

        PgBulkInsert<SampleEntity> pgBulkInsert = new PgBulkInsert<>(new SampleEntityMapping());

        pgBulkInsert.saveAll(PostgreSqlUtils.getPGConnection(connection), entities.stream());

        ResultSet rs = getAll();

        while (rs.next()) {
            float v = rs.getFloat("col_real");

            Assert.assertEquals(2.0001, v, 1e-6);
        }
    }

    @Test
    public void saveAll_Double_Precision_Test() throws SQLException {

        // This list will be inserted.
        List<SampleEntity> entities = new ArrayList<>();

        // Create the Entity to insert:
        SampleEntity entity = new SampleEntity();
        entity.col_double = 2.0001;

        entities.add(entity);

        PgBulkInsert<SampleEntity> pgBulkInsert = new PgBulkInsert<>(new SampleEntityMapping());

        pgBulkInsert.saveAll(PostgreSqlUtils.getPGConnection(connection), entities.stream());

        ResultSet rs = getAll();

        while (rs.next()) {
            double v = rs.getDouble("col_double");

            Assert.assertEquals(2.0001, v, 1e-10);
        }
    }

    private boolean createTable() throws SQLException {
        String sqlStatement = String.format("CREATE TABLE %s.unit_test\n", schema) +
                "            (\n" +
                "                col_smallint smallint,\n" +
                "                col_integer integer,\n" +
                "                col_long bigint,\n" +
                "                col_real real,\n" +
                "                col_double double precision,\n" +
                "                col_bytea bytea,\n" +
                "                col_boolean boolean\n" +
                "            );";

        Statement statement = connection.createStatement();

        return statement.execute(sqlStatement);
    }

    private ResultSet getAll() throws SQLException {
        String sqlStatement = String.format("SELECT * FROM %s.unit_test", schema);

        Statement statement = connection.createStatement();

        return statement.executeQuery(sqlStatement);
    }

}
