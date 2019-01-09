package de.bytefish.pgbulkinsert.pgsql.handlers;

import de.bytefish.pgbulkinsert.PgBulkInsert;
import de.bytefish.pgbulkinsert.mapping.AbstractMapping;
import de.bytefish.pgbulkinsert.util.PostgreSqlUtils;
import de.bytefish.pgbulkinsert.utils.TransactionalTestBase;
import mil.nga.sf.Geometry;
import mil.nga.sf.Point;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class PostgisExtensionTest extends TransactionalTestBase {

    private class PostgisEntity {

        private Geometry col_postgis;

        public Geometry getCol_postgis() {
            return col_postgis;
        }

        public void setCol_postgis(Geometry col_postgis) {
            this.col_postgis = col_postgis;
        }
    }

    @Override
    protected void onSetUpInTransaction() throws Exception {
        createTable();
    }

    @Override
    protected void onSetUpBeforeTransaction() throws Exception {

    }

    private class PostgisEntityMapping extends AbstractMapping<PostgisExtensionTest.PostgisEntity> {

        public PostgisEntityMapping() {
            super(schema, "postgis_table");
            mapPostgis("col_postgis", PostgisExtensionTest.PostgisEntity::getCol_postgis);
        }
    }

    private boolean createTable() throws SQLException {
        String sqlStatement = String.format("CREATE TABLE %s.postgis_table(\n", schema) +
                "                col_postgis Geometry(POINT) \n" +
                "            );";

        Statement statement = connection.createStatement();

        return statement.execute(sqlStatement);
    }

    @Test
    @Ignore("This Test Requires the postgis extension to be enabled.")
    public void saveAll_Postgis_Test() throws SQLException {

        // This list will be inserted.
        List<PostgisExtensionTest.PostgisEntity> entities = new ArrayList<>();

        // Create the Map to Store:
        Geometry postgisData = new Point(1, 1);

        // Create the Entity to insert:
        PostgisExtensionTest.PostgisEntity entity = new PostgisExtensionTest.PostgisEntity();
        entity.setCol_postgis(postgisData);

        entities.add(entity);

        PgBulkInsert<PostgisExtensionTest.PostgisEntity> bulkInsert = new PgBulkInsert<>(new PostgisExtensionTest.PostgisEntityMapping());

        bulkInsert.saveAll(PostgreSqlUtils.getPGConnection(connection), entities.stream());

        ResultSet rs = getAll();

        while (rs.next()) {
            String wkt = rs.getString("geom");
            Assert.assertEquals("POINT(1 1)", wkt);
        }
    }

    private ResultSet getAll() throws SQLException {
        String sqlStatement = String.format("SELECT ST_AsText(col_postgis) AS geom FROM %s.postgis_table", schema);
        Statement statement = connection.createStatement();
        return statement.executeQuery(sqlStatement);
    }

}
