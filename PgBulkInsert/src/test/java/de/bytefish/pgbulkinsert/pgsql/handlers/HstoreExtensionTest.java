// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.pgsql.handlers;

import de.bytefish.pgbulkinsert.PgBulkInsert;
import de.bytefish.pgbulkinsert.mapping.AbstractMapping;
import de.bytefish.pgbulkinsert.util.PostgreSqlUtils;
import de.bytefish.pgbulkinsert.utils.TransactionalTestBase;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class HstoreExtensionTest extends TransactionalTestBase {

    private class HStoreEntity {

        private Map<String, String> col_hstore;

        public Map<String, String> getCol_hstore() {
            return col_hstore;
        }

        public void setCol_hstore(Map<String, String> col_hstore) {
            this.col_hstore = col_hstore;
        }
    }

    @Override
    protected void onSetUpInTransaction() throws Exception {
        createTable();
    }

    @Override
    protected void onSetUpBeforeTransaction() throws Exception {

    }

    private class HStoreEntityMapping extends AbstractMapping<HStoreEntity> {

        public HStoreEntityMapping() {
            super(schema, "hstore_table");

            mapHstore("col_hstore", HStoreEntity::getCol_hstore);
        }
    }

    private boolean createTable() throws SQLException {
        String sqlStatement = String.format("CREATE TABLE %s.hstore_table(\n", schema) +
                "                col_hstore hstore \n" +
                "            );";

        Statement statement = connection.createStatement();

        return statement.execute(sqlStatement);
    }

    @Test
    @Ignore("This Test Requires the hstore extension to be enabled.")
    public void saveAll_Hstore_Test() throws SQLException {

        // This list will be inserted.
        List<HStoreEntity> entities = new ArrayList<>();

        // Create the Map to Store:
        Map<String, String> hstoreData = new HashMap<>();


        hstoreData.put("Philipp", "Cool Cool Cool!");

        // Create the Entity to insert:
        HStoreEntity entity = new HStoreEntity();

        entity.setCol_hstore(hstoreData);

        entities.add(entity);

        PgBulkInsert<HStoreEntity> bulkInsert = new PgBulkInsert<>(new HStoreEntityMapping());

        bulkInsert.saveAll(PostgreSqlUtils.getPGConnection(connection), entities.stream());

        ResultSet rs = getAll();

        while (rs.next()) {
            Map<String, String> v = (Map<String,String>) rs.getObject("col_hstore");

            Assert.assertEquals(1, v.size());
            Assert.assertEquals(true, v.containsKey("Philipp"));
            Assert.assertEquals("Cool Cool Cool!", v.get("Philipp"));
        }
    }

    private ResultSet getAll() throws SQLException {
        String sqlStatement = String.format("SELECT * FROM %s.hstore_table", schema);

        Statement statement = connection.createStatement();

        return statement.executeQuery(sqlStatement);
    }




}
