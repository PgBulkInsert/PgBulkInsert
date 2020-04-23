// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.pgsql.handlers;

import de.bytefish.pgbulkinsert.PgBulkInsert;
import de.bytefish.pgbulkinsert.mapping.AbstractMapping;
import de.bytefish.pgbulkinsert.pgsql.model.geometric.*;
import de.bytefish.pgbulkinsert.pgsql.model.network.MacAddress;
import de.bytefish.pgbulkinsert.util.PostgreSqlUtils;
import de.bytefish.pgbulkinsert.utils.TransactionalTestBase;
import org.junit.Assert;
import org.junit.Test;
import org.postgresql.geometric.*;
import org.postgresql.util.PGobject;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class NetworkTypesTest extends TransactionalTestBase {

    private class NetworkEntity {

        private MacAddress col_mac_addr;

        public MacAddress getCol_mac_addr() {
            return col_mac_addr;
        }

        public void setCol_mac_addr(MacAddress col_mac_addr) {
            this.col_mac_addr = col_mac_addr;
        }
    }

    @Override
    protected void onSetUpInTransaction() throws Exception {
        createTable();
    }

    @Override
    protected void onSetUpBeforeTransaction() throws Exception {

    }

    private class NetworkEntityMapping extends AbstractMapping<NetworkEntity> {

        public NetworkEntityMapping() {
            super(schema, "network_table");

            mapMacAddress("col_mac_addr", NetworkEntity::getCol_mac_addr);
        }
    }

    private boolean createTable() throws SQLException {
        String sqlStatement = String.format("CREATE TABLE %s.network_table(\n", schema) +
                "                col_mac_addr macaddr \n" +
                "            );";

        Statement statement = connection.createStatement();

        return statement.execute(sqlStatement);
    }


    @Test
    public void saveAll_Point_Test() throws SQLException {

        // This list will be inserted.
        List<NetworkEntity> entities = new ArrayList<>();

        byte[] macAddressBytes = new byte[6];

        macAddressBytes[0] = 1;
        macAddressBytes[1] = 2;
        macAddressBytes[2] = 3;
        macAddressBytes[3] = 4;
        macAddressBytes[4] = 5;
        macAddressBytes[5] = 6;


        MacAddress macAddress = new MacAddress(macAddressBytes);

        // Build the Entities to store:
        NetworkEntity entity = new NetworkEntity();

        entity.setCol_mac_addr(macAddress);

        entities.add(entity);

        // Save them:
        PgBulkInsert<NetworkEntity> pgBulkInsert = new PgBulkInsert<>(new NetworkEntityMapping());

        pgBulkInsert.saveAll(PostgreSqlUtils.getPGConnection(connection), entities.stream());

        ResultSet rs = getAll();

        while (rs.next()) {
            PGobject v = (PGobject) rs.getObject("col_mac_addr");

            Assert.assertNotNull(v);

            Assert.assertEquals("macaddr", v.getType());
            Assert.assertEquals("01:02:03:04:05:06", v.getValue());
        }
    }

    private ResultSet getAll() throws SQLException {
        String sqlStatement = String.format("SELECT * FROM %s.network_table", schema);

        Statement statement = connection.createStatement();

        return statement.executeQuery(sqlStatement);
    }



}
