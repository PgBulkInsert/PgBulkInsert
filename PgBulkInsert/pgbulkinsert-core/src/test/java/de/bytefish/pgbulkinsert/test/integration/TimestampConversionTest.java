// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.test.integration;

import de.bytefish.pgbulkinsert.PgBulkInsert;
import de.bytefish.pgbulkinsert.mapping.AbstractMapping;
import de.bytefish.pgbulkinsert.test.mapping.PersonMapping;
import de.bytefish.pgbulkinsert.test.model.Person;
import de.bytefish.pgbulkinsert.test.utils.TransactionalTestBase;
import de.bytefish.pgbulkinsert.util.PostgreSqlUtils;
import org.junit.Assert;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TimestampConversionTest extends TransactionalTestBase {

    @Override
    protected void onSetUpInTransaction() throws Exception {
        createTable();
    }

    public class EMail {

        private Timestamp emailCreateTime;

        public Timestamp getEmailCreateTime() {
            return emailCreateTime;
        }

        public void setEmailCreateTime(Timestamp emailCreateTime) {
            this.emailCreateTime = emailCreateTime;
        }
    }

    public class EMailMapping extends AbstractMapping<EMail>
    {
        public EMailMapping(String schema) {
            super(schema, "unit_test");

            mapTimeStamp("email_create_time", x -> x != null ? x.getEmailCreateTime().toLocalDateTime() : null);
        }
    }

    @Test
    public void timestampEntityExampleTest() throws SQLException {

        // Create the Timestamp as 2013-1-1 00:00:
        LocalDateTime emailCreationDate_asLocalDateTime = LocalDateTime.of(2013, 1, 1, 0,0);
        Timestamp emailCreationDate_asTimeStamp = Timestamp.valueOf(emailCreationDate_asLocalDateTime);

        // Create a sample Mail:
        EMail email = new EMail();

        email.setEmailCreateTime(emailCreationDate_asTimeStamp);

        // The List to insert:
        List<EMail> emails = Arrays.asList(email);

        // Create the BulkInserter:
        PgBulkInsert<EMail> bulkInsert = new PgBulkInsert<>(new EMailMapping(schema));

        // Now save all entities of a given stream:
        bulkInsert.saveAll(PostgreSqlUtils.getPGConnection(connection), emails);
        
        // And assert all have been written to the database:
        Assert.assertEquals(1, getRowCount());
    }

    private boolean createTable() throws SQLException {

        String sqlStatement = String.format("CREATE TABLE %s.unit_test\n", schema) +
                "            (\n" +
                "                email_create_time timestamp\n" +
                "            );";

        Statement statement = connection.createStatement();

        return statement.execute(sqlStatement);
    }

    private int getRowCount() throws SQLException {

        Statement s = connection.createStatement();

        ResultSet r = s.executeQuery(String.format("SELECT COUNT(*) AS rowcount FROM %s.unit_test", schema));
        r.next();
        int count = r.getInt("rowcount");
        r.close();

        return count;
    }

}
