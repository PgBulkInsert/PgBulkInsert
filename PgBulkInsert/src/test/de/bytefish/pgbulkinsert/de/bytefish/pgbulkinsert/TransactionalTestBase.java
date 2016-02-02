// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.de.bytefish.pgbulkinsert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public abstract class TransactionalTestBase {

    protected Connection connection;

    @Before
    public void setUp() throws SQLException {
        connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/sampledb", "philipp", "test_pwd");

        connection.setAutoCommit(false); //transaction block start
    }

    @After
    public void tearDown() throws SQLException {
        connection.close();
    }


}
