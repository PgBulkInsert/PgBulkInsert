// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.test.utils;

import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

public abstract class TransactionalTestBase {

    protected Connection connection;

    protected String schema;

    @Before
    public void setUp() throws Exception {
        Properties properties = getProperties("db.properties");

        connection = DriverManager.getConnection(
                properties.getProperty("db.url"),
                properties.getProperty("db.user"),
                properties.getProperty("db.password"));

        schema = properties.getProperty("db.schema");

        onSetUpBeforeTransaction();
        connection.setAutoCommit(false); // Start the Transaction:
        onSetUpInTransaction();
    }

    @After
    public void tearDown() throws Exception {

        onTearDownInTransaction();
        connection.rollback();
        onTearDownAfterTransaction();

        connection.close();
    }

    protected void onSetUpInTransaction() throws Exception {}

    protected void onSetUpBeforeTransaction() throws Exception {}

    protected void onTearDownInTransaction() throws Exception {}

    protected void onTearDownAfterTransaction() throws Exception {}

    private static Properties getProperties(String filename) {

        Properties props = new Properties();

        InputStream is = ClassLoader.getSystemResourceAsStream(filename);

        try {
            props.load(is);
        }
        catch (IOException e) {
            throw new RuntimeException("Could not load unittest.properties", e);
        }

        return props;
    }
}