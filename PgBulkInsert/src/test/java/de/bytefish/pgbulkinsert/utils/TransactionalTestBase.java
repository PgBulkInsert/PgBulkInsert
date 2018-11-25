// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import ru.yandex.qatools.embed.postgresql.EmbeddedPostgres;
import ru.yandex.qatools.embed.postgresql.distribution.Version;

public abstract class TransactionalTestBase {

    protected Connection connection;

    protected static final String SCHEMA = "sample";
    
    private static String url;
    private static EmbeddedPostgres postgres;
    
    @BeforeClass
    public static void beforeClass() throws Exception {
    	postgres = new EmbeddedPostgres(Version.V9_6_11);
    	// predefined data directory
    	url = postgres.start("localhost", 5432, "sampledb", "philipp", "test_pwd");
    	createSchema();
    }
    
    @AfterClass
    public static void afterClass() {
    	postgres.stop();
    }

    @Before
    public void setUp() throws Exception {
    	connection = DriverManager.getConnection(url);

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
    
    
    private static void createSchema() throws Exception {
    	try(Connection connection = DriverManager.getConnection(url)) {
            Statement statement = connection.createStatement();
            statement.execute("CREATE SCHEMA " + SCHEMA);
    	}
    }

    protected void onSetUpInTransaction() throws Exception {}

    protected void onSetUpBeforeTransaction() throws Exception {}

    protected void onTearDownInTransaction() throws Exception {}

    protected void onTearDownAfterTransaction() throws Exception {}
}