.. _bulk_processor:

Bulk Processor API
==================

Integrating bulk inserts into existing applications can be tricky, because you often don't want 
to deal with batching of entities or you can't obscure existing interfaces. This is where the 
:code:`BulkProcessor` API of `PgBulkInsert`_ fits in.

The :code:`BulkProcessor` provides a simple interface, which flushes bulk operations automatically 
based on the number of entities or after a given time period. 

BulkProcessor
~~~~~~~~~~~~~

Imagine we want to bulk insert a large amount of persons into a PostgreSQL database using a 
:code:`BulkProcessor`. Each :code:`Person` has a first name, a last name and a birthdate.

Database Table
~~~~~~~~~~~~~~

The table in the PostgreSQL database might look like this:

.. code-block:: sql

    CREATE TABLE sample.person_example
    (
        first_name text,
        last_name text,
        birth_date date
    );

Domain Model
~~~~~~~~~~~~

The domain model in the application might look like this:

.. code-block:: java

    // Copyright (c) Philipp Wagner. All rights reserved.
    // Licensed under the MIT license. See LICENSE file in the project root for full license information.
    
    package model;
    
    import java.time.LocalDate;
    
    public class Person {
    
        private String firstName;
    
        private String lastName;
    
        private LocalDate birthDate;
    
        public Person() {}
    
        public String getFirstName() {
            return firstName;
        }
    
        public void setFirstName(String firstName) {
            this.firstName = firstName;
        }
    
        public String getLastName() {
            return lastName;
        }
    
        public void setLastName(String lastName) {
            this.lastName = lastName;
        }
    
        public LocalDate getBirthDate() {
            return birthDate;
        }
    
        public void setBirthDate(LocalDate birthDate) {
            this.birthDate = birthDate;
        }
    
    }

Implementing the Bulk Inserter
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Then the mapping between the database table and the domain model has to defined. This is done by 
implementing the abstract base class :code:`PgBulkInsert<TEntity>`, where :code:`TEntity` is the 
:code:`Person` class in this example.

.. code-block:: java

    // Copyright (c) Philipp Wagner. All rights reserved.
    // Licensed under the MIT license. See LICENSE file in the project root for full license information.
    
    package mapping;
    
    import model.Person;
    
    import de.bytefish.pgbulkinsert.PgBulkInsert;
    
    public class PersonBulkInsert extends PgBulkInsert<Person>
    {
        public PersonBulkInsert() {
            super("sample", "person_example");
    
            mapString("first_name", Person::getFirstName);
            mapString("last_name", Person::getLastName);
            mapDate("birth_date", Person::getBirthDate);
        }
    }

Connection Pooling (with DBCP2)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The :code:`BulkProcessor` needs a way to obtain a :code:`Connection` for the database access. That's why 
the :code:`BulkProcessor` takes a factory for creating connections. In my projects I simply use the great 
`DBCP2`_ project for handling database connections.

You can add the following dependencies to your :code:`pom.xml` to include `DBCP2`_ in your project:

.. code-block:: xml

    <dependency>
        <groupId>org.apache.commons</groupId>
        <artifactId>commons-dbcp2</artifactId>
        <version>2.0.1</version>
    </dependency>

The Connection Factory for the :code:`BulkProcessor` can now be implemented.

.. code-block:: java

    // Copyright (c) Philipp Wagner. All rights reserved.
    // Licensed under the MIT license. See LICENSE file in the project root for full license information.
    
    package connection;
    
    import de.bytefish.pgbulkinsert.functional.Func1;
    import org.apache.commons.dbcp2.BasicDataSource;
    
    import java.net.URI;
    import java.sql.Connection;
    
    public class PooledConnectionFactory implements Func1<Connection> {
    
        private final BasicDataSource connectionPool;
    
        public PooledConnectionFactory(URI databaseUri) {
            this.connectionPool = new BasicDataSource();
    
            initializeConnectionPool(connectionPool, databaseUri);
        }
    
        private void initializeConnectionPool(BasicDataSource connectionPool, URI databaseUri) {
            final String dbUrl = "jdbc:postgresql://" + databaseUri.getHost() + databaseUri.getPath();
    
            if (databaseUri.getUserInfo() != null) {
                connectionPool.setUsername(databaseUri.getUserInfo().split(":")[0]);
                connectionPool.setPassword(databaseUri.getUserInfo().split(":")[1]);
            }
            connectionPool.setDriverClassName("org.postgresql.Driver");
            connectionPool.setUrl(dbUrl);
            connectionPool.setInitialSize(1);
        }
    
        @Override
        public Connection invoke() throws Exception {
            return connectionPool.getConnection();
        }
    }

Implementing the Application
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

And finally we can implement the sample application, which is using the :code:`BulkProcessor`. The 
:code:`BulkProcessor` takes a so called :code:`BulkWriteHandler`. This :code:`BulkWriteHandler` handles 
the writing of a list of batched entities. 

The :code:`BulkProcessor` is thread-safe, so it can safely be used from multiple threads.

The example application writes :code:`1000` Person entities to the database, using a Bulk Size of 
:code:`100` entities.

.. code-block:: java

    // Copyright (c) Philipp Wagner. All rights reserved.
    // Licensed under the MIT license. See LICENSE file in the project root for full license information.
    
    package app;
    
    import connection.PooledConnectionFactory;
    import de.bytefish.pgbulkinsert.pgsql.processor.BulkProcessor;
    import de.bytefish.pgbulkinsert.pgsql.processor.handler.BulkWriteHandler;
    import mapping.PersonBulkInsert;
    import model.Person;
    
    import java.net.URI;
    import java.time.LocalDate;
    import java.util.ArrayList;
    import java.util.List;
    
    public class BulkProcessorApp {
    
        public static void main(String[] args) throws Exception {
            // Database to connect to:
            URI databaseUri = URI.create("postgres://philipp:test_pwd@127.0.0.1:5432/sampledb");
            // Bulk Actions after which the batched entities are written:
            final int bulkSize = 100;
            // Create a new BulkProcessor:
            try(BulkProcessor<Person> bulkProcessor = new BulkProcessor<>(new BulkWriteHandler<>(new PersonBulkInsert(), new PooledConnectionFactory(databaseUri)), bulkSize)) {
                // Create some Test data:
                List<Person> thousandPersons = getPersonList(1000);
                // Now process them with the BulkProcessor:
                for (Person p : thousandPersons) {
                    bulkProcessor.add(p);
                }
            }
        }
    
        private static List<Person> getPersonList(int numPersons) {
            List<Person> persons = new ArrayList<>();
    
            for (int pos = 0; pos < numPersons; pos++) {
                Person p = new Person();
    
                p.setFirstName("Philipp");
                p.setLastName("Wagner");
                p.setBirthDate(LocalDate.of(1986, 5, 12));
    
                persons.add(p);
            }
    
            return persons;
        }
    
    }

.. _COPY command: http://www.postgresql.org/docs/current/static/sql-copy.html
.. _DBCP2: https://commons.apache.org/proper/commons-dbcp/
.. _PgBulkInsert: https://github.com/bytefish/PgBulkInsert
.. _JTinyCsvParser: https://github.com/bytefish/JTinyCsvParser