# PgBulkInsert #

[MIT License]: https://opensource.org/licenses/MIT
[COPY command]: http://www.postgresql.org/docs/current/static/sql-copy.html
[PgBulkInsert]: https://github.com/bytefish/PgBulkInsert
[Npgsql]: https://github.com/npgsql/npgsql

![](https://github.com/PgBulkInsert/PgBulkInsert/workflows/Java%20CI%20with%20Maven/badge.svg)
![](https://maven-badges.herokuapp.com/maven-central/de.bytefish.pgbulkinsert/pgbulkinsert-core/badge.svg)
[![Maintainability](https://api.codeclimate.com/v1/badges/311ce156ff026549ba7f/maintainability)](https://codeclimate.com/github/PgBulkInsert/PgBulkInsert/maintainability)
[![Test Coverage](https://api.codeclimate.com/v1/badges/311ce156ff026549ba7f/test_coverage)](https://codeclimate.com/github/PgBulkInsert/PgBulkInsert/test_coverage)

PgBulkInsert is a Java library for Bulk Inserts to PostgreSQL using the Binary COPY Protocol. 

It provides a wrapper around the PostgreSQL [COPY command]:

> The [COPY command] is a PostgreSQL specific feature, which allows efficient bulk import or export of 
> data to and from a table. This is a much faster way of getting data in and out of a table than using 
> INSERT and SELECT.

This project wouldn't be possible without the great [Npgsql] library, which has a beautiful implementation of the Postgres protocol.

## Setup ##

[PgBulkInsert] is available in the Central Maven Repository. 

You can add the following dependencies to your pom.xml to include [PgBulkInsert] in your project.

```xml
<dependency>
	<groupId>de.bytefish.pgbulkinsert</groupId>
	<artifactId>pgbulkinsert-core</artifactId>
	<version>6.0.0</version>
</dependency>

<dependency>
	<groupId>de.bytefish.pgbulkinsert</groupId>
	<artifactId>pgbulkinsert-rowwriter</artifactId>
	<version>6.0.0</version>
</dependency>
```

If you are working with Java8 you have to add a ``-jdk8`` to the package names:


```xml
<dependency>
	<groupId>de.bytefish.pgbulkinsert</groupId>
	<artifactId>pgbulkinsert-core-jdk8</artifactId>
	<version>6.0.0</version>
</dependency>

<dependency>
	<groupId>de.bytefish.pgbulkinsert</groupId>
	<artifactId>pgbulkinsert-rowwriter-jdk8</artifactId>
	<version>6.0.0</version>
</dependency>
```



## Supported PostgreSQL Types ##

* [Numeric Types](http://www.postgresql.org/docs/current/static/datatype-numeric.html)
    * smallint
    * integer
    * bigint
    * real
    * double precision
	* numeric
* [Date/Time Types](http://www.postgresql.org/docs/current/static/datatype-datetime.html)
    * timestamp
    * timestamptz
    * date
    * time
* [Character Types](http://www.postgresql.org/docs/current/static/datatype-character.html)
    * text
* [JSON Types](https://www.postgresql.org/docs/current/static/datatype-json.html)
    * jsonb
* [Boolean Type](http://www.postgresql.org/docs/current/static/datatype-boolean.html)
    * boolean
* [Binary Data Types](http://www.postgresql.org/docs/current/static/datatype-binary.html)
    * bytea
* [Network Address Types](http://www.postgresql.org/docs/current/static/datatype-net-types.html)
    * inet (IPv4, IPv6)
    * macaddr
* [UUID Type](http://www.postgresql.org/docs/current/static/datatype-uuid.html)
    * uuid
* [Array Type](https://www.postgresql.org/docs/current/static/arrays.html)
    * One-Dimensional Arrays
* [Range Type](https://www.postgresql.org/docs/current/rangetypes.html)
    * int4range
    * int8range
    * numrange
    * tsrange
    * tstzrange
    * daterange
* [hstore](https://www.postgresql.org/docs/current/static/hstore.html)
    * hstore
* [Geometric Types](https://www.postgresql.org/docs/current/static/datatype-geometric.html)
    * point
    * line
    * lseg
    * box
    * path
    * polygon
    * circle

## Usage ##

You can use the [PgBulkInsert] API in various ways.

The first one is to use the ``SimpleRowWriter`` when you don't have an explicit Java POJO, that matches a Table. The second way is to use an 
``AbstractMapping<TEntityType>`` to define a mapping between a Java POJO and a PostgreSQL table. The third way is to use the ``JpaMapping`` 
module, that allows you to reuse existing JPA mappings.

## Using the SimpleRowWriter ##

Using the ``SimpleRowWriter`` doesn't require you to define a separate mapping. It requires you to define the PostgreSQL table structure using 
a ``SimpleRowWriter.Table``, that has a schema name (optional), table name and column names:

```java
// Schema of the Table:
String schemaName = "sample";

// Name of the Table:
String tableName = "row_writer_test";

// Define the Columns to be inserted:
String[] columnNames = new String[] {
        "value_int",
        "value_text"
};

// Create the Table Definition:
SimpleRowWriter.Table table = new SimpleRowWriter.Table(schemaName, tableName, columnNames);
```

Once created you create the ``SimpleRowWriter`` by using the ``Table`` and a ``PGConnection``.

Now to write a row to PostgreSQL you call the ``startRow`` method. It expects you to pass a 
``Consumer<SimpleRow>`` into it, which defines what data to write to the row. The call to 
``startRow`` is synchronized, so it is safe to be called from multiple threads.

```java
// Create the Writer:
try(SimpleRowWriter writer = new SimpleRowWriter(table, pgConnection)) {

    // ... write your data rows:
    for(int rowIdx = 0; rowIdx < 10000; rowIdx++) {

        // ... using startRow and work with the row, see how the order doesn't matter:
        writer.startRow((row) -> {
            row.setText("value_text", "Hi");
            row.setInteger("value_int", 1);
        });
    }
}
```

So the complete example looks like this:

```java
public class SimpleRowWriterTest extends TransactionalTestBase {

    // ...
    
    @Test
    public void rowBasedWriterTest() throws SQLException {

        // Get the underlying PGConnection:
        PGConnection pgConnection = PostgreSqlUtils.getPGConnection(connection);

        // Schema of the Table:
        String schemaName = "sample";
        
        // Name of the Table:
        String tableName = "row_writer_test";

        // Define the Columns to be inserted:
        String[] columnNames = new String[] {
                "value_int",
                "value_text"
        };

        // Create the Table Definition:
        SimpleRowWriter.Table table = new SimpleRowWriter.Table(schemaName, tableName, columnNames);

        // Create the Writer:
        try(SimpleRowWriter writer = new SimpleRowWriter(table, pgConnection)) {

            // ... write your data rows:
            for(int rowIdx = 0; rowIdx < 10000; rowIdx++) {

                // ... using startRow and work with the row, see how the order doesn't matter:
                writer.startRow((row) -> {
                    row.setText("value_text", "Hi");
                    row.setInteger("value_int", 1);
                });

            }
        }

        // Now assert, that we have written 10000 entities:

        Assert.assertEquals(10000, getRowCount());
    }
}
```

### Handling Null Characters or... 'invalid byte sequence for encoding "UTF8": 0x00' ###

If you see the error message ``invalid byte sequence for encoding "UTF8": 0x00`` your data contains Null Characters. Although ``0x00`` is 
valid UTF-8 PostgreSQL does not support writing it, because it uses C-style string termination internally. 

PgBulkInsert allows you to enable a Null Value handling, that removes all ``0x00`` occurences and replaces them with an empty string:
    
```java
// Create the Table Definition:
SimpleRowWriter.Table table = new SimpleRowWriter.Table(schema, tableName, columnNames);

// Create the Writer:
SimpleRowWriter writer = new SimpleRowWriter(table);

// Enable the Null Character Handler:
writer.enableNullCharacterHandler();
```

If you need to customize the Null Character Handling, then you can use the ``setNullCharacterHandler(Function<String, String> nullCharacterHandler)`` function.

## Using the AbstractMapping ##

The ``AbstractMapping`` is the second possible way to map a POJO for usage in PgBulkInsert. Imagine we want to bulk insert a large amount of people 
into a PostgreSQL database. Each ``Person`` has a first name, a last name and a birthdate.

### Database Table ###

The table in the PostgreSQL database might look like this:

```sql
 CREATE TABLE sample.unit_test
(
    first_name text,
    last_name text,
    birth_date date
);
```

### Domain Model ###

The domain model in the application might look like this:

```java
private class Person {

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
```

### Bulk Inserter ###

Then you have to implement the ``AbstractMapping<Person>``, which defines the mapping between the table and the domain model.

```java
public class PersonMapping extends AbstractMapping<Person>
{
    public PersonMapping() {
        super("sample", "unit_test");

        mapText("first_name", Person::getFirstName);
        mapText("last_name", Person::getLastName);
        mapDate("birth_date", Person::getBirthDate);
    }
}
```

This mapping is used to create the ``PgBulkInsert<Person>``:

```java
PgBulkInsert<Person> bulkInsert = new PgBulkInsert<Person>(new PersonMapping());
```

### Using the Bulk Inserter ###

[IntegrationTest.java]: https://github.com/bytefish/PgBulkInsert/blob/master/PgBulkInsert/pgbulkinsert-core/src/test/java/de/bytefish/pgbulkinsert/integration/IntegrationTest.java

And finally we can write a Unit Test to insert ``100000`` people into the database. You can find the entire Unit Test on GitHub as [IntegrationTest.java].

```java
@Test
public void bulkInsertPersonDataTest() throws SQLException {
    // Create a large list of People:
    List<Person> personList = getPersonList(100000);
    // Create the BulkInserter:
    PgBulkInsert<Person> bulkInsert = new PgBulkInsert<Person>(new PersonMapping(schema));
    // Now save all entities of a given stream:
    bulkInsert.saveAll(PostgreSqlUtils.getPGConnection(connection), personList.stream());
    // And assert all have been written to the database:
    Assert.assertEquals(100000, getRowCount());
}

private List<Person> getPersonList(int num) {
    List<Person> personList = new ArrayList<>();

    for (int pos = 0; pos < num; pos++) {
        Person p = new Person();

        p.setFirstName("Philipp");
        p.setLastName("Wagner");
        p.setBirthDate(LocalDate.of(1986, 5, 12));

        personList.add(p);
    }

    return personList;
}
```

## Using JPA Mappings ##

### Adding a Dependency to JPA Module ###

In order to map between PgBulkInsert and an existing JPA Mapping you need to use the ``pgbulkinsert-jpa`` module and 
add it as a dependency to your application:

```xml
<dependency>
	<groupId>de.bytefish.pgbulkinsert</groupId>
	<artifactId>pgbulkinsert-jpa</artifactId>
	<version>6.0.0</version>
</dependency>
```

### Create the Mapping ###

To create the Mapping you simply need to pass your class, like this: ``new JpaMapping<>(SampleEntity.class);``.

Here is a complete example:

```java
// Copyright (c) Philipp Wagner. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package de.bytefish.pgbulkinsert.test.jpa;

import de.bytefish.pgbulkinsert.PgBulkInsert;
import de.bytefish.pgbulkinsert.jpa.JpaMapping;
import de.bytefish.pgbulkinsert.util.PostgreSqlUtils;
import de.bytefish.pgbulkinsert.utils.TransactionalTestBase;
import org.junit.Assert;
import org.junit.Test;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class JpaMappingTests extends TransactionalTestBase {

    @Entity
    @Table(name = "unit_test", schema = "sample")
    public class SampleEntity {

        @Id
        @Column(name = "id")
        private Long id;

        @Column(name = "int_field")
        private Integer intField;

        @Column(name = "text_field")
        private String textField;

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public Integer getIntField() {
            return intField;
        }

        public void setIntField(Integer intField) {
            this.intField = intField;
        }

        public String getTextField() {
            return textField;
        }

        public void setTextField(String textField) {
            this.textField = textField;
        }
    }


    @Override
    protected void onSetUpInTransaction() throws Exception {
        createTable();
    }

    @Test
    public void bulkImportSampleEntities() throws SQLException {
        // Create a large list of People:
        List<SampleEntity> personList = getSampleEntityList(100000);
        // Create the JpaMapping:
        JpaMapping<SampleEntity> mapping = new JpaMapping<>(SampleEntity.class);
        // Create the Bulk Inserter:
        PgBulkInsert<SampleEntity> bulkInsert = new PgBulkInsert<>(mapping);
        // Now save all entities of a given stream:
        bulkInsert.saveAll(PostgreSqlUtils.getPGConnection(connection), personList.stream());
        // And assert all have been written to the database:
        Assert.assertEquals(100000, getRowCount());
    }

    private List<SampleEntity> getSampleEntityList(int num) {
        List<SampleEntity> results = new ArrayList<>();

        for (int pos = 0; pos < num; pos++) {
            SampleEntity p = new SampleEntity();

            p.setId(pos + 1L);
            p.setIntField(pos);
            p.setTextField(Integer.toString(pos));

            results.add(p);
        }

        return results;
    }

    private boolean createTable() throws SQLException {

        String sqlStatement = String.format("CREATE TABLE %s.unit_test\n", schema) +
                "            (\n" +
                "                id int8,\n" +
                "                int_field int4,\n" +
                "                text_field text\n" +
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
```

### Define the Postgres Types ###

The ``JpaMapping`` makes a guess, what Postgres type you are going to use. But it could be wrong of course! Imagine you want 
to map an ``Enumerated`` to a database, but your table uses an ``int4`` (Integer) instead of a ``int2`` (Short). This problem 
is hard to solve with Reflection or additional JPA annotations. 

So the ``JpaMapping`` allows you to pass a map between column name and Postgres type into it:

```java
@Test
public void customEnumTypeMappingTest() throws SQLException {

    // Create the Map:        
    Map<String, DataType> postgresColumnMapping = new HashMap<>();

    postgresColumnMapping.put("enum_smallint_field_as_integer", DataType.Int4);

    // Create the JpaMapping and pass the map:
    JpaMapping<SampleEntity> mapping = new JpaMapping<>(SampleEntity.class, postgresColumnMapping);
    
    // ...
    
}
```

## Running the Tests ##

Running the Tests requires a PostgreSQL database. 

You have to configure the test database connection in the module ``pgbulkinsert-core`` and file ``db.properties``:

```ini
db.url=jdbc:postgresql://127.0.0.1:5432/sampledb
db.user=philipp
db.password=test_pwd
db.schema=public
```

The tests are transactional, that means any test data will be rolled back once a test finishes. But it probably makes 
sense to set up a separate ``db.schema`` for your tests, if you want to avoid polluting the ``public`` schema or have 
different permissions.

## License ##

PgBulkInsert is released with under terms of the [MIT License]:

* [https://github.com/bytefish/PgBulkInsert](https://github.com/bytefish/PgBulkInsert)


## Resources ##

* [Npgsql](https://github.com/npgsql/npgsql)
* [Postgres on the wire - A look at the PostgreSQL wire protocol (PGCon 2014)](https://www.pgcon.org/2014/schedule/attachments/330_postgres-for-the-wire.pdf)


