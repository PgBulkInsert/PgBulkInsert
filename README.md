# PgBulkInsert #

[MIT License]: https://opensource.org/licenses/MIT
[COPY command]: http://www.postgresql.org/docs/current/static/sql-copy.html
[PgBulkInsert]: https://github.com/bytefish/PgBulkInsert

PgBulkInsert is a Java library for Bulk Inserts to PostgreSQL using the Binary COPY Protocol. 

It provides a wrapper around the PostgreSQL [COPY command]:

> The [COPY command] is a PostgreSQL specific feature, which allows efficient bulk import or export of 
> data to and from a table. This is a much faster way of getting data in and out of a table than using 
> INSERT and SELECT.

## Setup ##

[PgBulkInsert] is available in the Central Maven Repository. 

You can add the following dependencies to your pom.xml to include [PgBulkInsert] in your project.

```xml
<dependency>
	<groupId>de.bytefish</groupId>
	<artifactId>pgbulkinsert</artifactId>
	<version>1.3</version>
</dependency>
```

## Documentation ##

The [PgBulkInsert] documentation is located at:

* [http://bytefish.github.io/PgBulkInsert/](http://bytefish.github.io/PgBulkInsert/)


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
    * date
* [Character Types](http://www.postgresql.org/docs/current/static/datatype-character.html)
    * text
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

## License ##

PgBulkInsert is released with under terms of the [MIT License]:

* [https://github.com/bytefish/PgBulkInsert](https://github.com/bytefish/PgBulkInsert)

## Basic Usage ##

Imagine we want to bulk insert a large amount of persons into a PostgreSQL database. Each ``Person`` has a first name, a last name and a birthdate.

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

Then you have to implement the ``PgBulkInsert<Person>``, which defines the mapping between the table and the domain model.

```java
public class PersonBulkInserter extends PgBulkInsert<Person>
{
    public PersonBulkInserter() {
        super("sample", "unit_test");

        mapString("first_name", Person::getFirstName);
        mapString("last_name", Person::getLastName);
        mapDate("birth_date", Person::getBirthDate);
    }
}
```

### Using the Bulk Inserter ###

And finally we can write a Unit Test to insert ``100000`` Persons into the database. You can find the entire Unit Test on GitHub: [IntegrationTest.java](https://github.com/bytefish/PgBulkInsert/blob/master/PgBulkInsert/src/test/de/bytefish/pgbulkinsert/de/bytefish/pgbulkinsert/IntegrationTest.java). 

```java
@Test
public void bulkInsertPersonDataTest() throws SQLException {
    // Create a large list of Persons:
    List<Person> persons = getPersonList(100000);
    
    // Create the BulkInserter:
    PersonBulkInserter personBulkInserter = new PersonBulkInserter();
    
    // Now save all entities of a given stream:
    personBulkInserter.saveAll(PostgreSqlUtils.getPGConnection(connection), persons.stream());
    
    // And assert all have been written to the database:
    Assert.assertEquals(100000, getRowCount());
}

private List<Person> getPersonList(int numPersons) {
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
```

## BulkProcessor ##

* [Working with a BulkProcessor in PgBulkInsert](http://bytefish.de/blog/pgbulkinsert_bulkprocessor/)

## Resources ##

* [Npgsql](https://github.com/npgsql/npgsql)
* [Postgres on the wire - A look at the PostgreSQL wire protocol (PGCon 2014)](https://www.pgcon.org/2014/schedule/attachments/330_postgres-for-the-wire.pdf)


