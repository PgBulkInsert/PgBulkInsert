.. _quickstart:

Quickstart
==========

Imagine we want to bulk insert a large amount of persons into a PostgreSQL database. Each :code:`Person` 
has a first name, a last name and a birthdate.

Database Table
~~~~~~~~~~~~~~

The table in the PostgreSQL database might look like this:

.. code-block:: sql

    CREATE TABLE sample.unit_test
    (
        first_name text,
        last_name text,
        birth_date date
    );

Domain Model
~~~~~~~~~~~~

The domain model in the application might look like this:

.. code-block:: java

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

Implementing the Bulk Inserter
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Then you have to implement the :code:`PgBulkInsert<Person>`, which defines the mapping between the 
table and the domain model.

.. code-block:: java

    public class PersonBulkInserter extends PgBulkInsert<Person>
    {
        public PersonBulkInserter() {
            super("sample", "unit_test");
    
            mapString("first_name", Person::getFirstName);
            mapString("last_name", Person::getLastName);
            mapDate("birth_date", Person::getBirthDate);
        }
    }

Using the Bulk Inserter
~~~~~~~~~~~~~~~~~~~~~~~

And finally we can write a Unit Test to insert :code:`100000` Persons into the database. You can find 
the entire Unit Test on GitHub: `IntegrationTest.java <https://github.com/bytefish/PgBulkInsert/blob/master/PgBulkInsert/src/test/de/bytefish/pgbulkinsert/de/bytefish/pgbulkinsert/IntegrationTest.java>`_. 

.. code-block:: java

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


	
.. _JTinyCsvParser: https://github.com/bytefish/JTinyCsvParser
.. MIT License: https://opensource.org/licenses/MIT