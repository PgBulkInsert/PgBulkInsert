.. _overview:

Overview
========

PgBulkInsert is a Java library for Bulk Inserts to PostgreSQL using the Binary COPY Protocol.

It provides a wrapper around the PostgreSQL `COPY command`_:

    The COPY command is a PostgreSQL specific feature, which allows efficient bulk import or export 
    of data to and from a table. This is a much faster way of getting data in and out of a table 
    than using INSERT and SELECT.

Setup
~~~~~

You can add the following dependencies to your :code:`pom.xml` to include `PgBulkInsert`_ in your project.

.. code-block:: xml

    <dependency>
        <groupId>de.bytefish</groupId>
        <artifactId>pgbulkinsert</artifactId>
        <version>1.1</version>
    </dependency>
    
Supported PostgreSQL Types
~~~~~~~~~~~~~~~~~~~~~~~~~~

* `Numeric Types <http://www.postgresql.org/docs/current/static/datatype-numeric.html>`_
    * smallint
    * integer
    * bigint
    * real
    * double precision
* `Date/Time Types <http://www.postgresql.org/docs/current/static/datatype-datetime.html>`_
    * timestamp
    * date
* `Character Types <http://www.postgresql.org/docs/current/static/datatype-character.html>`_
    * text
* `Character Types <http://www.postgresql.org/docs/current/static/datatype-character.html>`_
    * text
* `JSON Types <https://www.postgresql.org/docs/current/static/datatype-json.html>`_
    * jsonb
* `Boolean Type <http://www.postgresql.org/docs/current/static/datatype-boolean.html>`_
    * boolean
* `Binary Data Types <http://www.postgresql.org/docs/current/static/datatype-binary.html>`_
    * bytea
* `Network Address Types <http://www.postgresql.org/docs/current/static/datatype-net-types.html>`_
    * inet (IPv4, IPv6)
    * macaddr
* `UUID Type <http://www.postgresql.org/docs/current/static/datatype-uuid.html>`_
    * uuid
* `Array Type <https://www.postgresql.org/docs/current/static/arrays.html>`_
    * One-Dimensional Arrays
* `hstore <https://www.postgresql.org/docs/current/static/hstore.html>`_
    * hstore
* `Geometric Types <https://www.postgresql.org/docs/current/static/datatype-geometric.html>`_
    * point
    * line
    * lseg
    * box
    * path
    * polygon
    * circle
    
.. _PgBulkInsert: https://github.com/bytefish/PgBulkInsert    
.. _COPY Command: http://www.postgresql.org/docs/current/static/sql-copy.html