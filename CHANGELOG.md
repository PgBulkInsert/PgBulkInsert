# CHANGELOG #

## 4.0 ##

Added support for [Range Types](https://www.postgresql.org/docs/current/rangetypes.html):

* int4range
* int8range
* numrange
* tsrange
* tstzrange
* daterange

Thanks to [@csimplestring](https://github.com/csimplestring) for help and valuable feedback!

## 4.0-alpha-2 ##

* Bugfixes for the Range Type

## 4.0-alpha-1 ##

* Added support for Ranges, such as ``tstzrange``. Thanks to [@csimplestring](https://github.com/csimplestring) for the Feature request!
    * Use ``mapTsTzRange`` if you are using the ``AbstractMapping``
    * Use ``setTsTzRange`` if you are using the ``SimpleRowWriter``

## 3.8 ##

* Many improvements added by [@tangyibo](https://github.com/tangyibo/)!
    * Closing the Stream of the ``SimpleRowWriter``, so it doesn't block anymore.
    * Improved error logging to get to the underlying cause of the error in the JDBC driver.
    * Added Null Character Handling, so Null Bytes (``0x00``) do not crash the ``COPY``

You can enable the Null Character Handling like this. It removes all ``0x00`` occurences and replaces them with an empty string:
    
```java
// Create the Table Definition:
SimpleRowWriter.Table table = new SimpleRowWriter.Table(schema, tableName, columnNames);

// Create the Writer:
SimpleRowWriter writer = new SimpleRowWriter(table);

// Enable the Null Character Handler:
writer.enableNullCharacterHandler();
```

If you need to customize the Null Character Handling, then you can use the ``setNullCharacterHandler(Function<String, String> nullCharacterHandler)`` function.
    
## 3.5 ##

* Updates the PostgreSQL JDBC driver to Version ``42.2.9``.

## 3.4 ##

* Improving the ``ZonedDateTime`` handling.

## 3.3 ##

* [@robsonbittencourt](https://github.com/robsonbittencourt) reported a severe bug in the Numeric Value handling. Everyone should update to 3.3!

## 3.2 ##

* [@kowalczm](https://github.com/kowalczm) further improved the ``ByteArrayValueHandler`` in [Pull Request #29](https://github.com/bytefish/PgBulkInsert/pull/29).

## 3.1 ##

* Thanks to [@kowalczm](https://github.com/kowalczm) for adding support for primitive types (``byte``, ``int``, ``float``, ``double``, ...), see [Pull Request #27](https://github.com/bytefish/PgBulkInsert/pull/27). This makes boxing primitive types unnecessary now and will make it easier to integrate the library into existing applications.

## 3.0 ##

* The library now uses the standard functional interfaces of Java, see [Pull Request #24](https://github.com/bytefish/PgBulkInsert/pull/24). The ``ByteArrayValueHandler`` now uses primitive Byte Arrays (``byte[]``), which makes boxing unnecessary. This was a breaking change in the API and as such, the major revision was increased to 3.0.
* An overloaded ``saveAll`` method has been added by [@kowalczm](https://github.com/kowalczm) in [Pull Request #25](https://github.com/bytefish/PgBulkInsert/pull/25). You can now save a ``Collection`` instead of being forced to use streams.

## 2.2 ##

* Thanks to [@momania](https://github.com/momania) for fixing [Issue #20](https://github.com/bytefish/PgBulkInsert/issues/20), which was a severe bug with the mapping procedures. This led to improvements for the ``AbstractMapping`` API now exposing almost all types as arrays.

## 2.1 ##

* [Issue #16](https://github.com/bytefish/PgBulkInsert/issues/16) updates the PostgreSQL JDBC driver to Version ``42.2.2``. A configurable buffer size was added to the API for improving the throughput to PostgreSQL. Thanks to [@kowalczm](https://github.com/kowalczm) for both improvements.

## 2.0 ##

* [Pull Request #19](https://github.com/bytefish/PgBulkInsert/pull/19) was a major refactoring of the API by creating an ``AbstractMapping``. This now separates the Mapping and the actual saving, making it possible (and easier) to reuse the PgBulkInsert Mapping API as a standalone implementation. As a major breaking change, the major revision has been set to 2.0. Thanks to [@The-Alchemist](https://github.com/The-Alchemist) for raising the issue and making improvements to the library.

## 1.4 ##

* [Issue #11](https://github.com/bytefish/PgBulkInsert/issues/11) added Microsecond resolution for Timestamps. Thanks to [@zach-m](https://github.com/zach-m) for raising the issue.

## 1.3 ##

* [Issue #10](https://github.com/bytefish/PgBulkInsert/issues/10) added support for the Numeric Data Type to the API. Thanks to [@li-xiangdong](https://github.com/li-xiangdong) for raising the issue.

## 1.2 ##

* [Commit b4543db](https://github.com/bytefish/PgBulkInsert/commit/b4543db958437ab88a1683b576e638a65bc11710) restructured the Value Handler API, which was an internal implementation detail, so the major revision wasn't increased.

## 1.1 ##

* Added Geometric Data Types, JSONB.

## 1.0 ##

* Initial Release.
