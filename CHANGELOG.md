# CHANGELOG #

## 3.1 ##

* Thanks to [@kowalczm](https://github.com/kowalczm) for adding support for primitive types (``byte``, ``int``, ``float``, ``double``, ...), see [Pull Request #27](https://github.com/bytefish/PgBulkInsert/pull/27). This makes boxing primitive types unnecessary now and will make it easier to integrate the library into existing applications.

## 3.0 ##

* The library now uses the standard functional interfaces of Java, see [Pull Request #24](https://github.com/bytefish/PgBulkInsert/pull/24). The ``ByteArrayValueHandler`` now uses primite Byte Arrays (``byte[]``), which makes boxing unnecessary. This was a breaking change in the API and as such, the major revision was increased to 3.0.
* An overloaded ``saveAll`` method have been added by [@kowalczm](https://github.com/kowalczm) in [Pull Request #25](https://github.com/bytefish/PgBulkInsert/pull/25). You can now save a ``Collection`` instead of being forced to use streams.

## 2.2 ##

* Thanks to [@momania](https://github.com/momania) for fixing [Issue #20](https://github.com/bytefish/PgBulkInsert/issues/20), which was a severe bug with the mapping procedures. This led to improvements for the ``AbstractMapping`` API now exposing almost all types as arrays.

## 2.1 ##

* [Issue #16] updated the PostgreSQL JDBC driver to Version ``42.2.2``. A configurable buffer size was added to the API for improving the throughput to PostgreSQL. Thanks to [@kowalczm](https://github.com/kowalczm) for both improvements.

## 2.0 ##

* [Pull Request #19](https://github.com/bytefish/PgBulkInsert/pull/19) was a major refactoring of the API by creating an ``AbstractMapping``. This now separates the Mapping and the actual saving, making it possible (and easier) to reuse the PgBulkInsert Mapping API as a standalone implementation. Thanks to [@The-Alchemist](https://github.com/The-Alchemist) for raising the issue and making improvements to the library.