# CHANGELOG #

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
