# CHANGELOG #

## 3.1 ##

* Thanks to [@kowalczm](https://github.com/kowalczm) for adding support for primitive types (``byte``, ``int``, ``float``, ``double``, ...), see [Pull Request #27](https://github.com/bytefish/PgBulkInsert/pull/27). This makes boxing primitive types unnecessary now and will make it easier to integrate the library into existing applications.

## 3.0 ##

* The library now uses the standard functional interfaces of Java, see [Pull Request #24](https://github.com/bytefish/PgBulkInsert/pull/24). The ``ByteArrayValueHandler`` now uses primite Byte Arrays (``byte[]``), which makes boxing unnecessary. This was a breaking change in the API and as such, the major revision was increased to 3.0.
* An overloaded ``saveAll`` method have been added by [@kowalczm](https://github.com/kowalczm) in [Pull Request #25](https://github.com/bytefish/PgBulkInsert/pull/25). You can now save a ``Collection`` instead of being forced to use streams.
