# Swift-Kuery-PostgreSQL
PostgreSQL plugin for Swift-Kuery framework

![Mac OS X](https://img.shields.io/badge/os-Mac%20OS%20X-green.svg?style=flat)
![Linux](https://img.shields.io/badge/os-linux-green.svg?style=flat)
![Apache 2](https://img.shields.io/badge/license-Apache2-blue.svg?style=flat)

## Summary
[PostgreSQL](https://www.postgresql.org/) plugin for [Swift-Kuery](https://github.com/IBM-Swift/Swift-Kuery) framework.

## PostgreSQL installation

### Linux
```
$ apt-get install libpq-dev
```

### macOS
```
$ sudo brew install postgresql
```

## Running Swift-Kuery-PostgreSQL

First create an instance of `Swift-Kuery-PostgreSQL` by calling:

```swift
public required init(host: String, port: Int32, options: [ConnectionOptions]?)
```
**Where:**
- *host* and *port* are the host and the port of PostgreSQL
- *ConnectionOptions*  an optional set of:
   * *options* - command-line options to be sent to the server
   * *databaseName* - the database name
   * *userName* - the user name
   * *password* - the user password
   * *connectionTimeout* - maximum wait for connection in seconds. Zero or not specified means wait indefinitely.

For more information please see [PostgreSQL manual](https://www.postgresql.org/docs/8.0/static/libpq.html#LIBPQ-CONNECT).

To establish a connection call:

```swift
public func connect(onCompletion: (QueryError?) -> ())
```

## License
This library is licensed under Apache 2.0. Full license text is available in [LICENSE](LICENSE.txt).
