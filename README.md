# Swift-Kuery-PostgreSQL
PostgreSQL plugin for Swift-Kuery framework

![Mac OS X](https://img.shields.io/badge/os-Mac%20OS%20X-green.svg?style=flat)
![Linux](https://img.shields.io/badge/os-linux-green.svg?style=flat)
![Apache 2](https://img.shields.io/badge/license-Apache2-blue.svg?style=flat)

## Summary
[PostgreSQL](https://www.postgresql.org/) plugin for the [Swift-Kuery](https://github.com/IBM-Swift/Swift-Kuery) framework. It enables you to use Swift-Kuery to manipulate data in PostgreSQL database.

## PostgreSQL client installation
To use Swift-Kuery-PostgreSQL you must have the appropriate PostgreSQL C-language client installed.

### macOS
```
$ brew install postgresql
```

### Linux
```
$ sudo apt-get install libpq-dev
```

## Using Swift-Kuery-PostgreSQL

First create an instance of `Swift-Kuery-PostgreSQL` by calling:

```swift
let connection = PostgreSQLConnection(host: host, port: port, options: [ConnectionOptions]?)
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
PostgreSQLConnection.connect(onCompletion: (QueryError?) -> ())
```
You now have a connection that can be used to execute SQL queries created using Swift-Kuery.

## Getting Started with Swift-Kuery-PostgreSQL locally

### Install PostgreSQL server

#### Mac
```
brew install postgresql
```

#### Ubuntu Linux
```
sudo apt-get install postgresql postgresql-contrib
```

Make sure you have the database running. This installation should have also installed two applications we need, namely (createdb and psql) which will be used as clients to your locally running PostgreSQL.
### Create a database
Let's create a database called `school`:
```
createdb school
```

### Create the tables
Now, let's create a couple tables we will need.

We will use the interactive session so open up the client to the database we created:

```
$ psql school
psql (9.5.4)
Type "help" for help.

school=#
```

First, create the student table:

```sql
CREATE TABLE student (
 studentId BIGSERIAL PRIMARY KEY,
 name varchar(100) NOT NULL CHECK (name <> '')
);
```

And now, create the grades table:

```sql
CREATE TABLE grades (
  key BIGSERIAL PRIMARY KEY,
  studentId integer NOT NULL,
  course varchar(40) NOT NULL,
  grade integer
);
```

### Populate the tables

First the students table:

```sql
INSERT INTO student VALUES (1, 'Tommy Watson');
INSERT INTO student VALUES (2, 'Fred Flintstone');
```

And then the grades table:

```sql
INSERT INTO grades (studentId, course, grade) VALUES (1, 'How to build your first computer', 99);
INSERT INTO grades (studentId, course, grade) VALUES (2, 'How to work at a rock quarry', 71);
```

### Use Swift-Kuery
Now we are set to connect to our database from Swift and use Swift-Kuery to query our data into our Swift application.

#### Create simple Swift executable
First create a directory for our project and then initialize it.

```
$ mkdir swift-kuery-play
$ cd swift-kuery-play
$ swift package init --type executable
Creating executable package: swift-kuery-play
Creating Package.swift
Creating .gitignore
Creating Sources/
Creating Sources/main.swift
Creating Tests/
$
```

Now, add Swift-Kuery as a dependency for our project.
Edit Package.swift to contain:

```swift
import PackageDescription

let package = Package(
    name: "swift-kuery-play",

	dependencies: [
		.Package(url: "https://github.com/IBM-Swift/HeliumLogger.git",       majorVersion: 1, minor: 1),
		.Package(url: "https://github.com/IBM-Swift/Kitura.git",             majorVersion: 1, minor: 2),
		.Package(url: "https://github.com/IBM-Swift/Swift-Kuery-PostgreSQL", majorVersion: 0, minor: 2)
	]
)
```

Now, let's edit your main.swift file to contain:

```swift
import SwiftKuery
import SwiftKueryPostgreSQL
import Kitura
import Foundation

import HeliumLogger

HeliumLogger.use()

let router = Router()

class Grades : Table {
  let tableName = "grades"
  let key = Column("key")
  let course = Column("course")
  let grade = Column("grade")
  let studentId = Column("studentId")
}

let grades = Grades()

let connection = PostgreSQLConnection(host: "localhost", port: 5432, options: [.databaseName("school")])

func grades(_ callback:@escaping (String)->Void) -> Void {
  connection.connect() { error in
    if let error = error {
      callback("Error is \(error)")
      return
    }
    else {
      // Build and execute your query here.

      // First build query
      let query = Select(grades.course, grades.grade, from: grades)

      connection.execute(query: query) { result in
        if let resultSet = result.asResultSet {
          var retString = ""

          for title in resultSet.titles {
            // The column names of the result.
            retString.append("\(title.padding(toLength: 35, withPad: " ", startingAt: 0)))")
          }
          retString.append("\n")

          for row in resultSet.rows {
            for value in row {
              if let value = value as? String {
                retString.append("\(value.padding(toLength: 35, withPad: " ", startingAt: 0))")
              }
            }
            retString.append("\n")
          }
          callback(retString)
        }
        else if let queryError = result.asError {
          // Something went wrong.
          callback("Something went wrong \(queryError)")
        }
      }
    }
  }
}

router.get("/") {
  request, response, next in

  grades() {
    resp in
    response.send(resp)
    next()
  }
}

// Use port 8090 unless overridden by environment variable
let port = Int(ProcessInfo.processInfo.environment["PORT"] ?? "8090") ?? 8090

Kitura.addHTTPServer(onPort: port, with: router)
Kitura.run()
```

Now build the program and run it:

```
$ swift build
$ .build/debug/swift-kuery-play
```

Now open a web page to <a href="http://localhost:8090">http://localhost:8090</a> and you should see:

```
course                             grade                              
How to build your first computer   99                                 
How to work at a rock quarry       71      
```

Now we can change our query line and see different results.

Change the line:

```swift
      let query = Select(grades.course, grades.grade, from: grades)
```

to

```swift
      let query = Select(grades.course, grades.grade, from: grades)
        .where(grades.grade > 80)
```

and we should only see grades greater than 80:

```
course                             grade                              
How to build your first computer   99                                 
```



## License
This library is licensed under Apache 2.0. Full license text is available in [LICENSE](LICENSE.txt).
