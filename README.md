<p align="center">
    <a href="http://kitura.io/">
        <img src="https://raw.githubusercontent.com/IBM-Swift/Kitura/master/Sources/Kitura/resources/kitura-bird.svg?sanitize=true" height="100" alt="Kitura">
    </a>
</p>


<p align="center">
    <a href="https://ibm-swift.github.io/Swift-Kuery-PostgreSQL/index.html">
    <img src="https://img.shields.io/badge/apidoc-SwiftKueryPostgreSQL-1FBCE4.svg?style=flat" alt="APIDoc">
    </a>
    <a href="https://travis-ci.org/IBM-Swift/Swift-Kuery-PostgreSQL">
    <img src="https://travis-ci.org/IBM-Swift/Swift-Kuery-PostgreSQL.svg?branch=master" alt="Build Status - Master">
    </a>
    <img src="https://img.shields.io/badge/os-macOS-green.svg?style=flat" alt="macOS">
    <img src="https://img.shields.io/badge/os-linux-green.svg?style=flat" alt="Linux">
    <img src="https://img.shields.io/badge/license-Apache2-blue.svg?style=flat" alt="Apache 2">
    <a href="http://swift-at-ibm-slack.mybluemix.net/">
    <img src="http://swift-at-ibm-slack.mybluemix.net/badge.svg" alt="Slack Status">
    </a>
</p>

# Swift-Kuery-PostgreSQL

[PostgreSQL](https://www.postgresql.org/) plugin for the [Swift-Kuery](https://github.com/IBM-Swift/Swift-Kuery) framework. It enables you to use Swift-Kuery to manipulate data in a PostgreSQL database.

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

## Usage

#### Add dependencies

Add the `SwiftKueryPostgreSQL` package to the dependencies within your application’s `Package.swift` file. Substitute `"x.x.x"` with the latest `SwiftKueryPostgreSQL` [release](https://github.com/IBM-Swift/Swift-Kuery-PostgreSQL/releases).

```swift
.package(url: "https://github.com/IBM-Swift/Swift-Kuery-PostgreSQL.git", from: "x.x.x")
```

Add `SwiftKueryPostgreSQL` to your target's dependencies:

```swift
.target(name: "example", dependencies: ["SwiftKueryPostgreSQL"]),
```

#### Import package

  ```swift
  import SwiftKueryPostgreSQL
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

For more details refer to the [PostgreSQL manual](https://www.postgresql.org/docs/8.0/static/libpq.html#LIBPQ-CONNECT).

<br>

Alternatively, call:

```swift
let connection = PostgreSQLConnection(url: URL(string: "Postgres://\(username):\(password)@\(host):\(port)/\(databaseName)")!)
```

To establish a connection to the database call:

```swift
connection.connect() { result in
    guard result.success else {
        // Connection not established, handle error
        return
    }
    // Connection established
}
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
Now, let's create the tables we need for this example.

Use the interative `psql` client to open the database we created:

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

Next, create the grades table:

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
Now we are set to connect to our database from Swift and use Swift-Kuery to query the data into our Swift application.

#### Create simple Swift executable
First create a directory for our project and then initialize it.

```
$ mkdir swift-kuery-play
$ cd swift-kuery-play
$ swift package init --type executable
Creating executable package: swift-kuery-play
Creating Package.swift
Creating README.md
Creating .gitignore
Creating Sources/
Creating Sources/swift-kuery-play/main.swift
Creating Tests/
$
```

Now, add Swift-Kuery-PostgreSQL as a dependency for our project, this will automatically pull in Swift-Kuery.
Edit `Package.swift` to contain the following, substituting `"x.x.x"` with the latest `Kitura` and `Swift-Kuery-PostgreSQL` releases.

```swift
// swift-tools-version:4.0
import PackageDescription

let package = Package(
    name: "swift-kuery-play",
    dependencies: [
        .package(url: "https://github.com/IBM-Swift/Kitura.git", from: "x.x.x"),
        .package(url: "https://github.com/IBM-Swift/Swift-Kuery-PostgreSQL", from: "x.x.x")
    ],
    targets: [
        .target(
            name: "swift-kuery-play",
            dependencies: ["Kitura", "SwiftKueryPostgreSQL"]),
    ]
)
```

Now, edit your `main.swift` file to contain:

```swift
import SwiftKuery
import SwiftKueryPostgreSQL
import Kitura

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

func grades(_ callback: @escaping (String) -> Void) -> Void {
    connection.connect() { result in
        guard result.success else {
            guard let error = result.asError else {
                return callback("Error connecting: Unknown Error")
            }
            return callback("Error connecting: \(error)")
        }
        // Build and execute your query here.

        // First build query
        let query = Select(grades.course, grades.grade, from: grades)

        // Execute query
        connection.execute(query: query) { result in
            guard let resultSet = result.asResultSet else {
                guard let error = result.asError else {
                    return callback("Error executing query: Unknown Error")
                }
                return callback("Error executing query: \(error)")
            }
            var retString = ""
            resultSet.getColumnTitles() { titles, error in
                guard let titles = titles else {
                    guard let error = error else {
                        return callback("Error fetching column titles: Unknown Error")
                    }
                    return callback("Error fetching column titles: \(error)")
                }
                for title in titles {
                    //The column names of the result.
                    retString.append("\(title.padding(toLength: 35, withPad: " ", startingAt: 0))")
                }
                retString.append("\n")

                resultSet.forEach() { row, error in
                    guard let row = row else {
                        // A null row means we have run out of results unless we encountered an error
                        if let error = error {
                            return callback("Error fetching row: \(error)")
                        }
                        // No error so all rows are processed, make final callback passing result.
                        return callback(retString)
                    }
                    for value in row {
                        if let value = value {
                            let valueString = String(describing: value)
                            retString.append("\(valueString.padding(toLength: 35, withPad: " ", startingAt: 0))")
                        }
                    }
                    retString.append("\n")
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

Kitura.addHTTPServer(onPort: 8080, with: router)
Kitura.run()
```

Now build the program and run it:

```
$ swift build
$ .build/debug/swift-kuery-play
```

Now open a web page to <a href="http://localhost:8080">http://localhost:8080</a> and you should see:

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

Another possibility is to use `QueryResult.asRows` that returns the result as an array of dictionaries where each dictionary represents a row of the result with the column title as the key.       
 Change your `grades` function as follows:

```swift
func grades(_ callback: @escaping (String) -> Void) -> Void {
    connection.connect() { result in
        guard result.success else {
            guard let error = result.asError else {
                return callback("Error connecting: Unknown Error")
            }
            return callback("Error connecting: \(error)")
        }
        let query = Select(grades.course, grades.grade, from: grades)
        connection.execute(query: query) { result in
            result.asRows() { rows, error in
                guard let rows = rows else {
                    guard let error = error else {
                        return callback("Error getting rows: Unknown Error")
                    }
                    return callback("Error getting rows: \(error)")
                }
                var retString = ""
                for row in rows {
                    for (title, value) in row {
                        if let value = value {
                            retString.append("\(title): \(value) ")
                        }
                    }
                    retString.append("\n")
                }
                return callback(retString)
            }
        }
    }
}
```
At <a href="http://localhost:8080">http://localhost:8080</a> you should see:

```
grade: 99 course: How to build your first computer
grade: 71 course: How to work at a rock quarry  
```
## API Documentation
For more information visit our [API reference](https://ibm-swift.github.io/SwiftKueryPostgreSQL/index.html).

## Community

We love to talk server-side Swift, and Kitura. Join our [Slack](http://swift-at-ibm-slack.mybluemix.net/) to meet the team!

## License
This library is licensed under Apache 2.0. Full license text is available in [LICENSE](https://github.com/IBM-Swift/SwiftKueryPostgreSQL/blob/master/LICENSE.txt)
