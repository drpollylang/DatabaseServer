# DB Assignment - Notes

Goal: to build a database server. 

What it must **do**:
- Maintain persistent data as a number of files on the filesystem
- Receive incoming requests (in SQL)
- Interrogate and manipulate a set of stored records
- Return the results to the user

The server should **be**:
- Robust - detects and traps errors and continues running at all time (handles errors when they occur and does not crash)
- Code quality - conforms to coding standards and specifications covred in lectures

## Persistant storage

- Server searches for a folder called `databases`. If it does not find one, creates one
- Within `databases` are directories with the names of the databases.
- Within the database directories are files corresponding to its tables
- Within the table files, the first row (0) are the column headings. The rows correspond to (tab-separated) records.
- The first (0th) column in each table contains a unique numerical identifier (primary key) which is always called `id`
- Database names and table names are case-insensitive - converted to lowercase before saving out
- Column names are case insensitive for *querying*, but case is preserved when storing

`id` must be unique to each record, and never repeated, even if the record is deleted. Simple incrementor would work, but the current value of the incrementor must be preserved even if the program is shut down and then restarted again. Stored in a separate data file within the database directory? Can be called metadata - first row is `current_id` tab-separated from its value. When the database starts up, it reads from this file and stores the value in a global variable. Before it shuts down, it writes the value back into the `metadata` file. 

## Data Structures

- Once data has been read in from filesystem, must be stored in memory using classes
- Tabular nature of the data - each table should be represented by a hash table object - extend the class with additional methods as required?
  - Hash table where the keys are Strings (column names) and the values are ArrayLists (the values of the records for that column)
  - Any missing values are `null` in the ArrayList
  - For a given record, the values of the columns are the values of the arrayLists at a given index. 



## Communicating with User

Run server from command line using: `mvnw exec:java@server`

Incoming commands are passed to `handleCommand` method for processing - need to add to the method to respond to commands

Note! Response must be **returned** by `handleCommand` and NOT just printed to console!

Command line client (for manually checking that server is operating correctly) can be run using: `mvnw exec:java@client`

BUT client should not replace automated test scripts!

### What do we need the server to do?

- Read in data corresponding to a particular table in a particular database

// TODO
- Add to `handleCommand` so that it handles requests from user and returns a response that is printed to the console in some format
  - [OK] for valid queries followed by results of query
  - [ERROR] if query is invalid or something goes wrong, followed by a human-readble message providing info about the results of the query.
 

## Query Language

- Write a handler for incoming messages that:
  - Parse incoming command
  - Perform the specified query
  - Update data stored in database (save to filesystem)
  - Return an appropraite response to client

Server supports the following queries:
- USE: switches the database against which the following queries will be run
- CREATE: constructs a new database or table (depending on the provided parameters)
- INSERT: adds a new record (row) to an existing table
- SELECT: searches for records that match the given condition
- UPDATE: changes the existing data contained within the rows of a table
- ALTER: changes the structure (columns) of an existing table
- DELETE: removes records that match the given condition from an existing table
- DROP: removes a specified table from a database, or removes the entire database
- JOIN: performs an inner join on two tables (returning all permutations of all matching records)

What does the server actually need to be able to do?

Modifying filesystem:
- Change the database from which it is reading
- Create a new database
- Create a new table within a database
- Removing a specified table from a database
- Removing the entire database

Reading/writing data between memory and filesystem:
- Read in data from a database table (filesystem)
- Write data to a database table (filesystem)

Modifying tables:
- Add a new record to a database table
- Changing existing data contained within rows of a table
- Changing the structure (columns) of a table
- Removing records that match a given condition from an existing table

Searching tables:
- Searching table for a record that matches a given condition
- Performing an *inner join* on two tables, returning all permutations of matching records.

## Parsing SQL requests

Client inputs a query -> passed to `handleCommand` -> passed to BasicTokeniser -> passed to QueryParser -> passed to QueryHandler -> instantiates a Table class -> Table retrieves specified data -> performs manipulations -> passes any requested data to `handleCommand` -> ...which returns it along with the status response -> passes back to DBServer -> prints response to console. 

### Tokeniser

Given as Basic Tokeniser

### Parser



# TODOs

## Done
- Read/write functionality - can read data from filesystem tab files into memory, store as a hash table of ArrayLists, retrieve and modify data, and write back to filesystem. Order of column names is currently not preserved
  - Automated testing of db IO written
  - Error handling of IO - trying to read/write to files that fdon't exist/don't have permissions for, malformed tab files, missing data. Incorporated into test scripts. 
- Implemented a DBTable Class - stores tables as Hashtables of ArrayLists - where each key in HashTable is a column name, and each value is an ArrayList storing the value of that column for each row. Empty cells are represented by `null` values.
  - Methods to retrieve data as rows, columnNames, values
- Started to implement QueryHandler class that handles queries - interrogates DBTable and returns reponse
  - Implemented filesystem manipulations - create/drop database/table.
  - Automated testing of create/drop database/table functionality written.
  - Error handling for filesystem manipulation - ie trying to create an already existing db/table, trying to delete a database/table that doesnt exist, etc.
- Started implementing table metadata - each database contains a metadata.txt file which is created when the database is created. This contains one value (currently): a number representing the next available `id` primary key. In DBTable, this is returned as a static class property representing the next `id` to be assigned. This is to ensure that `id`s are unique across a database - even when the database is closed and restarted.    

## Done 28 Feb 25

- Implemented Tokeniser (based on BasicTokeniser provided by lecturers)
- Implemented Parser
- Added test scripts for the Tokeniser and Parser
- Implemented custom DBExceptions and added to Parser


## TODO Next:
- QueryHandler - based on information from QueryParser, instantiates a Table, performs operations, and returns the results to the DBServer/Client.
- Control flow between Client/Server, Tokeniser, Parser, Handler.
- Error handling - robust - test scripts



### Outcomes (by FRIDAY 7 March!)
- All basic functionality implemented
- Server able to take in queries, parse them, perform data operations and return result to user
- Error handling - robust to errors. Test scripts written to try and break system 


## Other TODO
- Implement idCount in DBTable - increment whenever a record is added to the table. Write the new value of idCount to metadata.txt whenever data is written out to the filesystem



# TODO Next

- Create and instantiate a Condition class - use to write methods in DBTable for getting the index of a row/column by condition.
- Implement and test the manipulations (read/write also)
- DBServer to contain a hashmap of Tables (keys = tablenames, values = Tables) - read in initially. Write whenever updated.
- Refactor command types into different classes that are extensions of an abstract DBCommand
- Refactor handler into methods within the command classes (which are passed the DBServer to access data)

