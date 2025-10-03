# SQL Database Server

An SQL database server, including an SQL tokeniser and parser, error handling and socket connections implemented in Java using Maven framework.


##### Demo: Select
<p align="center">
<img src="https://github.com/drpollylang/DatabaseServer/blob/main/assets/demos/db_select_conditions.gif" alt="" style="height:75vh;">
</p>


# Usage

## Opening a connection to the server

First, get the server running on port 8888. In a terminal, move into the cw-db directory, then run the following:

```
./mvnw exec:java@server
```

Open a new terminal (*do not close the first one!*) and run the client by typing the following onto the command line:

```
./mvnw exec:java@client
```

You will see a prompt starting with `SQL:>`. Type your commands at this prompt.

##### Demo: Create Database and Tables

<p align="center">
<img src="https://github.com/drpollylang/DatabaseServer/blob/main/assets/demos/db_create_database_and_tables.gif" alt="" style="height:75vh;">
</p>


## SQL Parser and Supported Commands

The Database Server includes a SQL parser based on the `./assets/docs/BNF.txt` grammar. The following SQL commands are supported using the syntax contained within this grammar: 
- `USE <Database Name>`
- `CREATE` - create new database/table
    - `CREATE DATABASE <Database Name>`
    - `CREATE TABLE <Table Name> (<Attribute List>)`
- `DROP` - delete database/table
    - `DROP DATABASE <Database Name>`
    - `DROP TABLE <Table Name>`
- `ALTER` - add or remove columns from table
    - `ALTER TABLE <TableName> ADD <AttributeName>`
    - `ALTER TABLE <TableName> DROP <AttributeName>`
- `INSERT` - add a new row of values to a table
    - `INSERT INTO <TableName> VALUES (<ValueList>)`
- `SELECT` - select data from table - select by column name and/or apply conditions to output
    - `"SELECT <*|AttributeList> FROM <TableName>` 
    - `SELECT <*|AttributeList> FROM <TableName> WHERE <Condition>`
- `UPDATE`- change data in a table
    - `UPDATE <TableName> SET <NameValueList> WHERE <Condition>`   
- `DELETE` - delete data from a table
    - `DELETE FROM <TableName> WHERE <Condition>`             
- `JOIN` - join two tables together by matching on a column (attribute) from each table
    - `JOIN <TableName> AND <TableName> ON <AttributeName> AND <AttributeName>`


##### Demo: Alter Table

<p align="center">
<img src="https://github.com/drpollylang/DatabaseServer/blob/main/assets/demos/db_alter_table.gif" alt="" style="height:75vh;">
</p>

##### Demo: Error Handling

<p align="center">
<img src="https://github.com/drpollylang/DatabaseServer/blob/main/assets/demos/db_error_handling.gif" alt="" style="height:75vh;">
</p>

##### ##### Demo: Drop Table and Database

<p align="center">
<img src="https://github.com/drpollylang/DatabaseServer/blob/main/assets/demos/db_drop_database_and_table.gif" alt="" style="height:75vh;">
</p>

## Persistent Storage

When you create a new database, a folder with the same name as the new database will be created in the `cw-db/databases` folder. If there is no folder called `databases`, a new one will be created. Within the database directory, files with the names of the database tables will be created to act as persistent storage for the database tables. This means that when the connection to the server is closed, the data stored in the database tables will persist, and can be accessed during a different session. 

