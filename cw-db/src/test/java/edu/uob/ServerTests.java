package edu.uob;

import java.io.*;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;

public class ServerTests {

    private DBServer server;

    // Create a new server _before_ every @Test
    @BeforeEach
    public void setup() {
        server = new DBServer();
    }

    private String sendCommandToServer(String command) {
        // Try to send a command to the server - this call will timeout if it takes too long (in case the server enters an infinite loop)
        return server.handleCommand(command);
    }


    @Test
    public void handleUseTest() {
        sendCommandToServer("CREATE DATABASE newdb;");
        String query = "USE newdb;";
        assertEquals("[OK]", sendCommandToServer(query));

        // Trying to access a database that doesn't exist
        assertTrue(sendCommandToServer("USE aNonExistentDatabase;").contains("[ERROR]"));
    }

    @Test
    public void handleCreateDropTest() {
        // Valid query
        sendCommandToServer("DROP DATABASE newdb;");
        String response;
        assertEquals(sendCommandToServer("CREATE DATABASE newdb;"), "[OK]");
        assertEquals(sendCommandToServer("USE newdb;"), "[OK]");
        sendCommandToServer("DROP TABLE animals;");
        assertEquals(sendCommandToServer("CREATE TABLE animals;"), "[OK]");
        assertEquals(sendCommandToServer("DROP TABLE animals;"), "[OK]");
        assertEquals(sendCommandToServer("CREATE TABLE animals (type, colour);"), "[OK]");

        // Valid query
        sendCommandToServer("DROP DATABASE anotherdb;");
        assertEquals(sendCommandToServer("CREATE DATABASE anotherdb;"), "[OK]");
        assertEquals(sendCommandToServer("USE anotherdb;"), "[OK]");
        assertEquals(sendCommandToServer("CREATE TABLE table1 (col1, col2);"), "[OK]");
        assertEquals(sendCommandToServer("DROP DATABASE anotherdb;"), "[OK]");

        // Test case-insensitivity - any database/table names input by user should be converted
        // into lower case before saving out to filesystem.
        sendCommandToServer("DROP DATABASE atestdatabase;");
        assertEquals(sendCommandToServer("CREATE DATABASE aTestDatabase;"), "[OK]");
        assertEquals(sendCommandToServer("USE atestdatabase;"), "[OK]");
        assertEquals(sendCommandToServer("CREATE TABLE aTestTABLE;"), "[OK]");
        assertTrue(new File("./databases/atestdatabase/atesttable.tab").exists());
        assertEquals(sendCommandToServer("DROP DATABASE ATESTDATABASE;"), "[OK]");

        // Test that CREATE TABLE can gracefully handle 'id' being given as one of the table columns
        sendCommandToServer("USE newdb;");
        assertEquals("[OK]", sendCommandToServer("CREATE TABLE flowers (id, type, colour);"));
        response = sendCommandToServer("SELECT * FROM flowers;");
        assertTrue(response.contains("[OK]"), "Creating table and passing 'id' as a column name failed to return [OK]");
        assertTrue(response.contains("id") && response.contains("type") && response.contains("colour"),
                "Creating table and passing 'id' as a column name failed to create a new table with the columns" +
                        "id, type and colour.");

        // Test that attempting to create databases/tables with invalid names returns [ERROR
        response = sendCommandToServer("CREATE DATABASE Another_Database;");
        assertTrue(response.contains("[ERROR]"), "An invalid query was made (attempting to create a database with an invalid name), however an [ERROR] tag was not returned");
        assertFalse(response.contains("[OK]"), "An invalid query was made (attempting to create a database with an invalid name), however an [OK] tag was returned");
        assertTrue(response.contains("Malformed query. The identifier another_database is invalid"), "Error message informing user that the database cannot be created because the identifier is invalid was not returned.");
        

        response = sendCommandToServer("CREATE TABLE _AnInvalid?Name;");
        assertTrue(response.contains("[ERROR]"), "An invalid query was made (attempting to create a table with an invalid name), however an [ERROR] tag was not returned");
        assertFalse(response.contains("[OK]"), "An invalid query was made (attempting to create a table with an invalid name), however an [OK] tag was returned");
        assertTrue(response.contains("Malformed query. The identifier _aninvalid?name is invalid"), "Error message informing user that the table cannot be created because the identifier is invalid was not returned.");
        

        // trying to create a database that already exists - [ERROR]
        assertEquals(sendCommandToServer("CREATE DATABASE anotherdb;"), "[OK]");
        response = sendCommandToServer("CREATE DATABASE anotherdb;");
        assertTrue(response.contains("[ERROR]"));
        assertTrue(response.contains("Sorry, this database already exists"));
        

        // trying to drop a database that doesn't exist - [ERROR]
        assertEquals(sendCommandToServer("DROP DATABASE anotherdb;"), "[OK]");
        response = sendCommandToServer("DROP DATABASE anotherdb;");
        assertTrue(response.contains("[ERROR]"));
        assertTrue(response.contains("does not exist"));
        

        // trying to drop a table that doesn't exist - [ERROR]
        assertEquals(sendCommandToServer("CREATE DATABASE anotherdb;"), "[OK]");
        assertEquals(sendCommandToServer("USE anotherdb;"), "[OK]");
        response = sendCommandToServer("DROP TABLE nonExistentTable;");
        assertTrue(response.contains("[ERROR]"));
        assertTrue(response.contains("does not exist"));
        

        // trying to create a table that already exists - [ERROR]
        assertEquals(sendCommandToServer("CREATE TABLE table1;"), "[OK]");
        response = sendCommandToServer("CREATE TABLE table1 (col1, col2);");
        assertTrue(response.contains("[ERROR]"));
        assertTrue(response.contains("already exists"));
        

        // Malformed queries handled - missing semicolon, mispelt/wrong-order keywords - [ERROR]
        response = sendCommandToServer("CREATE TABEL aNewTable;");
        assertTrue(response.contains("[ERROR]"));
        assertTrue(response.contains("Query is malformed."));

        response = sendCommandToServer("CREATE NEW TABLE aNewTable;");
        assertTrue(response.contains("[ERROR]"));
        assertTrue(response.contains("Query is malformed."));

        response = sendCommandToServer("CREATE TABLE aNewTable col1, col2;");
        assertTrue(response.contains("[ERROR]"));
        assertTrue(response.contains("Query is malformed."));
    }

    @Test
    public void handleAlterInsertTest() {
        sendCommandToServer("DROP DATABASE anotherdb;");
        assertEquals(sendCommandToServer("CREATE DATABASE anotherdb;"), "[OK]");
        assertEquals(sendCommandToServer("USE anotherdb;"), "[OK]");
        assertEquals(sendCommandToServer("CREATE TABLE table1 (col1, col2);"), "[OK]");
        assertEquals(sendCommandToServer("INSERT INTO table1 VALUES (1, 2);"), "[OK]");
        assertEquals(sendCommandToServer("ALTER TABLE table1 ADD col3;"), "[OK]");
        assertEquals(sendCommandToServer("INSERT INTO table1 VALUES (3, 4, 5);"), "[OK]");
        assertEquals(sendCommandToServer("ALTER TABLE table1 DROP col3;"), "[OK]");

        // Test the case-insensitvity of queries and case preservation of column names in Insert
        assertEquals(sendCommandToServer("ALTER TABLE table1 ADD AFourthColumn;"), "[OK]");
        String response = sendCommandToServer("SELECT afourthcolumn FROM table1;");
        assertTrue(response.contains("[OK]"), "A valid query was made, however an [OK] tag was not returned");
        assertFalse(response.contains("[ERROR]"), "A valid query was made, however an [ERROR] tag was returned");
        assertTrue(response.contains("AFourthColumn"), "Case-insensitivity of select querying/case preservation of column names in response failed.");
        

        // Trying to alter a table that doesn't exist returns [ERROR]
        response = sendCommandToServer("ALTER TABLE ANonExistentTable ADD col1;");
        assertTrue(response.contains("[ERROR]"), "An invalid query was made (attempting to alter a table that doesn't exist), however an [ERROR] tag was not returned");
        assertFalse(response.contains("[OK]"), "An invalid query was made (attempting to alter a table that doesn't exist), however an [OK] tag was returned");
        assertTrue(response.contains("this table does not exist"), "Error message informing user that the selected table does not exist was not returned.");
        

        // Trying to drop columns that don't exist/are misspelt returns [ERROR]
        response = sendCommandToServer("ALTER TABLE table1 DROP col3;");
        assertTrue(response.contains("[ERROR]"), "An invalid query was made (attempting to drop a column that doesn't exist), however an [ERROR] tag was not returned");
        assertFalse(response.contains("[OK]"), "An invalid query was made (attempting to drop a column that doesn't exist), however an [OK] tag was returned");
        assertTrue(response.contains("This column does not exist in this table."), "Error message informing user that the column cannot be dropped because it does not exist was not returned.");
        

        // Trying to add columns that already exist returns [ERROR]
        response = sendCommandToServer("ALTER TABLE table1 ADD col2;");
        assertTrue(response.contains("[ERROR]"), "An invalid query was made (attempting to add a column that already exists), however an [ERROR] tag was not returned");
        assertFalse(response.contains("[OK]"), "An invalid query was made (attempting to add a column that already exists), however an [OK] tag was returned");
        assertTrue(response.contains("Column col2 already exists in this table."), "Error message informing user that the column cannot be added because it already exists was not returned.");
        

        // Trying to add a column with an invalid name (i.e. contains chars other than letters and digits) returns [ERROR]
        response = sendCommandToServer("ALTER TABLE table1 ADD 123col;");
        assertFalse(response.contains("[ERROR]"), "A valid query was made, however an [ERROR] tag was returned");
        assertTrue(response.contains("[OK]"), "A valid query was made, however an [OK] tag was not returned");
        

        response = sendCommandToServer("ALTER TABLE table1 ADD _col;");
        assertTrue(response.contains("[ERROR]"), "An invalid query was made (attempting to add a column with an invalid name), however an [ERROR] tag was not returned");
        assertFalse(response.contains("[OK]"), "An invalid query was made (attempting to add a column with an invalid name), however an [OK] tag was returned");
        assertTrue(response.contains("Malformed query. The identifier _col is invalid."), "Error message informing user that the column cannot be added because the identifier is invalid was not returned.");
        

        response = sendCommandToServer("ALTER TABLE table1 ADD c0l!;");
        assertTrue(response.contains("[ERROR]"), "An invalid query was made (attempting to add a column with an invalid name), however an [ERROR] tag was not returned");
        assertFalse(response.contains("[OK]"), "An invalid query was made (attempting to add a column with an invalid name), however an [OK] tag was returned");
        assertTrue(response.contains("Malformed query. The identifier c0l! is invalid."), "Error message informing user that the column cannot be added because identifier is invalid was not returned.");
        

        // Trying to alter the id column returns [ERROR]
        response = sendCommandToServer("ALTER TABLE table1 DROP id;");
        assertTrue(response.contains("[ERROR]"), "An invalid query was made (attempting to drop the id column), however an [ERROR] tag was not returned");
        assertFalse(response.contains("[OK]"), "An invalid query was made (attempting to drop the id column), however an [OK] tag was returned");
        assertTrue(response.contains("Manually changing the 'id' column is forbidden."), "Error message informing user that dropping the id column is forbidden.");
        

        sendCommandToServer("CREATE TABLE table2;");
        response = sendCommandToServer("ALTER TABLE table2 ADD id;");
        assertTrue(response.contains("[ERROR]"), "An invalid query was made (attempting to add the id column), however an [ERROR] tag was not returned");
        assertFalse(response.contains("[OK]"), "An invalid query was made (attempting to add the id column), however an [OK] tag was returned");
        assertTrue(response.contains("Manually changing the 'id' column is forbidden."), "Error message informing user that adding the id column is forbidden.");
        

        // Trying to insert too many/too few values into the table returns [ERROR]
        response = sendCommandToServer("INSERT INTO table1 VALUES (6);");
        assertTrue(response.contains("[ERROR]"), "An invalid query was made (attempting to add a column that already exists), however an [ERROR] tag was not returned");
        assertFalse(response.contains("[OK]"), "An invalid query was made (attempting to add a column that already exists), however an [OK] tag was returned");
        assertTrue(response.contains("number of columns in the table does not match the number of values to be entered into the table."), "Error message informing user that the column cannot be added because it already exists was not returned.");
        
        
        // Malformed query - missing/misspelt keywords, wrong order returns [ERROR]
        response = sendCommandToServer("ALTR TABLE table1 ADD newcol;");
        assertTrue(response.contains("[ERROR]"));
        assertTrue(response.contains("The keyword ALTR is invalid"));

        response = sendCommandToServer("ALTER table1 ADD newcol;;");
        assertTrue(response.contains("[ERROR]"));
        assertTrue(response.contains("Query is malformed."));

        response = sendCommandToServer("ALTER TABLE table1 INSERT newcol;");
        assertTrue(response.contains("[ERROR]"));
        assertTrue(response.contains("Malformed query. The third word of an ALTER query should be either ADD or DROP"));
    }

    @Test
    public void handleSelectTest() {
        sendCommandToServer("DROP DATABASE newdb;");
        sendCommandToServer("CREATE DATABASE newdb;");
        assertEquals(sendCommandToServer("USE newdb;"), "[OK]");
        sendCommandToServer("DROP TABLE animals;");
        assertEquals(sendCommandToServer("CREATE TABLE animals (type, colour, number);"), "[OK]");
        assertEquals(sendCommandToServer("INSERT INTO animals VALUES (lion, gold, 1);"), "[OK]");
        assertEquals(sendCommandToServer("INSERT INTO animals VALUES (dolphin, grey, 3);"), "[OK]");
        assertEquals(sendCommandToServer("INSERT INTO animals VALUES (sheep, white, 22);"), "[OK]");
        assertEquals(sendCommandToServer("INSERT INTO animals VALUES (cat, black, 13);"), "[OK]");
        assertEquals(sendCommandToServer("INSERT INTO animals VALUES (spider, black, 100);"), "[OK]");
        assertEquals(sendCommandToServer("INSERT INTO animals VALUES (pony, white, 6);"), "[OK]");

        String response = sendCommandToServer("SELECT * FROM animals;");
        
        assertTrue(response.contains("[OK]"), "A valid query was made, however an [OK] tag was not returned");
        assertFalse(response.contains("[ERROR]"), "A valid query was made, however an [ERROR] tag was returned");
        assertTrue(response.contains("lion"), "An attempt was made to add lion to the table, but they were not returned by SELECT *");
        assertTrue(response.contains("pony"), "An attempt was made to add pony to the table, but they were not returned by SELECT *");

        

        response = sendCommandToServer("SELECT animals, colour FROM animals;");
        
        assertTrue(response.contains("[OK]"), "A valid query was made, however an [OK] tag was not returned");
        assertFalse(response.contains("[ERROR]"), "A valid query was made, however an [ERROR] tag was returned");
        assertTrue(response.contains("white"), "SELECT statement did not return the colour column");
        assertFalse(response.contains("pony"), "SELECT statement returned the type column when it was supposed to only return the colour column");

        response = sendCommandToServer("SELECT * FROM animals WHERE colour == white;");
        
        assertTrue(response.contains("[OK]"), "A valid query was made, however an [OK] tag was not returned");
        assertFalse(response.contains("[ERROR]"), "A valid query was made, however an [ERROR] tag was returned");
        assertTrue(response.contains("sheep"), "Filtering table by colour == white failed to return the sheep record");
        assertFalse(response.contains("lion"), "Filtering table by colour == white erroneously returned the lion record");

        response = sendCommandToServer("SELECT * FROM animals WHERE colour == white OR number > 20;");
        
        assertTrue(response.contains("[OK]"), "A valid query was made, however an [OK] tag was not returned");
        assertFalse(response.contains("[ERROR]"), "A valid query was made, however an [ERROR] tag was returned");
        assertTrue(response.contains("sheep"), "Filtering table by colour == white failed to return the sheep record");
        assertTrue(response.contains("spider"), "Filtering table by colour == white OR number > 20 failed to return the spider record");
        assertFalse(response.contains("lion"), "Filtering table by colour == white OR number > 20 erroneously returned the lion record");

        response = sendCommandToServer("SELECT * FROM animals WHERE colour == white AND number > 20;");
        
        assertTrue(response.contains("[OK]"), "A valid query was made, however an [OK] tag was not returned");
        assertFalse(response.contains("[ERROR]"), "A valid query was made, however an [ERROR] tag was returned");
        assertTrue(response.contains("sheep"), "Filtering table by colour == white failed to return the sheep record");
        assertFalse(response.contains("spider"), "Filtering table by colour == white AND number > 20 erroneously returned the spider record");
        assertFalse(response.contains("pony"), "Filtering table by colour == white AND number > 20 erroneously returned the pony record");

        // Test like - regex pattern matching
        // Where animal type starts with 's'
        response = sendCommandToServer("SELECT * FROM animals WHERE type LIKE '^s.*';");
        
        assertTrue(response.contains("[OK]"), "A valid query was made, however an [OK] tag was not returned");
        assertTrue(response.contains("sheep"), "Pattern matching search for animal type starting with 's' failed to return sheep");
        assertTrue(response.contains("spider"), "Pattern matching search for animal type starting with 's' failed to return spider");
        assertFalse(response.contains("pony"), "Pattern matching search for animal type starting with 's' returned pony");

        // Contains 'l'
        response = sendCommandToServer("SELECT * FROM animals WHERE type LIKE 'l';");
        assertTrue(response.contains("[OK]"), "A valid query was made, however an [OK] tag was not returned");
        assertTrue(response.contains("lion"), "Pattern matching search for animal type containing 'l' failed to return lion");
        assertTrue(response.contains("dolphin"), "Pattern matching search for animal type containing 'l' failed to return dolphin");
        assertFalse(response.contains("sheep"), "Pattern matching search for animal type containing 'l' returned sheep");

        // If no matches found, just return an empty table
        response = sendCommandToServer("SELECT * FROM animals WHERE type LIKE 'abc';");
        assertTrue(response.contains("[OK]"), "A valid query was made, however an [OK] tag was not returned");
        assertTrue(response.contains("id") && response.contains("type") && response.contains("colour") && response.contains("number"),
                "Pattern matching for a pattern that does not exist in table failed to return empty table with column headers intact.");
        assertFalse(response.contains("lion") || response.contains("dolphin") || response.contains("sheep") || response.contains("cat"),
                "Pattern matching for a pattern that does not exist in table failed to return an empty table without data.");
        

        // handles SELECT with brackets and no brackets e.g. SELECT * FROM animals WHERE (type == 'lion') OR (type == 'dolphin');
        // should be equivalent to SELECT * FROM animals WHERE type == 'lion' OR type == 'dolphin';
        String responseWithBrackets = sendCommandToServer("SELECT * FROM animals WHERE (type == 'lion') OR (type == 'dolphin');");
        String responseNoBrackets = sendCommandToServer("SELECT * FROM animals WHERE type == 'lion' OR type == 'dolphin';");
        assertEquals(responseNoBrackets, responseWithBrackets, "Response from SELECT statement with brackets around the conditions was not the same as the response from the same statement without brackets");

        // Testing case in-sensitivity/preservation of case in column names
        assertEquals(sendCommandToServer("USE newdb;"), "[OK]");
        sendCommandToServer("DROP TABLE lotr;");
        assertEquals(sendCommandToServer("CREATE TABLE lotr (Name, Race, Alive);"), "[OK]");
        assertEquals(sendCommandToServer("INSERT INTO lotr VALUES (Aragorn, Man, TRUE);"), "[OK]");
        assertEquals(sendCommandToServer("INSERT INTO lotr VALUES (Gimli, Dwarf, TRUE);"), "[OK]");
        assertEquals(sendCommandToServer("INSERT INTO lotr VALUES (Legolas, Elf, TRUE);"), "[OK]");
        assertEquals(sendCommandToServer("INSERT INTO lotr VALUES (Boromir, Man, FALSE);"), "[OK]");
        assertEquals(sendCommandToServer("INSERT INTO lotr VALUES (Frodo, Hobbit, TRUE);"), "[OK]");
        assertEquals(sendCommandToServer("INSERT INTO lotr VALUES (Gandalf, Wizard, KINDA);"), "[OK]");

        response = sendCommandToServer("select * from lotr;");
        assertTrue(response.contains("Name"), "Case not preserved in table headers");
        assertTrue(response.contains("Aragorn"), "Case not preserved in table data");
        assertTrue(response.contains("TRUE"), "Case not preserved in table data");
        

        response = sendCommandToServer("SELECT * FROM lotr WHERE name == 'gimli';");
        assertTrue(response.contains("Gimli"), "Case not preserved when querying table data. Queries are not case-insensitive");
        

        // Multiple conditions - precedence?
        // This one should return 2 rows: Aragorn (man and alive) and Boromir (name like B)
        response = sendCommandToServer("select * from lotr where (race == Man AND alive == TRUE) OR (name LIKE 'B');");
        
        assertTrue(response.contains("Aragorn") && response.contains("Boromir"), "Precedence not working");

        // This one should return 1 row: Boromir ((man or alive) and name like B)
        response = sendCommandToServer("select * from lotr where (race == Man OR alive == TRUE) AND (name LIKE 'B');");
        
        assertTrue(response.contains("Boromir"), "Precedence not working");
        assertFalse(response.contains("Aragorn"), "Precedence not working");

        // Swapping the order should not alter the returned results
        // If precedence is NOT working, returns: Boromir
        // If precedence IS working, returns: Legolas, Gimli, Boromir, Gandalf
        response = sendCommandToServer("select * from lotr where name LIKE 'L' OR (race == Man AND alive == FALSE);");
        
        assertTrue(response.contains("Legolas") && response.contains("Boromir") &&
                response.contains("Gimli"), "Precedence not working");

        // This should return legolas and gimli if precedence is working, and all of the alive people if not
        response = sendCommandToServer("select * from lotr where name LIKE 'L' AND (race == Man OR alive == TRUE);");
        
        assertTrue(response.contains("Legolas") && response.contains("Gimli"), "Precedence not working");
        assertFalse(response.contains("Aragorn"), "Precedence not working");


        // trying to select from a table that doesnt exist returns [ERROR]
        response = sendCommandToServer("SELECT * FROM ANonExistentTable;");
        assertTrue(response.contains("[ERROR]"), "An invalid query was made (selecting from a table that doesn't exist), however an [ERROR] tag was not returned");
        assertFalse(response.contains("[OK]"), "An invalid query was made (selecting from a table that doesn't exist), however an [OK] tag was returned");
        assertTrue(response.contains("this table does not exist"), "Error message informing user that the selected table does not exist was not returned.");

        // Trying to select columns that don't exist returns only columns that do exist
        response = sendCommandToServer("SELECT name, age FROM lotr;");
        assertTrue(response.contains("[OK]"), "A valid query was made, however an [OK] tag was not returned");
        assertTrue(response.contains("Name"), "The column that does exist in the table was not returned in response to the query.");
        assertFalse(response.contains("Age"), "The column that does NOT exist in the table was returned in response to the query.");

        // Trying to select ONLY columns that don't exist returns an [ERROR]
        response = sendCommandToServer("SELECT DateOfBirth, Age FROM lotr;");
        assertTrue(response.contains("[ERROR]"), "An [ERROR] tag was not returned in response to a command to select only columns that don't exist in the table.");
        assertTrue(response.contains("None of the selected columns exist in this table."), "The correct error message was not returned in response to a request to select columns that don't exist in the table.");
        assertFalse(response.contains("DateOfBirth"), "A column that does NOT exist in the table was returned in response to the query.");

        // Applying a WHERE condition to columns that don't exist returns [ERROR]
        response = sendCommandToServer("SELECT * FROM lotr WHERE Age > 20;");
        assertTrue(response.contains("[ERROR]"), "An [ERROR] tag was not returned in response to a command containing a WHERE condition based on columns that don't exist in the table.");
        assertTrue(response.contains("Column age does not exist in this table"), "An appropriate error message was not returned in response to a command containing a WHERE condition based on columns that don't exist in the table.");

        // Trying to update columns using where conditions that are malformed (invalid comparators/boolean operators) returns [ERROR]
        response = sendCommandToServer("SELECT * FROM lotr WHERE name = Aragorn;");
        assertTrue(response.contains("[ERROR]"), "An invalid query was made (attempting to select a column based on a malformed condition - invalid comparator), however an [ERROR] tag was not returned");
        assertFalse(response.contains("[OK]"), "An invalid query was made (attempting to select a column based on a malformed condition - invalid comparator), however an [OK] tag was returned");
        assertTrue(response.contains("Malformed condition"), "Error message informing user that the condition is malformed (invalid comparator) was not returned.");

        response = sendCommandToServer("SELECT * FROM lotr WHERE name == Aragorn ANDALSO Race == Man;");
        assertTrue(response.contains("[ERROR]"), "An invalid query was made (attempting to select a column based on a malformed condition - invalid boolean operator), however an [ERROR] tag was not returned");
        assertFalse(response.contains("[OK]"), "An invalid query was made (attempting to select a column based on a malformed condition - invalid boolean operator), however an [OK] tag was returned");
        assertTrue(response.contains("Malformed condition"), "Error message informing user that the condition is malformed (invalid boolean operator) was not returned.");

        // Malformed queries (misspelled keywords, wrong order, etc.) returns [ERROR]
        response = sendCommandToServer("SELCT * FROM lotr;");
        assertTrue(response.contains("[ERROR]"));
        assertTrue(response.contains("The keyword SELCT is invalid"));

        response = sendCommandToServer("SELECT * FROM TABLE lotr;;");
        assertTrue(response.contains("[ERROR]"));
        assertTrue(response.contains("Malformed query."));

        response = sendCommandToServer("SELECT FROM lotr;");
        assertTrue(response.contains("[ERROR]"));
        assertTrue(response.contains("No columns were selected"));

        // Select with conditions where numeric comparators are used on strings and string comparators
        // are used on integers.
        // Select with >, <. <=, >= comparators on string return empty table
        response = sendCommandToServer("SELECT * FROM lotr WHERE name > Aragorn;");
        assertTrue(response.contains("[OK]"), "Select where no valid comparison is possible (> on string) failed to return [OK]");
        assertFalse(response.contains("[ERROR]"), "Select where no valid comparison is possible (> on string) failed to return [OK]");
        assertTrue(response.contains("id") && response.contains("Name") && response.contains("Race") && response.contains("Alive"),
                "Select where no valid comparison is possible (> on string) failed to return an empty table with intact headers");
        assertFalse(response.contains("Aragorn") && response.contains("Gimli"),
                "Select where no valid comparison is possible (> on string) failed to return an empty table with no data present");

        // Select with >, <. <=, >= comparators on string return lexographic comparison
        response = sendCommandToServer("SELECT * FROM lotr WHERE name > 'g';");
        assertTrue(response.contains("[OK]"), "Select with lexographic comparison (> on string) failed to return [OK]");
        assertFalse(response.contains("[ERROR]"), "Select with lexographic comparison (> on string) returned [ERROR]");
        assertTrue(response.contains("id") && response.contains("Name") && response.contains("Race") && response.contains("Alive"),
                "Select with lexographic comparison (> on string) failed to return a table with intact headers");
        assertFalse(response.contains("Aragorn"), "Select with lexographic comparison (> on string) failed to return correct data");
        assertTrue(response.contains("Legolas"), "Select with lexographic comparison (> on string) failed to return correct data");

        // Select where comparison is impossible (e.g. a string being compared to an integer) returns empty table
        response = sendCommandToServer("SELECT * FROM lotr WHERE name > 12;");
        assertTrue(response.contains("[OK]"), "Select where no valid comparison is possible (> on string) failed to return [OK]");
        assertFalse(response.contains("[ERROR]"), "Select where no valid comparison is possible (> on string) returned [ERROR]");
        assertTrue(response.contains("id") && response.contains("Name") && response.contains("Race") && response.contains("Alive"),
                "Select where no valid comparison is possible (> on string) failed to return an empty table with intact headers");
        assertFalse(response.contains("Aragorn") && response.contains("Gimli"),
                "Select where no valid comparison is possible (> on string) failed to return an empty table with no data present");

        // Select with != comparator on string return values not equal to the input value
        response = sendCommandToServer("SELECT * FROM lotr WHERE name != Aragorn;");
        assertTrue(response.contains("[OK]"), "Select with != comparator on string failed to return [OK]");
        assertFalse(response.contains("[ERROR]"), "Select with != comparator on string returned [ERROR]");
        assertFalse(response.contains("Aragorn"), "Select with != comparator on string returned the string it was supposed to select against");
        assertTrue(response.contains("Gimli"), "Select with != comparator on string failed to return values not equal to the comparison value");

        // Select with 'like' comparators on integer treats the integer data as string
        assertEquals(sendCommandToServer("USE newdb;"), "[OK]");
        assertEquals(sendCommandToServer("DROP TABLE animals;"), "[OK]");
        assertEquals(sendCommandToServer("CREATE TABLE animals (type, colour, number);"), "[OK]");
        assertEquals(sendCommandToServer("INSERT INTO animals VALUES (lion, gold, 1);"), "[OK]");
        assertEquals(sendCommandToServer("INSERT INTO animals VALUES (dolphin, grey, 3);"), "[OK]");
        assertEquals(sendCommandToServer("INSERT INTO animals VALUES (sheep, white, 22);"), "[OK]");
        assertEquals(sendCommandToServer("INSERT INTO animals VALUES (cat, black, 13);"), "[OK]");
        response = sendCommandToServer("SELECT * FROM animals WHERE number LIKE '2';");
        assertTrue(response.contains("[OK]"), "Select with string pattern matching on integer failed to return [OK]");
        assertFalse(response.contains("[ERROR]"), "Select with string pattern matching on integer returned [ERROR]");
        assertTrue(response.contains("id") && response.contains("type") && response.contains("colour") && response.contains("number"),
                "Select with string pattern matching on integer failed to return a table with intact headers");
        assertTrue(response.contains("sheep") && response.contains("22"),
                "Select with string pattern matching on integer failed to return pattern-matched data");

    }

    @Test
    public void handleUpdateDeleteTest() {
        sendCommandToServer("DROP DATABASE newdb;");
        sendCommandToServer("CREATE DATABASE newdb;");
        assertEquals(sendCommandToServer("USE newdb;"), "[OK]");
        sendCommandToServer("DROP TABLE animals;");
        assertEquals(sendCommandToServer("CREATE TABLE animals (type, colour, number);"), "[OK]");
        assertEquals(sendCommandToServer("INSERT INTO animals VALUES (lion, gold, 1);"), "[OK]");
        assertEquals(sendCommandToServer("INSERT INTO animals VALUES (dolphin, grey, 3);"), "[OK]");
        assertEquals(sendCommandToServer("INSERT INTO animals VALUES (sheep, white, 22);"), "[OK]");
        assertEquals(sendCommandToServer("INSERT INTO animals VALUES (cat, black, 13);"), "[OK]");
        assertEquals(sendCommandToServer("INSERT INTO animals VALUES (spider, black, 100);"), "[OK]");
        assertEquals(sendCommandToServer("INSERT INTO animals VALUES (pony, white, 6);"), "[OK]");

        // Test Update
        assertEquals(sendCommandToServer("UPDATE animals SET number = 7 WHERE type == 'pony';"), "[OK]");
        String response = sendCommandToServer("SELECT number FROM animals WHERE type == pony;");
        assertTrue(response.contains("7"), "Failed to update the pony record to have number = 7.");

        assertEquals(sendCommandToServer("UPDATE animals SET colour = tabby WHERE type == cat;"), "[OK]");
        response = sendCommandToServer("SELECT colour FROM animals WHERE type == cat;");
        assertTrue(response.contains("tabby"), "Failed to update the cat record to have colour = tabby.");

        // Test Delete
        assertEquals(sendCommandToServer("DELETE FROM animals WHERE type == 'spider';"), "[OK]");
        response = sendCommandToServer("SELECT * FROM animals;");
        assertFalse(response.contains("spider"), "Failed to delete the spider record.");
        assertTrue(response.contains("dolphin"), "Deleted records that were not specified in the condition number > 20!.");

        assertEquals(sendCommandToServer("DELETE FROM animals WHERE number > 20;"), "[OK]");
        response = sendCommandToServer("SELECT * FROM animals;");
        assertFalse(response.contains("sheep"), "Failed to delete records where number > 20.");
        assertTrue(response.contains("lion"), "Deleted records that were not specified in the condition number > 20!.");

        // Test case preservation of column names + case-insensitivity of querying
        assertEquals(sendCommandToServer("USE newdb;"), "[OK]");
        sendCommandToServer("DROP TABLE lotr;");
        assertEquals(sendCommandToServer("CREATE TABLE lotr (Name, Race, Alive);"), "[OK]");
        assertEquals(sendCommandToServer("INSERT INTO lotr VALUES (Aragorn, Man, TRUE);"), "[OK]");
        assertEquals(sendCommandToServer("INSERT INTO lotr VALUES (Gimli, Dwarf, TRUE);"), "[OK]");
        assertEquals(sendCommandToServer("UPDATE lotr SET alive = FALSE WHERE name == gimli;"), "[OK]");
        response = sendCommandToServer("SELECT * FROM lotr WHERE name == gimli;");
        assertTrue(response.contains("FALSE"), "Failed to preserve case in input value.");
        assertFalse(response.contains("false"), "Failed to preserve case in input value.");

        // Trying to update/delete tables that don't exist returns [ERROR]
        response = sendCommandToServer("UPDATE ANonExistentTable SET col1 = 1 WHERE col2 == 2;");
        assertTrue(response.contains("[ERROR]"), "An invalid query was made (attempting to update a table that doesn't exist), however an [ERROR] tag was not returned");
        assertFalse(response.contains("[OK]"), "An invalid query was made (attempting to update a table that doesn't exist), however an [OK] tag was returned");
        assertTrue(response.contains("this table does not exist"), "Error message informing user that the selected table does not exist was not returned.");

        response = sendCommandToServer("DELETE FROM ANonExistentTable WHERE col1 == 1;");
        assertTrue(response.contains("[ERROR]"), "An invalid query was made (attempting to delete from a table that doesn't exist), however an [ERROR] tag was not returned");
        assertFalse(response.contains("[OK]"), "An invalid query was made (attempting to delete from a table that doesn't exist), however an [OK] tag was returned");
        assertTrue(response.contains("this table does not exist"), "Error message informing user that the selected table does not exist was not returned.");

        // Trying to update columns that don't exist returns [ERROR]
        response = sendCommandToServer("UPDATE lotr SET Age = 200 WHERE Name == Gimli;");
        assertTrue(response.contains("[ERROR]"), "An invalid query was made (attempting to update columns that don't exist), however an [ERROR] tag was not returned");
        assertFalse(response.contains("[OK]"), "An invalid query was made (attempting to update columns that don't exist), however an [OK] tag was returned");
        assertTrue(response.contains("Column name age does not exist within this table"), "Error message informing user that the column to be updated does not exist was not returned.");

        // Trying to update the 'id' column returns ERROR
        response = sendCommandToServer("UPDATE lotr SET id = 13 WHERE Name == Gimli;");
        assertTrue(response.contains("[ERROR]"), "An invalid query was made (attempting to manually update the id column), however an [ERROR] tag was not returned");
        assertFalse(response.contains("[OK]"), "An invalid query was made (attempting to manually update the id column), however an [OK] tag was returned");
        assertTrue(response.contains("Manually changing the 'id' column is forbidden."), "Error message informing user that it is forbidden to manually change the id column was not returned.");


        // Trying to update columns using conditions on columns that dont exist returns [ERROR]
        response = sendCommandToServer("UPDATE lotr SET Name = Frodo WHERE Age == 53;");
        assertTrue(response.contains("[ERROR]"), "An invalid query was made (WHERE condition on column that doesn't exist), however an [ERROR] tag was not returned");
        assertFalse(response.contains("[OK]"), "An invalid query was made (WHERE condition on column that doesn't exist), however an [OK] tag was returned");
        assertTrue(response.contains("Column age does not exist in this table"), "Error message informing user that the condition column does not exist was not returned.");

        // Trying to update columns using where conditions that are never true for any of the data in table returns [ERROR]
        response = sendCommandToServer("UPDATE lotr SET Alive = FALSE WHERE Name == Denethor;");
        assertTrue(response.contains("[ERROR]"), "An invalid query was made (attempting to update a column based on a condition that is never true), however an [ERROR] tag was not returned");
        assertFalse(response.contains("[OK]"), "An invalid query was made (attempting to update a column based on a condition that is never true), however an [OK] tag was returned");
        assertTrue(response.contains("No rows in the table matched the condition. Nothing was changed."), "Error message informing user that the condition is never true was not returned.");

        // Trying to update columns using where conditions that are malformed (invalid comparators/boolean operators) returns [ERROR]
        response = sendCommandToServer("UPDATE lotr SET Alive = FALSE WHERE name = Aragorn;");
        assertTrue(response.contains("[ERROR]"), "An invalid query was made (attempting to update a column based on a malformed condition - invalid comparator), however an [ERROR] tag was not returned");
        assertFalse(response.contains("[OK]"), "An invalid query was made (attempting to update a column based on a malformed condition - invalid comparator), however an [OK] tag was returned");
        assertTrue(response.contains("Malformed condition"), "Error message informing user that the condition is malformed (invalid comparator) was not returned.");

        response = sendCommandToServer("UPDATE lotr SET Alive = FALSE WHERE name == Aragorn ANDALSO Race == Man;");
        assertTrue(response.contains("[ERROR]"), "An invalid query was made (attempting to update a column based on a malformed condition - invalid boolean operator), however an [ERROR] tag was not returned");
        assertFalse(response.contains("[OK]"), "An invalid query was made (attempting to update a column based on a malformed condition - invalid boolean operator), however an [OK] tag was returned");
        assertTrue(response.contains("Malformed condition"), "Error message informing user that the condition is malformed (invalid boolean operator) was not returned.");

        // Trying to delete rows using conditions on columns that dont exist returns [ERROR]
        response = sendCommandToServer("DELETE FROM lotr WHERE Age == 200;");
        assertTrue(response.contains("[ERROR]"), "An invalid query was made (WHERE condition on column that doesn't exist), however an [ERROR] tag was not returned");
        assertFalse(response.contains("[OK]"), "An invalid query was made (WHERE condition on column that doesn't exist), however an [OK] tag was returned");
        assertTrue(response.contains("Column age does not exist in this table"), "Error message informing user that the condition column does not exist was not returned.");

        // Trying to delete rows using where conditions that are never true for any of the data in table returns [ERROR]
        response = sendCommandToServer("DELETE FROM lotr WHERE Name == Denethor;");
        assertTrue(response.contains("[ERROR]"), "An invalid query was made (attempting to delete rows based on a condition that is never true), however an [ERROR] tag was not returned");
        assertFalse(response.contains("[OK]"), "An invalid query was made (attempting to delete rows based on a condition that is never true), however an [OK] tag was returned");
        assertTrue(response.contains("No rows in the table matched the condition. Nothing was changed."), "Error message informing user that the condition is never true was not returned.");

        // Malformed queries - wrong/misspelled keywords, wrong order returns [ERROR]
        response = sendCommandToServer("UPDTE lotr SET Alive = FALSE WHERE Name == Gimli;");
        assertTrue(response.contains("[ERROR]"));
        assertTrue(response.contains("The keyword UPDTE is invalid"));

        response = sendCommandToServer("UPDATE TABLE lotr SET Alive = FALSE WHERE Name == Gimli;");
        assertTrue(response.contains("[ERROR]"));
        assertTrue(response.contains("Malformed query"));

        response = sendCommandToServer("UPDATE lotr WHERE Name == Gimli;");
        assertTrue(response.contains("[ERROR]"));
        assertTrue(response.contains("Query is malformed."));

        response = sendCommandToServer("UPDATE lotr SET Alive = FALSE;");
        assertTrue(response.contains("[ERROR]"));
        assertTrue(response.contains("Query is malformed"));

        response = sendCommandToServer("UPDATE lotr SET Alive = FALSE WERE Name == Gimli;");
        assertTrue(response.contains("[ERROR]"));
        assertTrue(response.contains("Query is malformed"));

        response = sendCommandToServer("DELET FROM lotr WHERE name == Gimli;");
        assertTrue(response.contains("[ERROR]"));
        assertTrue(response.contains("The keyword DELET is invalid"));

        response = sendCommandToServer("DELETE lotr WHERE name == Gimli;");
        assertTrue(response.contains("[ERROR]"));
        assertTrue(response.contains("Query is malformed"));

        response = sendCommandToServer("DELETE FROM lotr;");
        assertTrue(response.contains("[ERROR]"));
        assertTrue(response.contains("Query is malformed"));

        response = sendCommandToServer("DELETE FROM lotr name == Gimli;");
        assertTrue(response.contains("[ERROR]"));
        assertTrue(response.contains("Query is malformed"));
    }

    @Test
    public void handleJoinTest() {
        sendCommandToServer("DROP DATABASE newdb;");
        sendCommandToServer("CREATE DATABASE newdb;");
        assertEquals(sendCommandToServer("USE newdb;"), "[OK]");
        sendCommandToServer("DROP TABLE pets;");
        assertEquals(sendCommandToServer("CREATE TABLE pets (Name, Owner, Species);"), "[OK]");
        assertEquals(sendCommandToServer("INSERT INTO pets VALUES (Bobby, Amy, dog);"), "[OK]");
        assertEquals(sendCommandToServer("INSERT INTO pets VALUES (Luna, John, dog);"), "[OK]");
        assertEquals(sendCommandToServer("INSERT INTO pets VALUES (Dexter, Danny, cat);"), "[OK]");
        assertEquals(sendCommandToServer("INSERT INTO pets VALUES (Alonso, Sarah, cat);"), "[OK]");
        assertEquals(sendCommandToServer("INSERT INTO pets VALUES (Rocky, Don, mouse);"), "[OK]");
        assertEquals(sendCommandToServer("INSERT INTO pets VALUES (Elise, Daisy, gecko);"), "[OK]");

        sendCommandToServer("DROP TABLE owners;");
        assertEquals(sendCommandToServer("CREATE TABLE owners (Name, Age);"), "[OK]");
        assertEquals(sendCommandToServer("INSERT INTO owners VALUES (Amy, 34);"), "[OK]");
        assertEquals(sendCommandToServer("INSERT INTO owners VALUES (John, 50);"), "[OK]");
        assertEquals(sendCommandToServer("INSERT INTO owners VALUES (Daisy, 12);"), "[OK]");
        assertEquals(sendCommandToServer("INSERT INTO owners VALUES (Don, 49);"), "[OK]");
        assertEquals(sendCommandToServer("INSERT INTO owners VALUES (Sarah, 76);"), "[OK]");
        assertEquals(sendCommandToServer("INSERT INTO owners VALUES (Mary, 61);"), "[OK]");

        String response = sendCommandToServer("JOIN owners AND pets ON name AND owner;");
        assertTrue(response.contains("[OK]"), "A valid query was made, however an [OK] tag was not returned");
        assertFalse(response.contains("[ERROR]"), "A valid query was made, however an [ERROR] tag was returned");
        assertTrue(response.contains("Bobby"), "Joining table resulted in loss of data. No 'bobby' record in joined table");
        assertTrue(response.contains("pets.Name"), "Joined table does not contain the expected prepended column name 'pets.name'");
        assertTrue(response.contains("owners.Age"), "Joined table does not contain the expected prepended column name 'owners.age'");
        assertFalse(response.contains("Mary"), "Inner join returned rows that were not present in one of the tables.");
        assertFalse(response.contains("Dexter"), "Inner join returned rows that were not present in one of the tables.");

        // Trying to join tables that don't exist returns [ERROR]
        response = sendCommandToServer("JOIN owners AND pet ON name AND owner;");
        assertTrue(response.contains("[ERROR]"), "An invalid query was made (attempting to join a table that doesn't exist), however an [ERROR] tag was not returned");
        assertFalse(response.contains("[OK]"), "An invalid query was made (attempting to join a table that doesn't exist), however an [OK] tag was returned");
        assertTrue(response.contains("this table does not exist"), "Error message informing user that the selected table does not exist was not returned.");

        response = sendCommandToServer("JOIN owner AND pets ON name AND owner;");
        assertTrue(response.contains("[ERROR]"), "An invalid query was made (attempting to update a table that doesn't exist), however an [ERROR] tag was not returned");
        assertFalse(response.contains("[OK]"), "An invalid query was made (attempting to update a table that doesn't exist), however an [OK] tag was returned");
        assertTrue(response.contains("this table does not exist"), "Error message informing user that the selected table does not exist was not returned.");

        // Trying to join on columns that don't exist in the tables returns [ERROR]
        response = sendCommandToServer("JOIN owners AND pets ON petName AND owner;");
        assertTrue(response.contains("[ERROR]"), "An invalid query was made (attempting to update columns that don't exist), however an [ERROR] tag was not returned");
        assertFalse(response.contains("[OK]"), "An invalid query was made (attempting to update columns that don't exist), however an [OK] tag was returned");
        assertTrue(response.contains("The joining column petname does not exist in the table."), "Error message informing user that the column to be updated does not exist was not returned.");

        response = sendCommandToServer("JOIN owners AND pets ON name AND ownerName;");
        assertTrue(response.contains("[ERROR]"), "An invalid query was made (attempting to update columns that don't exist), however an [ERROR] tag was not returned");
        assertFalse(response.contains("[OK]"), "An invalid query was made (attempting to update columns that don't exist), however an [OK] tag was returned");
        assertTrue(response.contains("The joining column ownername does not exist in the table."), "Error message informing user that the column to be updated does not exist was not returned.");

        // Inner join where no data is matched between tables returns empty table
        sendCommandToServer("DROP TABLE owners2;");
        assertEquals(sendCommandToServer("CREATE TABLE owners2 (Name, Age);"), "[OK]");
        assertEquals(sendCommandToServer("INSERT INTO owners VALUES (Walter, 33);"), "[OK]");
        assertEquals(sendCommandToServer("INSERT INTO owners VALUES (Elizabeth, 80);"), "[OK]");
        response = sendCommandToServer("JOIN owners2 AND pets ON name AND owner;");
        assertTrue(response.contains("[OK]"), "A valid query was made (joining two tables with no matches), however an [OK] tag was not returned");
        assertFalse(response.contains("[ERROR]"), "A valid query was made (joining two tables with no matches), however an [ERROR] tag was returned");
        assertTrue(response.contains("owners2.Age") && response.contains("pets.Name"), "Prepended table headers were not returned.");

        // Inner join where the only columns in the tables are id and the joining columns - only a new id column is returned
        sendCommandToServer("DROP TABLE names1;");
        sendCommandToServer("DROP TABLE names2;");
        assertEquals(sendCommandToServer("CREATE TABLE names1 (Name);"), "[OK]");
        assertEquals(sendCommandToServer("INSERT INTO names1 VALUES (Walter);"), "[OK]");
        assertEquals(sendCommandToServer("INSERT INTO names1 VALUES (Elizabeth);"), "[OK]");
        assertEquals(sendCommandToServer("CREATE TABLE names2 (Name);"), "[OK]");
        assertEquals(sendCommandToServer("INSERT INTO names2 VALUES (Elizabeth);"), "[OK]");
        assertEquals(sendCommandToServer("INSERT INTO names2 VALUES (Walter);"), "[OK]");
        response = sendCommandToServer("JOIN names1 AND names2 ON name AND name;");
        assertTrue(response.contains("[OK]"), "A valid query was made (joining two identical tables), however an [OK] tag was not returned");
        assertFalse(response.contains("[ERROR]"), "A valid query was made (joining two identical tables), however an [ERROR] tag was returned");
        assertTrue(response.contains("id"), "id header was not returned.");
        assertFalse(response.contains("names1.Name") && response.contains("names2.Name") &&
                response.contains("Walter") && response.contains("Elizabeth"), "table data from joining columns was returned.");

        // Malformed queries returns [ERROR]
        response = sendCommandToServer("JOYN owners AND pets ON name AND owner;");
        assertTrue(response.contains("[ERROR]"));
        assertTrue(response.contains("The keyword JOYN is invalid"));

        response = sendCommandToServer("JOIN owners ON name AND owner;");
        assertTrue(response.contains("[ERROR]"));
        assertTrue(response.contains("Query is malformed"));

        response = sendCommandToServer("JOIN owners, pets ON name, owner;");
        assertTrue(response.contains("[ERROR]"));
        assertTrue(response.contains("Query is malformed"));

        response = sendCommandToServer("JOIN owners AND pets ON name, owner;");
        assertTrue(response.contains("[ERROR]"));
        assertTrue(response.contains("Query is malformed"));

        response = sendCommandToServer("JOIN owners AND pets BY name AND owner;");
        assertTrue(response.contains("[ERROR]"));
        assertTrue(response.contains("Query is malformed"));
    }

    @Test
    public void handleJoinMultipleMatchesTest() {
        sendCommandToServer("DROP DATABASE newdb;");
        sendCommandToServer("CREATE DATABASE newdb;");
        // Test that join returns all permutations of all matches when
        // joining tables for which there are multiple matches.
        assertEquals(sendCommandToServer("USE newdb;"), "[OK]");
        sendCommandToServer("DROP TABLE pets;");
        assertEquals(sendCommandToServer("CREATE TABLE pets (Name, Owner, Species);"), "[OK]");
        assertEquals(sendCommandToServer("INSERT INTO pets VALUES (Bobby, Amy, dog);"), "[OK]");
        assertEquals(sendCommandToServer("INSERT INTO pets VALUES (Bobby, Danny, dog);"), "[OK]");
        assertEquals(sendCommandToServer("INSERT INTO pets VALUES (Dexter, Danny, cat);"), "[OK]");
        assertEquals(sendCommandToServer("INSERT INTO pets VALUES (Dexter, Amy, cat);"), "[OK]");
        assertEquals(sendCommandToServer("INSERT INTO pets VALUES (Rocky, Don, mouse);"), "[OK]");
        assertEquals(sendCommandToServer("INSERT INTO pets VALUES (Rubarb, Don, mouse);"), "[OK]");
        assertEquals(sendCommandToServer("INSERT INTO pets VALUES (Ralph, Don, mouse);"), "[OK]");
        assertEquals(sendCommandToServer("INSERT INTO pets VALUES (Elise, Daisy, gecko);"), "[OK]");

        sendCommandToServer("DROP TABLE owners;");
        assertEquals(sendCommandToServer("CREATE TABLE owners (Name, Age);"), "[OK]");
        assertEquals(sendCommandToServer("INSERT INTO owners VALUES (Amy, 34);"), "[OK]");
        assertEquals(sendCommandToServer("INSERT INTO owners VALUES (Danny, 50);"), "[OK]");
        assertEquals(sendCommandToServer("INSERT INTO owners VALUES (Daisy, 12);"), "[OK]");
        assertEquals(sendCommandToServer("INSERT INTO owners VALUES (Don, 49);"), "[OK]");
        assertEquals(sendCommandToServer("INSERT INTO owners VALUES (Sarah, 76);"), "[OK]");
        assertEquals(sendCommandToServer("INSERT INTO owners VALUES (Mary, 61);"), "[OK]");

        String response = sendCommandToServer("JOIN owners AND pets ON name AND owner;");
        
        assertTrue(response.contains("id"), "id header was not returned.");
        assertTrue(response.contains("34\tBobby") && response.contains("50\tBobby"),
                "Joining rows with multiple matches did not return all permutations of the matched rows.");
        assertTrue(response.contains("34\tDexter") && response.contains("50\tDexter"),
                "Joining rows with multiple matches did not return all permutations of the matched rows.");
        assertTrue(response.contains("49\tRocky") && response.contains("49\tRubarb") && response.contains("49\tRalph"),
                "Joining rows with multiple matches did not return all permutations of the matched rows.");
    }

}