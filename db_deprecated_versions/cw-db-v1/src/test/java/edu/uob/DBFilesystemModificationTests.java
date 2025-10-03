package edu.uob;

import java.util.*;
import java.io.*;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;

public class DBFilesystemModificationTests {
    private DBServer server;
    private QueryHandler handler;

    // Create a new server _before_ every @Test
    @BeforeEach
    public void setup() {
        server = new DBServer();
    }

    private String sendCommandToServer(String command) {
        // Try to send a command to the server - this call will timeout if it takes too long (in case the server enters an infinite loop)
        return assertTimeoutPreemptively(Duration.ofMillis(1000), () -> { return server.handleCommand(command);},
                "Server took too long to respond (probably stuck in an infinite loop)");
    }

    /*
    // Test creating a database works
    @Test
    public void createDatabaseTest() {
        handler = new QueryHandler("testdb", null);
        //String responseMessage = server.createDatabase("testdb");
        String responseMessage = handler.createDatabase();
        assertTrue("OK".equals(responseMessage), "Creating database did not return expected response message of 'OK'. Response message returned: " + responseMessage);
        File newDatabase = new File("./databases/testdb");
        assertTrue(newDatabase.exists());
    }

    // Test deleting a database works
    @Test
    public void deleteDatabaseTest() {
        // First ensure that the database exists
        File newDatabase = new File("./databases/testdb");
        assertTrue(newDatabase.exists(), "Oops! You can't create a database that doesn't exist! Please check that the testing database named 'testdb' exists");

        // Delete database
        //String responseMessage = server.deleteDatabase("testdb");
        handler = new QueryHandler("testdb", null);
        String responseMessage = handler.deleteDatabase();
        File droppedDatabase = new File("./databases/testdb");
        assertFalse(droppedDatabase.exists(), "Database deletion failed. The database named 'testdb' still exists.");
        assertTrue("OK".equals(responseMessage), "Database deletion failed to return the expected success response message of 'OK'. Actual response message recieved: " + responseMessage);
    }

    // Test that you are able to create database tables
    @Test
    public void createDBTables() {
        // First, create a new database. If the database already exists, delete it
        // and create a new one (ensures there are no tables already in database)
        handler = new QueryHandler("testdb", "table1");
        handler.deleteDatabase();

        //String createDBResponseMessage = server.createDatabase("testdb");
        String createDBResponseMessage = handler.createDatabase();
        assertTrue("OK".equals(createDBResponseMessage), "Failed to created database 'testdb'. Response message: " + createDBResponseMessage);

        // Now create tables
        //String table1Response = server.createDBTable("testdb", "table1");
        String table1Response = handler.createDBTable();
        assertTrue("OK".equals(table1Response), "Failed to create table1. Response message: " + table1Response);
        assertTrue(new File("./databases/testdb/table1.tab").exists());

        // If you try to create a table that already exists, the handler gracefully informs you of this
        // and does nothing else
        String table1Response2 = handler.createDBTable();

        assertTrue("This table already exists! Please choose a different name for your new table.".equals(table1Response2));

        // ...but you can create more tables
        handler.setCurrentTable("table2");
        String table2Response = handler.createDBTable();
        assertTrue("OK".equals(table2Response), "Failed to create table2. Response message: " + table2Response);
        assertTrue(new File("./databases/testdb/table2.tab").exists());
    }

    // Test that you are able to create database tables
    @Test
    public void deleteDBTables() {
        // First, create a new database. If the database already exists, delete it
        // and create a new one (ensures there are no tables already in database)
        handler = new QueryHandler("testdb", "table1");
        handler.deleteDatabase();
        String createDBResponseMessage = handler.createDatabase();
        assertTrue("OK".equals(createDBResponseMessage), "Failed to created database 'testdb'. Response message: " + createDBResponseMessage);

        // Now create tables to be deleted
        assertTrue("OK".equals(handler.createDBTable()));
        handler.setCurrentTable("table2");
        assertTrue("OK".equals(handler.createDBTable()));

        // There are now 2 tables in 'testdb': table1 and table2
        String deleteMessage = handler.deleteDBTable(); // delete table2
        assertTrue("OK".equals(deleteMessage), "Failed to delete table2. Responce message: " + deleteMessage);

        handler.setCurrentTable("table1");
        deleteMessage = handler.deleteDBTable();
        assertTrue("OK".equals(deleteMessage), "Failed to delete table1. Responce message: " + deleteMessage);

        // Gracefully informs user that it can't delete a table that doesn't exist
        deleteMessage = handler.deleteDBTable();
        assertTrue("Sorry, you can't delete table table1 because either the table or the database does not exist.".equals(deleteMessage));
    }

     */
}
