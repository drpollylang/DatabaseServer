package edu.uob;

import java.util.*;
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

import java.io.*;
import java.util.*;


public class DBFileIOTests {
    private DBServer server;

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

    // Read metadata
    /*
    @Test
    public void readMetadata() {
        QueryHandler handler = new QueryHandler("testdb", "table1");
        handler.deleteDatabase();
        handler.createDatabase();
        handler.createDBTable();
        DBTable table = new DBTable("./databases/testdb/table1.tab");

        // Check metadata
        assertEquals(table.idCounter, 0);
        System.out.println("Metadata: " + table.metadata);
    }



    // Test reading well-formed data into Hashtable
    @Test
    public void readFromFileToHashTable() throws IOException {
        String filepath = "../people.tab";
        DBTable table = new DBTable(filepath);
        assertEquals(table.getNumberOfColumns(), 4, "Error: Hashtable contained incorrect number of columns (" + table.getNumberOfColumns() + ") - should be 4.");
        assertEquals(table.getNumberOfRows(), 3, "Error: Hashtable contained incorrect number of columns (" + table.getNumberOfRows() + ") - should be 4.");

        //ArrayList<String> colNames = new ArrayList<>(Arrays.asList("id", "Name", "Age", "Email"));
        ArrayList<String> colNames = table.getColumnNames();
        assertTrue(colNames.contains("id") &&
                        colNames.contains("Name") &&
                        colNames.contains("Age") &&
                        colNames.contains("Email"),
                "Error: column names did not match expected column names. Column names in Hashtable are: " + colNames);

        //ArrayList<String> firstRow = new ArrayList<>(Arrays.asList("1", "Bob", "21", "bob@bob.net"));
        ArrayList<String> firstRow = table.getTableRow(0);
        assertTrue(firstRow.contains("1") &&
                firstRow.contains("Bob") &&
                firstRow.contains("21") &&
                firstRow.contains("bob@bob.net"), "Error: First row's values did not match those expected. First row's values in hash table are: " + firstRow);

        assertEquals(table.getTableValue("Name", 0), "Bob", "Error: values in hash table not as expected. Expected 'Bob', Actual: " + table.getTableValue("Name", 0));
    }

    // Test reading data with missing records into Hashtable
    // Should NOT crash or error, but replace any missing data with null
    @Test
    public void readFileToHashTableMissingData() throws IOException {
        String filepath = "../sheds_missingdata.tab";
        DBTable table = new DBTable(filepath);
        assertEquals(table.getNumberOfColumns(), 4, "Error: Hashtable contained incorrect number of columns (" + table.getNumberOfColumns() + ") - should be 4.");
        assertEquals(table.getNumberOfRows(), 3, "Error: Hashtable contained incorrect number of columns (" + table.getNumberOfRows() + ") - should be 4.");

        //ArrayList<String> colNames = new ArrayList<>(Arrays.asList("id", "Name", "Age", "Email"));
        ArrayList<String> colNames = table.getColumnNames();
        assertTrue(colNames.contains("id") &&
                        colNames.contains("Name") &&
                        colNames.contains("Height") &&
                        colNames.contains("PurchaserID"),
                "Error: column names did not match expected column names. Column names in Hashtable are: " + colNames);

        //ArrayList<String> firstRow = new ArrayList<>(Arrays.asList("1", "Bob", "21", "bob@bob.net"));
        ArrayList<String> firstRow = table.getTableRow(0);
        assertTrue(firstRow.contains("1") &&
                firstRow.contains("Dorchester") &&
                firstRow.contains("1800") &&
                firstRow.contains("3"), "Error: First row's values did not match those expected. First row's values in hash table are: " + firstRow);

        assertEquals(table.getTableValue("Name", 2), "Excelsior", "Error: values in hash table not as expected. Expected 'Excelsior', Actual: " + table.getTableValue("Name", 0));
        assertEquals(table.getTableValue("Height", 2), null, "Error: values in hash table not as expected. Expected: null, Actual: " + table.getTableValue("Height", 2));
    }

    // Test that server handles malformed data gracefully - informs user of error
    // without crashing
    @Test
    public void readFileToHashTableMalformed() throws IOException {
        String filepath = "../people_malformed.tab";
        Exception exception = assertThrows(Exception.class, () ->
                new DBTable(filepath));
    }

    // Test writing data to file
    @Test
    public void writeToFile() {
        DBTable table = new DBTable("./databases/peopledb/people.tab");
        // Modify the first row's age to be 22
        table.setTableValue("Age", 0, "22");
        // Write to table
        table.writeDataToFile();
        // Read data - still readable?
        DBTable table2 = new DBTable("./databases/peopledb/people.tab");
    }
     */
}
