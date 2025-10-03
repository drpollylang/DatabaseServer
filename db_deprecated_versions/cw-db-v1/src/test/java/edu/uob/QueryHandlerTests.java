package edu.uob;

import java.util.*;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


public class QueryHandlerTests {
    private QueryHandler handler;

    private void handlerWrapper(String query) {
        try {
            handler = new QueryHandler(query);
        } catch (DBException exception) {
            exception.printStackTrace();
        }
    }

    @Test
    public void handleUseTest() {
        String query = "USE peopledb;";
        handlerWrapper(query);
    }

    @Test
    public void handleCreateTest() {
        String query = "CREATE DATABASE newdb;";
        handlerWrapper(query);

        handlerWrapper("USE newdb;");

        query = "CREATE TABLE newtable;";
        handlerWrapper(query);

        handlerWrapper("DROP TABLE newtable;");

        query = "CREATE TABLE newtable (col1, col2);";
        handlerWrapper(query);
    }

    @Test
    public void handleDropTest() {
        // Can it handle dropping a database that contains tables?
        String query = "DROP DATABASE newdb;";
        handlerWrapper(query);

        handlerWrapper("CREATE DATABASE newdb;");
        handlerWrapper("USE newdb;");
        handlerWrapper("CREATE TABLE newtable;");

        // Drop table first, then empty database
        handlerWrapper("DROP TABLE newtable;");
        handlerWrapper("DROP DATABASE newdb;");
    }

    @Test
    public void handleAlterTest() {
        handlerWrapper("DROP DATABASE newdb;");
        handlerWrapper("CREATE DATABASE newdb;");
        handlerWrapper("USE newdb;");
        handlerWrapper("CREATE TABLE newtable (animals, colour);");

        String query = "ALTER TABLE newtable ADD noise;";
        handlerWrapper(query);

        // Check new column exists in database
        //DBTable animalTable;
        //try {
        //    animalTable = new DBTable("/home/pollylang/MScBristol/OOPwithJava/JavaProjects/db/cw-db/databases/newdb/newtable.tab");
        //} catch (DBException error) {
        //    error.printStackTrace();
        //    return;
        //}
        //System.out.println(animalTable.getColumnNames());
        //assertTrue(animalTable.getColumnNames().contains("noise"), "Uh-oh! New column noise is not in table...");

        query = "ALTER TABLE newtable DROP noise;";
        handlerWrapper(query);
        //try {
        //    animalTable = new DBTable("/home/pollylang/MScBristol/OOPwithJava/JavaProjects/db/cw-db/databases/newdb/newtable.tab");
        //} catch (DBException error) {
        //    error.printStackTrace();
        //    return;
        //}
        //System.out.println(animalTable.getColumnNames());
        //assertFalse(animalTable.getColumnNames().contains("noise"), "Uh-oh! Failed to drop column noise...");

        // TODO: make it so that adding and dropping are not converted to lower but preserve case
        // TODO: Error handling for managing tables/attributes that dont exist
    }

    /*
    @Test
    public void handleInsertTest() {
        DBTable peopleTable;
        try {
            peopleTable = new DBTable("/home/pollylang/MScBristol/OOPwithJava/JavaProjects/db/cw-db/databases/peopledb/people_copy.tab");
        } catch (DBException error) {
            error.printStackTrace();
            return;
        }
        int initialNumberOfRows = peopleTable.getNumberOfRows();
        handlerWrapper("USE peopledb;");
        String query = "INSERT INTO people_copy VALUES ('Sophie', sophie@apple.net, 31);";
        handlerWrapper(query);

        try {
            peopleTable = new DBTable("/home/pollylang/MScBristol/OOPwithJava/JavaProjects/db/cw-db/databases/peopledb/people_copy.tab");
        } catch (DBException error) {
            error.printStackTrace();
            return;
        }
        assertEquals(initialNumberOfRows+1, peopleTable.getNumberOfRows(), "Uh-oh! Failed to insert new row into table. Number of rows should be:." + initialNumberOfRows + " but was actually " + peopleTable.getNumberOfRows());
    }

     */

    @Test
    public void handleInsertTest() {
        handlerWrapper("DROP DATABASE newdb;");
        handlerWrapper("CREATE DATABASE newdb;");
        handlerWrapper("USE newdb;");
        handlerWrapper("CREATE TABLE newtable (animals, colour);");
        DBTable animalTable;
        //try {
        //    animalTable = new DBTable("/home/pollylang/MScBristol/OOPwithJava/JavaProjects/db/cw-db/databases/newdb/newtable.tab");
        //} catch (DBException error) {
        //    error.printStackTrace();
        //    return;
        //}
        handlerWrapper("SELECT * FROM newtable;");
        System.out.println(handler.getResponseCode());
        System.out.println(handler.getResponseMessage());

        handlerWrapper("USE newdb;");
        String query = "INSERT INTO newtable VALUES (lion, gold);";
        handlerWrapper(query);
        handlerWrapper("INSERT INTO newtable VALUES (dolphin, grey);");
        handlerWrapper("INSERT INTO newtable VALUES (sheep, white);");
        handlerWrapper("INSERT INTO newtable VALUES (cat, black);");

        handlerWrapper("SELECT * FROM newtable;");
        System.out.println(handler.getResponseCode());
        System.out.println(handler.getResponseMessage());
    }

    /*
    @Test
    public void handleSelectTest() {
        DBTable peopleTable;
        try {
            peopleTable = new DBTable("/home/pollylang/MScBristol/OOPwithJava/JavaProjects/db/cw-db/databases/peopledb/people_copy.tab");
        } catch (DBException error) {
            error.printStackTrace();
            return;
        }
        handlerWrapper("USE peopledb;");

        String query = "SELECT * FROM people_copy;";
        handlerWrapper(query);

        handlerWrapper("SELECT Name, Age FROM people_copy;");

        handlerWrapper("SELECT Name, Age FROM people_copy WHERE age > 40;");

        handlerWrapper("SELECT * FROM people_copy WHERE name == 'Dave';");

        handlerWrapper("SELECT * FROM people_copy WHERE age < 40 AND name LIKE 'Sophie';");

        handlerWrapper("SELECT email FROM people_copy WHERE age > 40 OR name LIKE 'Sophie';");

        // This should return an empty string and not error
        handlerWrapper("SELECT * FROM people_copy WHERE age <= 0;");

        // This should return an empty string and not error
        handlerWrapper("SELECT aRandomColumn FROM people_copy WHERE age > 20;");

    }

     */

    @Test
    public void handleSelectTest() {
        handlerWrapper("DROP DATABASE newdb;");
        handlerWrapper("CREATE DATABASE newdb;");
        handlerWrapper("USE newdb;");
        handlerWrapper("CREATE TABLE newtable (animals, colour, number);");

        String query = "INSERT INTO newtable VALUES (lion, gold, 1);";
        handlerWrapper(query);
        handlerWrapper("INSERT INTO newtable VALUES (dolphin, grey, 2);");
        handlerWrapper("INSERT INTO newtable VALUES (sheep, white, 3);");
        handlerWrapper("INSERT INTO newtable VALUES (cat, black, 4);");

        handlerWrapper("SELECT * FROM newtable;");
        handlerWrapper("SELECT animals FROM newtable;");
        handlerWrapper("SELECT animals, colour FROM newtable;");
        handlerWrapper("SELECT * FROM newtable WHERE animals == lion;");
        handlerWrapper("SELECT * FROM newtable WHERE number >= 3;");
        handlerWrapper("SELECT * FROM newtable WHERE animals == lion AND colour == gold;");
        handlerWrapper("SELECT * FROM newtable WHERE animals == lion OR number > 3;");

    }

    /*
    @Test
    public void handleUpdateTest() {
        handlerWrapper("USE peopledb;");
        DBTable peopleTable;
        try {
            peopleTable = new DBTable("/home/pollylang/MScBristol/OOPwithJava/JavaProjects/db/cw-db/databases/peopledb/people_copy.tab");
        } catch (DBException error) {
            error.printStackTrace();
            return;
        }
        assertEquals(peopleTable.getTableValue(peopleTable.getColumnNames().indexOf("age"), peopleTable.getTableData().size()-1), "31");

        // TODO: not implemented in QueryHandler yet
        String query = "UPDATE people_copy SET age = 35 WHERE name LIKE 'Sophie';";
        assertEquals(peopleTable.getTableValue(peopleTable.getColumnNames().indexOf("age"), peopleTable.getTableData().size()-1), "35");
    }

     */

    @Test
    public void handleUpdateTest() {
        handlerWrapper("DROP DATABASE newdb;");
        handlerWrapper("CREATE DATABASE newdb;");
        handlerWrapper("USE newdb;");
        handlerWrapper("CREATE TABLE newtable (animals, colour, number);");

        String query = "INSERT INTO newtable VALUES (lion, gold, 1);";
        handlerWrapper(query);
        handlerWrapper("INSERT INTO newtable VALUES (dolphin, grey, 2);");
        handlerWrapper("INSERT INTO newtable VALUES (sheep, white, 3);");
        handlerWrapper("INSERT INTO newtable VALUES (cat, black, 4);");

        handlerWrapper("SELECT * FROM newtable;");

        handlerWrapper("UPDATE newtable SET number = 20 WHERE animals == 'lion';");
        handlerWrapper("SELECT * FROM newtable;");
    }

    /*
    @Test
    public void handleDeleteTest() {
        handlerWrapper("USE peopledb;");
        handlerWrapper("INSERT INTO people_copy VALUES ('Martin', martin@apple.net, 65);");
        DBTable peopleTable;
        try {
            peopleTable = new DBTable("/home/pollylang/MScBristol/OOPwithJava/JavaProjects/db/cw-db/databases/peopledb/people_copy.tab");
        } catch (DBException error) {
            error.printStackTrace();
            return;
        }
        assertEquals(peopleTable.getTableValue(peopleTable.getColumnNames().indexOf("name"), peopleTable.getTableData().size()-1), "Martin", "Error: insertion of 'Martin' record failed.");

        // TODO: not implemented in QueryHandler yet
       String query = "DELETE FROM people_copy WHERE name LIKE 'Martin;";
       handlerWrapper(query);
       assertFalse(peopleTable.getTableValue(peopleTable.getColumnNames().indexOf("name"), peopleTable.getTableData().size()-1).equals("Martin"), "Error: deletion of 'Martin' record failed.");
    }

     */

    @Test
    public void handleDeleteTest() {
        handlerWrapper("DROP DATABASE newdb;");
        handlerWrapper("CREATE DATABASE newdb;");
        handlerWrapper("USE newdb;");
        handlerWrapper("CREATE TABLE newtable (animals, colour, number);");

        String query = "INSERT INTO newtable VALUES (lion, gold, 1);";
        handlerWrapper(query);
        handlerWrapper("INSERT INTO newtable VALUES (dolphin, grey, 2);");
        handlerWrapper("INSERT INTO newtable VALUES (sheep, white, 3);");
        handlerWrapper("INSERT INTO newtable VALUES (cat, black, 4);");

        handlerWrapper("SELECT * FROM newtable;");

        handlerWrapper("DELETE FROM newtable WHERE animals == 'lion';");
        handlerWrapper("SELECT * FROM newtable;");
    }

    @Test
    public void handleJoinTest() {
        handlerWrapper("USE testdb;");
        handlerWrapper("DROP TABLE pets;");
        handlerWrapper("DROP TABLE ages;");

        handlerWrapper("CREATE TABLE pets (name, pet);");
        handlerWrapper("CREATE TABLE ages (name, age);");
        handlerWrapper("INSERT INTO pets VALUES (Bob, mouse);");
        handlerWrapper("INSERT INTO pets VALUES (Amy, tortoise);");
        handlerWrapper("INSERT INTO pets VALUES (Jane, dog);");
        handlerWrapper("INSERT INTO pets VALUES (Dylan, cat);");
        handlerWrapper("INSERT INTO ages VALUES (Jane, 65);");
        handlerWrapper("INSERT INTO ages VALUES (Amy, 54);");
        handlerWrapper("INSERT INTO ages VALUES (Dylan, 11);");
        handlerWrapper("INSERT INTO ages VALUES (Nick, 71);");

        String query = "JOIN pets AND ages ON name AND name;";
        handlerWrapper(query);
        System.out.println(handler.getResponseCode());
        System.out.println(handler.getResponseMessage());

        //handlerWrapper("DROP TABLE pets;");
        //handlerWrapper("DROP TABLE ages;");
    }

}
