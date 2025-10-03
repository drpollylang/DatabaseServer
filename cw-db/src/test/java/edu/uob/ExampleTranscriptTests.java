package edu.uob;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ExampleTranscriptTests {
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
    public void exampleTranscriptTests() {
        sendCommandToServer("DROP dATABASE markbook;");
        String response;
        response = sendCommandToServer("CREATE DATABASE markbook;");
        System.out.println(response);
        response = sendCommandToServer("USE markbook;");
        System.out.println(response);
        response = sendCommandToServer("CREATE TABLE marks (name, mark, pass);");
        System.out.println(response);
        response = sendCommandToServer("INSERT INTO marks VALUES ('Simon', 65, TRUE);");
        System.out.println(response);
        response = sendCommandToServer("INSERT INTO marks VALUES ('Sion', 55, TRUE);");
        System.out.println(response);
        response = sendCommandToServer("INSERT INTO marks VALUES ('Rob', 35, FALSE);");
        System.out.println(response);
        response = sendCommandToServer("INSERT INTO marks VALUES ('Chris', 20, FALSE);");
        System.out.println(response);
        response = sendCommandToServer("SELECT * FROM marks;");
        System.out.println(response);
        response = sendCommandToServer("SELECT * FROM marks WHERE name != 'Sion';");
        System.out.println(response);
        response = sendCommandToServer("SELECT * FROM marks WHERE pass == TRUE;");
        System.out.println(response);
        response = sendCommandToServer("CREATE TABLE coursework (task, submission);");
        System.out.println(response);
        response = sendCommandToServer("INSERT INTO coursework VALUES (OXO, 3);");
        System.out.println(response);
        response = sendCommandToServer("INSERT INTO coursework VALUES (DB, 1);");
        System.out.println(response);
        response = sendCommandToServer("INSERT INTO coursework VALUES (OXO, 4);");
        System.out.println(response);
        response = sendCommandToServer("INSERT INTO coursework VALUES (STAG, 2);");
        System.out.println(response);
        response = sendCommandToServer("JOIN coursework AND marks ON submission AND id;");
        System.out.println(response);
        response = sendCommandToServer("UPDATE marks SET mark = 38 WHERE name == 'Chris';");
        System.out.println(response);
        response = sendCommandToServer("SELECT * FROM marks WHERE name == 'Chris';");
        System.out.println(response);
        response = sendCommandToServer("DELETE FROM marks WHERE name == 'Sion';");
        System.out.println(response);
        response = sendCommandToServer("SELECT * FROM marks;");
        System.out.println(response);
        response = sendCommandToServer("SELECT * FROM marks WHERE (pass == FALSE) AND (mark > 35);");
        System.out.println(response);
        response = sendCommandToServer("SELECT * FROM marks WHERE name LIKE 'i';");
        System.out.println(response);
        response = sendCommandToServer("SELECT id FROM marks WHERE pass == FALSE;");
        System.out.println(response);
        response = sendCommandToServer("SELECT name FROM marks WHERE mark>60;");
        System.out.println(response);
        response = sendCommandToServer("DELETE FROM marks WHERE mark<40;");
        System.out.println(response);
        response = sendCommandToServer("SELECT * FROM marks;");
        System.out.println(response);
        response = sendCommandToServer("ALTER TABLE marks ADD age;");
        System.out.println(response);
        response = sendCommandToServer("SELECT * FROM marks;");
        System.out.println(response);
        response = sendCommandToServer("UPDATE marks SET age = 35 WHERE name == 'Simon';");
        System.out.println(response);
        response = sendCommandToServer("SELECT * FROM marks;");
        System.out.println(response);
        response = sendCommandToServer("ALTER TABLE marks DROP pass;");
        System.out.println(response);
        response = sendCommandToServer("SELECT * FROM marks;");
        System.out.println(response);
        response = sendCommandToServer("SELECT * FROM marks");
        System.out.println(response);
        response = sendCommandToServer("SELECT * FROM crew;");
        System.out.println(response);
        response = sendCommandToServer("SELECT height FROM marks WHERE name == 'Chris';");
        System.out.println(response);
        response = sendCommandToServer("DROP TABLE marks;");
        System.out.println(response);
        response = sendCommandToServer("DROP DATABASE markbook;");
        System.out.println(response);
    }
}
