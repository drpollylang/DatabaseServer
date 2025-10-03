package edu.uob;

import java.util.*;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class QueryParserTests {
    private QueryTokeniser tokeniser;
    private QueryParser parser;

    // Create a new server _before_ every @Test
    @BeforeEach
    public void setup() {
        tokeniser = new QueryTokeniser();
    }

    @Test
    public void parseQueryUse() {
        ArrayList<Token> tokens = tokeniser.getTokens("USE testdb;");
        parser = new QueryParser(tokens);
        try {
            parser.parseQuery();
        } catch (DBException error) {
            error.printStackTrace();
        }
    }

    /*
    // TODO: invalid queries
    @Test
    public void parseQueryUseInvalid() {
        ArrayList<Token> tokens = tokeniser.getTokens("USE testdb"); // no terminator
        parser = new QueryParser(tokens);
        parser.parseQuery();
    }

    @Test
    public void parseQueryUseInvalid2() {
        ArrayList<Token> tokens = tokeniser.getTokens("US testdb;"); // incorrectly spelled USE
        parser = new QueryParser(tokens);
        parser.parseQuery();
    }

    @Test
    public void parseQueryUseInvalid3() {
        ArrayList<Token> tokens = tokeniser.getTokens("USE database;"); // invalid identifier (keyword)
        parser = new QueryParser(tokens);
        parser.parseQuery();
    }

    @Test
    public void parseQueryUseInvalid3() {
        ArrayList<Token> tokens = tokeniser.getTokens("USE;"); // invalid syntax
        parser = new QueryParser(tokens);
        parser.parseQuery();
    }
     */

    @Test
    public void parseQueryCreateDatabase() {
        ArrayList<Token> tokens = tokeniser.getTokens("CREATE DATABASE testdb;");
        parser = new QueryParser(tokens);
        try {
            parser.parseQuery();
        } catch (DBException error) {
            error.printStackTrace();
        }
    }

    @Test
    public void parseQueryCreateTable() {
        ArrayList<Token> tokens = tokeniser.getTokens("CREATE TABLE table1;");
        parser = new QueryParser(tokens);
        try {
            parser.parseQuery();
        } catch (DBException error) {
            error.printStackTrace();
        }

        ArrayList<Token> tokens2 = tokeniser.getTokens("CREATE TABLE table1 (id, Name, Age);");
        parser = new QueryParser(tokens2);
        try {
            parser.parseQuery();
        } catch (DBException error) {
            error.printStackTrace();
        }
    }

    @Test
    public void parseQueryDropDatabase() {
        ArrayList<Token> tokens = tokeniser.getTokens("DROP DATABASE testdb;");
        parser = new QueryParser(tokens);
        try {
            parser.parseQuery();
        } catch (DBException error) {
            error.printStackTrace();
        }
    }

    @Test
    public void parseQueryDropTable() {
        ArrayList<Token> tokens = tokeniser.getTokens("DROP TABLE table1;");
        parser = new QueryParser(tokens);
        try {
            parser.parseQuery();
        } catch (DBException error) {
            error.printStackTrace();
        }
    }

    @Test
    public void parseQueryAlterTable() {
        ArrayList<Token> tokens = tokeniser.getTokens("ALTER TABLE table1 DROP name;");
        parser = new QueryParser(tokens);
        try {
            parser.parseQuery();
        } catch (DBException error) {
            error.printStackTrace();
        }

        tokens = tokeniser.getTokens("ALTER TABLE table1 ADD name;");
        parser = new QueryParser(tokens);
        try {
            parser.parseQuery();
        } catch (DBException error) {
            error.printStackTrace();
        }
    }

    @Test
    public void parseQueryInsert() {
        ArrayList<Token> tokens = tokeniser.getTokens("INSERT INTO table1 VALUES ('Simon', 65, TRUE);");
        //for (Token token : tokens) {
        //    System.out.println(token.getTokenValue());
        //}
        parser = new QueryParser(tokens);
        try {
            parser.parseQuery();
        } catch (DBException error) {
            error.printStackTrace();
        }
    }

    @Test
    public void parseQuerySelect() {
        ArrayList<Token> tokens = tokeniser.getTokens("SELECT * FROM table1;");
        parser = new QueryParser(tokens);
        try {
            parser.parseQuery();
        } catch (DBException error) {
            error.printStackTrace();
        }

        tokens = tokeniser.getTokens("SELECT id, name, age FROM table1;");
        parser = new QueryParser(tokens);
        try {
            parser.parseQuery();
        } catch (DBException error) {
            error.printStackTrace();
        }

        tokens = tokeniser.getTokens("SELECT id, name, age FROM table1 WHERE age > 18;");
        parser = new QueryParser(tokens);
        try {
            parser.parseQuery();
        } catch (DBException error) {
            error.printStackTrace();
        }

        tokens = tokeniser.getTokens("SELECT id, name, age FROM table1 WHERE (age > 18) AND (name LIKE 'Barry');");
        parser = new QueryParser(tokens);
        try {
            parser.parseQuery();
        } catch (DBException error) {
            error.printStackTrace();
        }
    }

    @Test
    public void parseQueryUpdate() {
        ArrayList<Token> tokens = tokeniser.getTokens("UPDATE table1 SET age = 22 WHERE name == 'Chris';");
        parser = new QueryParser(tokens);
        try {
            parser.parseQuery();
        } catch (DBException error) {
            error.printStackTrace();
        }

        tokens = tokeniser.getTokens("UPDATE table1 SET age = 22, name = Bob WHERE name == 'Chris';");
        parser = new QueryParser(tokens);
        try {
            parser.parseQuery();
        } catch (DBException error) {
            error.printStackTrace();
        }

        tokens = tokeniser.getTokens("UPDATE table1 SET age = 22, name = Bob WHERE name == 'Chris' AND age < 18;");
        parser = new QueryParser(tokens);
        try {
            parser.parseQuery();
        } catch (DBException error) {
            error.printStackTrace();
        }
    }

    @Test
    public void parseQueryDelete() {
        ArrayList<Token> tokens = tokeniser.getTokens("DELETE FROM table1 WHERE name == 'Chris';");
        parser = new QueryParser(tokens);
        try {
            parser.parseQuery();
        } catch (DBException error) {
            error.printStackTrace();
        }

    }

    @Test
    public void parseQueryJoin() {
        ArrayList<Token> tokens = tokeniser.getTokens("JOIN table1 AND table2 ON submission AND id;");
        parser = new QueryParser(tokens);
        try {
            parser.parseQuery();
        } catch (DBException error) {
            error.printStackTrace();
        }
    }
}
