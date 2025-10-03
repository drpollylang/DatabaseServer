package edu.uob;

import java.io.*;
import java.util.ArrayList;

import edu.uob.DBException.*;

public class UseCMD extends DBcmd {
    private String previousDatabaseName;

    public UseCMD(ArrayList<Token> tokenStream) {
        this.tokenStream = tokenStream;
        this.previousDatabaseName = databaseName;
    }

    public void parseCommand() throws DBException {
        // <Use> ::= "USE" [DatabaseName]
        checkQueryLength(3);
        checkIdentifier(1);
        setDatabaseName(this.tokenStream.get(1).getTokenValue());
    }


    public String handleQuery() {
        // Check that database exists. If not, return error message and reset current database to previous
        if (!new File(databasePath).exists()) {
            if (this.previousDatabaseName != null) {
                setDatabaseName(this.previousDatabaseName);
            }
            return new databaseDoesNotExistException(databaseName).getMessage();
        }
        return "[OK]";
    }
}
