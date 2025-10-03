package edu.uob;

import java.io.*;
import edu.uob.DBException.*;

public class UseCMD extends DBcmd {
    private String previousDatabaseName;

    public UseCMD(String dbName) {
        this.previousDatabaseName = databaseName;
        setDatabaseName(dbName);
    }


    public String handleQuery() {
        // Check that database exists. If not, return error message and reset current database to previous
        if (!new File(databasePath).exists()) {
            setDatabaseName(this.previousDatabaseName);
            return new databaseDoesNotExistException(databaseName).getMessage();
        }
        return "[OK]";
    }
}
