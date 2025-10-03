package edu.uob;

import java.util.*;
import edu.uob.Token.TargetType;
import java.io.File;
import edu.uob.DBException.*;

public class DropCMD extends DBcmd {

    public DropCMD(ArrayList<Token> tokenStream) {
        this.tokenStream = tokenStream;
    }


    public void parseCommand() throws DBException {
        // <Drop> ::= "DROP" "DATABASE" [DatabaseName] | "DROP" "TABLE" [TableName]
        checkQueryLength(4);
        checkIdentifier(2);

        if (this.tokenStream.get(1).getTokenValue().equals("database")) {
            this.targetType = TargetType.DATABASE;
            this.parseDropDatabase();
        }
        else if (this.tokenStream.get(1).getTokenValue().equals("table")) {
            this.targetType = TargetType.TABLE;
            this.parseDropTable();
        }
        else {
            throw new DBException("Malformed query. The second word of a DROP query should be either " +
                    "DATABASE or TABLE, but you input " + this.tokenStream.get(1).getTokenValue());
        }
    }


    private void parseDropDatabase() {
        setDatabaseName(this.tokenStream.get(2).getTokenValue());
    }


    private void parseDropTable() {
        this.tableName = this.tokenStream.get(2).getTokenValue();
        this.tablePath = this.getFilePath(this.tableName);
    }


    public String handleQuery() {
        if (this.targetType == TargetType.DATABASE) {
            return deleteDatabase();
        }
        else if (this.targetType == TargetType.TABLE) {
            return deleteTable();
        }
        else {
            return new DBException("Sorry, something was wrong with your query. " +
                    "DROP must be followed by either DATABASE or TABLE.").getMessage();
        }
    }


    // Drop a database
    public String deleteDatabase() {
        databaseName = databaseName.toLowerCase();
        File databaseToBeDeleted;

        // If database doesn't exist, you can't delete it
        databaseToBeDeleted = new File(databasePath);
        if (!databaseToBeDeleted.exists()) return new databaseDoesNotExistException(databaseName).getMessage();

        for (File file : databaseToBeDeleted.listFiles()) {
            if (!file.delete()) {
                return new DBException("Failed to delete one or more files in the database.").getMessage();
            }
        }
       if (!databaseToBeDeleted.delete()) {
           return new DBException("Failed to delete " + databaseName + ".").getMessage();
       }
       return "[OK]";
    }


    // Drop a database table
    public String deleteTable() {
        // Check that the database and table exist
        File database = new File(databasePath);
        File tableToBeDeleted = new File(this.tablePath);
        if (!database.exists()) return new databaseDoesNotExistException(databaseName).getMessage();
        if (!tableToBeDeleted.exists()) return new tableDoesNotExistException(this.tableName).getMessage();
        // Delete metadata
        try {
            DBTable droppedTableData = new DBTable(this.tablePath);
            droppedTableData.deleteMetadataRow();
        } catch (DBException exception) {
            return exception.getMessage();
        }
        if (!tableToBeDeleted.delete()) {
            return new DBException("Failed to delete table " + this.tableName +
                    ". Do you have permission to delete this file?").getMessage();
        }
        return "[OK]";
    }
}
