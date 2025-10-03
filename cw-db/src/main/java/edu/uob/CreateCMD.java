package edu.uob;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import edu.uob.Token.*;
import edu.uob.DBException.*;

public class CreateCMD extends DBcmd {
    public CreateCMD(ArrayList<Token> tokenStream) {
        this.tokenStream = tokenStream;
    }

    public void parseCommand() throws DBException {
        // <Create> ::= <>CreateDatabase> | <CreateTable>
        checkTokenType(1, TokenType.keyword);
        checkIdentifier(2);
        if (this.tokenStream.get(1).getTokenValue().equals("database")) {
            this.targetType = TargetType.DATABASE;
            this.parseCreateDatabase();
        }
        else if (this.tokenStream.get(1).getTokenValue().equals("table")) {
            this.targetType = TargetType.TABLE;
            this.parseCreateTable();
        }
        else {
            throw new DBException("Malformed query. The second word of a CREATE query should " +
                    "be either DATABASE or TABLE.");
        }
    }


    private void parseCreateDatabase() throws DBException {
        // <CreateDatabase> ::= "CREATE" "DATABASE" [DatabaseName]
        // Check that DatabaseName is a stringLiteral or plainText identifier
        checkQueryLength(4);
        setDatabaseName(this.tokenStream.get(2).getTokenValue());
    }


    private void parseCreateTable() throws DBException{
        // <CreateTable> ::= "CREATE" "TABLE" [TableName] | "CREATE" "TABLE" [TableName] "(" <AttributeList> ")"
        this.tableName = this.tokenStream.get(2).getTokenValue();
        this.tablePath = this.getFilePath(this.tableName);

        if (!(this.tokenStream.get(3).getTokenValue().equals(";"))) {
            // Includes attributes
            if (!(this.tokenStream.get(3).getTokenValue().equals("(") ||
                    this.tokenStream.get(this.tokenStream.size()-2).getTokenValue().equals(")"))) {
                throw new malformedQueryException();
            }
            this.parseAttributeList(this.tokenStream.subList(4, this.tokenStream.size()-2));
        }
    }



    public String handleQuery() {
        if (this.targetType == TargetType.DATABASE) {
            return this.createDatabase();
        }
        else if (this.targetType == TargetType.TABLE) {
            return this.createTable();
        }
        else {
            return new DBException("Sorry, something was wrong with your query. " +
                    "CREATE must be followed by either DATABASE or TABLE.").getMessage();
        }
    }


    // Create a database
    public String createDatabase() {
        if (new File(databasePath).exists()) {
            return new DBException("Sorry, this database already exists. " +
                    "You cannot create another database with the same name.").getMessage();
        }
        try {
            // Create the database storage folder if it doesn't already exist !
            // Create a new metadata file
            Files.createDirectories(Paths.get(databasePath));
            String metadataPath = databasePath + File.separator + "metadata.tab";
            File newMetadata = new File(metadataPath);
            newMetadata.createNewFile();
        } catch(IOException ioe) {
            return new DBException("Can't seem to create database folder " + databasePath).getMessage();
        }
        return "[OK]";
    }


    // Create a database table
    public String createTable() {
        File newTableFile = new File(this.tablePath);
        try {
            isFilepathValid();
            // Create empty table - catch errors if permissions are not set
            newTableFile.createNewFile();
            // Write a new row of metadata for this table to the metadata file
            this.writeMetadata();
            // Create a new table and write it to the file system
            this.writeTable();
        } catch (Exception error) {
            return error.getMessage();
        }
        return "[OK]";
    }


    private void addNewColumns() throws DBException {
        if (this.attributeList == null) return;
        for (String s : this.attributeList) {
            // If 'id' is in the attributeList, just quietly skip it
            if (!s.equals("id")) {
                this.databaseTable.addColumn(s);
            }
        }
    }


    private void isFilepathValid() throws DBException {
        // Check that database exists
        this.checkDatabaseExists();
        // Check if table already exists
        if (checkFileExists(this.tablePath)) {
            throw new DBException("This table already exists! " +
                    "Please choose a different name for your new table.");
        }
    }


    private void writeMetadata() throws DBException {
        String metadataPath = databasePath + File.separator + "metadata.tab";
        // Add a new row to the metadata file. idCounter initialised to 1
        appendToFile(metadataPath, this.tableName + "\t1");
    }


    private void writeTable() throws DBException {
        this.databaseTable = new DBTable(this.tablePath);
        // Automatically add an 'id' column
        this.databaseTable.addColumn("id");
        // If there are any column names, write them to file
        this.addNewColumns();
        // Write new table to file system
        this.databaseTable.writeDataToFile();
    }
}