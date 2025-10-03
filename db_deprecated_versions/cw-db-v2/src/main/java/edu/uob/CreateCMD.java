package edu.uob;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import edu.uob.Token.*;

public class CreateCMD extends DBcmd {
    private TargetType targetType;
    private List<String> attributeList;
    private String tableName;
    private String tablePath;

    public CreateCMD(TargetType targetType, String targetName, List<String> attributeList) {
        this.targetType = targetType;
        if (targetType == Token.TargetType.DATABASE) {
            setDatabaseName(targetName);
        }
        else {
            this.tableName = targetName;
            this.tablePath = getFilePath(databaseName, tableName);
            this.attributeList = attributeList;
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


    private void addNewColumns(DBTable newTable) throws DBException {
        for (String s : this.attributeList) {
            // If 'id' is in the attributeList, just quietly skip it
            if (!s.equals("id")) {
                newTable.addColumn(s);
            }
        }
    }


    private void isFilepathValid() throws DBException {
        // Check that database exists
        if (!checkFileExists(databasePath)) {
            throw new DBException("Sorry, you cannot create a table in a database that does not exist.");
        }

        // Check if table already exists
        if (checkFileExists(this.tablePath)) {
            throw new DBException("This table already exists! " +
                    "Please choose a different name for your new table.");
        }
    }


    private void writeMetadata() throws IOException {
        String metadataPath = databasePath + File.separator + "metadata.tab";
        // Add a new row to the metadata file. idCounter initialised to 1
        appendToFile(metadataPath, this.tableName + "\t1");
    }


    private void writeTable() throws DBException {
        DBTable newTable;
        newTable = new DBTable(this.tablePath);
        // Automatically add an 'id' column
        newTable.addColumn("id");
        // If there are any column names, write them to file
        this.addNewColumns(newTable);
        // Write new table to file system
        newTable.writeDataToFile();
    }
}