package edu.uob;

import java.io.*;
import java.nio.file.Paths;
import java.util.ArrayList;
import edu.uob.DBException.*;

public abstract class DBcmd {
    protected static String databaseName;
    protected static String databasePath;

    public DBcmd() {
    }

    public abstract String handleQuery();


    public void setDatabaseName(String dbName) {
        databaseName = dbName;
        databasePath = getFilePath(databaseName, null);
    }


    // Utility functions
    // Convert database name and table name to a filepath that can be used
    // to instantiate a DBTable
    protected static String getFilePath(String databaseName, String tableName) {
        String storageFolderPath = Paths.get("databases").toAbsolutePath().toString();
        String filePath = storageFolderPath + File.separator + databaseName;
        if (tableName != null) {
            return filePath + File.separator + tableName + ".tab";
        }
        return filePath;
    }


    protected static boolean checkFileExists(String filePath) {
        File fileToBeChecked = new File(filePath);
        return fileToBeChecked.exists();
    }


    // Append text onto an existing file (used to write new table rows to metadata file)
    protected void appendToFile(String filePath, String text) throws IOException {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, true));
            writer.write("\n");
            writer.write(text);
            writer.close();
        } catch (IOException e) {
            throw new IOException("Failed to write to file" + filePath);
        }
    }


    protected static DBTable instantiateDatabaseTable(String tableName, String tablePath)
            throws DBException {
        if (!(new File(tablePath).exists())) {
            throw new tableDoesNotExistException(tableName);
        }
        return new DBTable(tablePath);
    }


    // Returns a list of the ids of columns to be deleted/updated (where condition is true)
    protected static ArrayList<String> filterTableByCondition(DBTable databaseTable, Condition condition)
            throws DBException {
        ArrayList<String> filteredDataIds = databaseTable.filterRows(condition);
        // If WHERE condition is never true, i.e. the above returns an empty list, error
        if (filteredDataIds.isEmpty()) {
            throw new DBException("No rows in the table matched the condition. Nothing was changed.");
        }
        return filteredDataIds;
    }
}
