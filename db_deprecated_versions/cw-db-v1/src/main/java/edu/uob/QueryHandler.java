package edu.uob;

import java.io.*;
import java.util.*;
import java.util.stream.*;
import java.nio.file.Paths;
import java.nio.file.Files;

import edu.uob.DBException.*;
import edu.uob.Token.*;

// TODO: finish implementing handler methods (manipulate table data according to query and return
//  response code and message)

// TODO: ensure compatibility with DBTable.
// TODO automated script testing and error handling

public class QueryHandler {

    private QueryTokeniser queryTokeniser;
    private QueryParser queryParser;
    private ParsedQuery parsedQuery;
    private DBTable databaseTable;
    private DBTable databaseTable2;
    private String databasePath;
    private String tablePath;
    private String tablePath2;
    private static String databaseName;
    // Outputs
    private String responseCode;
    private String responseMessage; // either a table (formatted as String) or an error message

    public QueryHandler(String query) throws DBException {
        // Instantiate tokeniser and parser. Pass query through both
        try {
            this.queryTokeniser = new QueryTokeniser();
            this.queryParser = new QueryParser(this.queryTokeniser.getTokens(query));
            this.queryParser.parseQuery();
        } catch (DBException exception) {
            this.responseCode = "[ERROR]";
            this.responseMessage = exception.getMessage();
            return;
        }
        // Get ParsedQuery from QueryParser and assign it to ParsedQuery
        this.parsedQuery = queryParser.getParsedQuery();

        // (error handling on static variable databaseName - error handling if the databaseName has not yet been defined)
        if (this.parsedQuery.getDatabaseName() == null && databaseName == null) {
            throw new noDatabaseSelectedException();
        } else if (this.parsedQuery.getDatabaseName() != null) {
            databaseName = this.parsedQuery.getDatabaseName();
        }
        this.databasePath = getFilePath(databaseName, null);

        // Based on the database name and table name in the ParsedQuery, instantiate and read data into a new DBTable
        if (this.parsedQuery.getTableName1() != null) {
            this.tablePath = getFilePath(databaseName, this.parsedQuery.getTableName1());
            this.databaseTable = new DBTable(this.tablePath);
        }

        if (this.parsedQuery.getTableName2() != null) {
            this.tablePath2 = getFilePath(databaseName, this.parsedQuery.getTableName2());
            this.databaseTable2 = new DBTable(this.tablePath2);
        }

        // Call a table operation method on the data, according to the commandType attribute of ParsedQuery
        // Each control flow path (whether valid response or error) updates the response code and message)
        if (this.parsedQuery.getCommandType() == null) {
            this.responseCode = "[ERROR]";
            this.responseMessage = "Something went wrong with parsing the type of command. Are you sure the query is formatted correctly?";
        } else {
            switch (this.parsedQuery.getCommandType()) {
                case USE:
                    this.handleUse();
                    break;
                case CREATE:
                    this.handleCreate();
                    break;
                case DROP:
                    this.handleDrop();
                    break;
                case ALTER:
                    this.handleAlter();
                    break;
                case INSERT:
                    this.handleInsert();
                    break;
                case SELECT:
                    this.handleSelect();
                    break;
                case UPDATE:
                    this.handleUpdate();
                    break;
                case DELETE:
                    this.handleDelete();
                    break;
                case JOIN:
                    this.handleJoin();
                    break;
                default:
                    this.responseCode = "[ERROR]";
                    this.responseMessage = "Sorry, there was a problem with your query. Invalid command type.";
            }
        }

    }


    private void handleUse() {
        // Switching database name already taken care of in the constructor
        this.responseCode = "OK";
        this.responseMessage = "";
    }

    private void handleCreate() {
        if (this.parsedQuery.getTargetType() == TargetType.DATABASE) {
            this.handleCreateDatabase();
        }
        else if (this.parsedQuery.getTargetType() == TargetType.TABLE) {
            this.handleCreateTable();
        }
        else {
            this.responseCode = "[ERROR]";
            this.responseMessage = "Sorry, something was wrong with your query. CREATE must be followed by either DATABASE or TABLE.";
        }
    }

    private void handleCreateDatabase() {
        this.createDatabase();

    }

    private void handleCreateTable() {
        if (this.parsedQuery.getTableName1() == null) {
            this.responseCode = "[ERROR]";
            this.responseMessage = "No table name in query. Please provide a name for the table you are trying to create";
        }
        else {
            this.tablePath = getFilePath(databaseName, this.parsedQuery.getTableName1());
            this.createDBTable();
        }
    }

    private void handleDrop() {
        if (this.parsedQuery.getTargetType() == TargetType.DATABASE) {
            this.handleDropDatabase();
        }
        else if (this.parsedQuery.getTargetType() == TargetType.TABLE) {
            this.handleDropTable();
        }
        else {
            this.responseCode = "[ERROR]";
            this.responseMessage = "Sorry, something was wrong with your query. DROP must be followed by either DATABASE or TABLE.";
        }
    }

    private void handleDropDatabase() {
        this.deleteDatabase();
    }

    private void handleDropTable() {
        this.deleteDBTable();
    }

    private void handleAlter() {
        if (this.parsedQuery.getAlterationType() == AlterationType.ADD) {
            try {
                this.databaseTable.addColumn(this.parsedQuery.getAttributeList().get(0));
                this.databaseTable.writeDataToFile();
                this.responseCode = "[OK]";
                this.responseMessage = "";
            } catch (Exception error) {
                this.responseCode = "[ERROR]";
                this.responseMessage = "Oops! Something went wrong when adding column. Error: " + error.getMessage();
            }
        }
        //else if (this.parsedQuery.getAlterationType() == AlterationType.DROP) {
        else {
            try {
                this.databaseTable.dropColumn(this.parsedQuery.getAttributeList().get(0));
                this.databaseTable.writeDataToFile();
                this.responseCode = "[OK]";
                this.responseMessage = "";
            } catch (DBException exception) {
                this.responseCode = "[ERROR]";
                this.responseMessage = "Oops! Something went wrong when adding column. Error: " + exception.getMessage();
            }
        }
        /*
        else {
            this.responseCode = "[ERROR]";
            this.responseMessage = "Sorry, that is not a valid alteration type. Valid alteration types are ADD or DROP.";
            return;
        }
        responseCode = "[OK]";
        responseMessage = "";

         */
    }

    private void handleInsert() {
        // Data validation: ValueList must contain the same number of values as the table contains
        // columns. If not, error.
        ArrayList<String> valueList = this.parsedQuery.getValueList();
        try {
            this.databaseTable.insertRowOfData(valueList);
            this.databaseTable.writeDataToFile();
            this.responseCode = "[OK]";
            this.responseMessage = "";
        } catch (DBException exception) {
            this.responseCode = "[ERROR]";
            this.responseMessage = exception.getMessage();
        }
    }

    private void handleSelect() {
        // TODO: Filter columns by column name (unless *, then return whole table)
        ArrayList<ArrayList<String>> selectData;

        // Handle WHERE condition (if there is one)
        // Do this first, because you may want to use conditions on columns
        // that aren't returned in filtered table.
        if (this.parsedQuery.getCondition() != null) {
            try {
                this.databaseTable.filterTableByCondition(this.parsedQuery.getCondition());
            } catch (DBException error) {
                this.responseCode = "[ERROR]";
                this.responseMessage = "Sorry, something went wrong when handling the condition. Are you sure the syntax is valid?";
                return;
            }
        }
        List<String> selectColumns = this.parsedQuery.getAttributeList();
        if (selectColumns.isEmpty()) {
            this.emptyTable();
            return;
        }
        // If WildAttribute is NOT *, filter table by selected columns (attribute
        // names) before proceeding.
        if (!(selectColumns.size() == 1 && selectColumns.get(0).equals("*"))) {
            try {
                this.selectFilterColumns();
            } catch (DBException exception) {
                this.responseCode = "[ERROR]";
                this.responseMessage = exception.getMessage();
                System.out.println(this.responseCode);
                System.out.println(this.responseMessage);
                return;
            }
        }

        if (this.databaseTable.getTableData().isEmpty()) {
            this.emptyTable();
            return;
        }

        // Everything went fine, so return response code/message and printable table
        this.responseCode = "[OK]";
        this.responseMessage = this.databaseTable.toString();

        System.out.println(this.responseCode);
        System.out.println(this.responseMessage);
    }

    private void emptyTable() {
        this.responseCode = "[OK]";
        this.responseMessage = "";
    }


    private void selectFilterColumns() throws DBException {
        List<String> selectColumns = this.parsedQuery.getAttributeList();
        List<Integer> selectColumnIndices = new ArrayList<>();
        ArrayList<String> columnNames = this.databaseTable.getColumnNames();
        for (String column : selectColumns) {
            int index = columnNames.indexOf(column);
            if (index >= 0) {
                selectColumnIndices.add(index);
            }
        }
        if (selectColumnIndices.isEmpty()) {
            throw new DBException("None of the selected columns exist in this table.");
        }
        this.databaseTable.filterColumnsByIndex(selectColumnIndices);
    }

    private void handleUpdate() {
        ArrayList<NameValuePair> nameValueList = this.parsedQuery.getNameValueList();
        Condition condition = this.parsedQuery.getCondition();

        // Apply condition - get ids of the rows the condition applies to
        ArrayList<String> filteredDataIds;
        try {
            filteredDataIds = this.databaseTable.getRowIndexWhereConditionTrue(condition);
        } catch (DBException error) {
            this.responseCode = "[ERROR]";
            this.responseMessage = "Something went wrong when applying the WHERE condition. Error message: " + error.getMessage();
            return;
        }
        // Set new values (according to name-value pairs) for rows where condition is true
        int idColumnIndex = this.databaseTable.getColumnNames().indexOf("id");
        try {
            for (NameValuePair nameValuePair : nameValueList) {
                for (int rowIndex = 0; rowIndex < this.databaseTable.getNumberOfRows(); rowIndex++) {

                    if (filteredDataIds.contains(this.databaseTable.getTableValue(idColumnIndex, rowIndex))) {
                        this.updateValue(nameValuePair, rowIndex);
                    }
                }
            }
        } catch (DBException error) {
            this.responseCode = "[ERROR]";
            this.responseMessage = "Failed to update table. Error message: " + error.getMessage();
        }
        this.databaseTable.writeDataToFile();
        this.responseCode = "[OK]";
        this.responseMessage = "";
    }

    private void updateValue(NameValuePair nameValuePair, int rowIndex) throws DBException {
        String attributeName = nameValuePair.getAttributeName();
        String value = nameValuePair.getValue();
        int columnIndex = this.databaseTable.getColumnNames().indexOf(attributeName);
        if (columnIndex == -1) {
            throw new DBException("Column name " + attributeName + " does not exist within this table");
        }
        this.databaseTable.setTableValue(columnIndex, rowIndex, value);
    }

    private void handleDelete() {
        Condition condition = this.parsedQuery.getCondition();
        // Apply condition - get ids of the rows the condition applies to
        ArrayList<String> filteredDataIds;
        try {
            filteredDataIds = this.databaseTable.getRowIndexWhereConditionTrue(condition);
        } catch (DBException error) {
            this.responseCode = "[ERROR]";
            this.responseMessage = "Something went wrong when applying the WHERE condition. Error message: " + error.getMessage();
            return;
        }
        // Delete all rows where condition is true and write results to filesystem
        int idColumnIndex = this.databaseTable.getColumnNames().indexOf("id");
        try {
            for (int rowIndex = 0; rowIndex < this.databaseTable.getNumberOfRows(); rowIndex++) {
                if (filteredDataIds.contains(this.databaseTable.getTableValue(idColumnIndex, rowIndex))) {
                    this.databaseTable.deleteTableRow(rowIndex);
                }
            }
        } catch (Exception error) {
            this.responseCode = "[ERROR]";
            this.responseMessage = "Failed to delete rows. Error message: " + error.getMessage();
        }
        this.databaseTable.writeDataToFile();
        this.responseCode = "[OK]";
        this.responseMessage = "";
    }

    private void handleJoin() {
        int joinColIndex1 = this.databaseTable.getColumnNames().indexOf(this.parsedQuery.getJoinAttribute1());
        int joinColIndex2 = this.databaseTable2.getColumnNames().indexOf(this.parsedQuery.getJoinAttribute2());
        if (joinColIndex1 == -1 || joinColIndex2 == -1) {
            this.responseCode = "[ERROR]";
            this.responseMessage = "Something went wrong while joining. One or more of the joining columns does not exist in their respective tables.";
            return;
        }
        // attribute names are prepended with name of table from which they originated
        try {
            this.prependColumnNames();
        } catch (DBException error) {
            this.responseCode = "[ERROR]";
            this.responseMessage = "Something went wromg. " + error.getMessage();
            return;
        }


        try {
            this.databaseTable.joinTable(this.databaseTable2, joinColIndex1, joinColIndex2);
        } catch (DBException error) {
            this.responseCode = "ERROR";
            this.responseMessage = "Something went wrong while joining. " + error;
            return;
        }
        this.responseCode = "[OK]";
        this.responseMessage = this.databaseTable.toString();
    }

    private void prependColumnNames() throws DBException {
        // attribute names are prepended with name of table from which they originated
        for (String columnName : this.databaseTable.getColumnNames()) {
            try {
                this.databaseTable.editColumnName(columnName, this.databaseTable.getTableName() + "." + columnName);
            } catch (Exception error) {
                throw new DBException("Failed to prepend column " + columnName + " : " + error.getMessage());
            }
        }

        for (String columnName : this.databaseTable2.getColumnNames()) {
            try {
                this.databaseTable2.editColumnName(columnName, this.databaseTable2.getTableName() + "." + columnName);
            } catch (Exception error) {
                throw new DBException("Failed to prepend column " + columnName + " : " + error.getMessage());
            }
        }
    }


    // Utility functions
    // Convert database name and table name to a filepath that can be used
    // to instantiate a DBTable
    private static String getFilePath(String databaseName, String tableName) {
        String storageFolderPath = Paths.get("databases").toAbsolutePath().toString();
        String filePath = storageFolderPath + File.separator + databaseName;
        if (tableName != null) {
            return filePath + File.separator + tableName + ".tab";
        }
        return filePath;
    }

    private static boolean checkFileExists(String filePath) {
        File fileToBeChecked = new File(filePath);
        return fileToBeChecked.exists();
    }


    // Create a database
    //public String createDatabase(String databaseName) {
    public void createDatabase() {
        //databaseName = databaseName.toLowerCase();
        //this.setCurrentFilePath(databaseName, null);
        if (new File(this.databasePath).exists()) {
            this.responseCode = "[ERROR]";
            this.responseMessage = "Sorry, this database already exists. You cannot create another database with the same name.";
            return;
        }
        try {
            // Create the database storage folder if it doesn't already exist !
            // Create a new metadata file
            Files.createDirectories(Paths.get(this.databasePath));
            String metadataPath = this.databasePath + File.separator + "metadata.txt";
            File newMetadata = new File(metadataPath);
            newMetadata.createNewFile();
            writeToFile(metadataPath, "1"); // idCounter initialised to 1
        } catch(IOException ioe) {
            this.responseCode = "[ERROR]";
            this.responseMessage = "Can't seem to create database folder " + this.databasePath;
            return;
        }
        this.responseMessage = "[OK]";
    }

    private void writeToFile(String filePath, String text) throws IOException {
        try {
            FileWriter writer = new FileWriter(filePath);
            writer.write(text);
            writer.close();
        } catch (IOException e) {
            throw new IOException("Failed to write to file" + filePath);
        }
    }

    // Drop a database
    //public String deleteDatabase(String databaseName) {
    public void deleteDatabase() {
        //databaseName = databaseName.toLowerCase();
        databaseName = databaseName.toLowerCase();
        //String databaseFilePath = storageFolderPath + File.separator + databaseName;

        File databaseToBeDeleted;
        File[] databaseContents;

        try {
            databaseToBeDeleted = new File(this.databasePath);
            databaseContents = databaseToBeDeleted.listFiles();
        } catch (Exception ioe) {
            this.responseCode = "[ERROR]";
            this.responseMessage = "Error: failed to open database " + databaseName + ". Has it already been deleted?";
            return;
        }

        try {
            for (File file : databaseContents) {
                file.delete();
            }
            databaseToBeDeleted.delete();
        } catch (Exception error) {
            this.responseCode = "[ERROR]";
            this.responseMessage = "Error: failed to delete one or more files in database. Error message: " + error;
            return;
        }
        this.responseCode = "[OK]";
        this.responseMessage = "";
    }


    // Create a database table
    //public String createDBTable(String databaseName, String tableName) {
    public void createDBTable() {
        //String filePath = this.getFilePath(databaseName, tableName);
        //String filePath = this.tablePath;

        File newTableFile = new File(this.tablePath);

        // Check that database exists
        //if (!this.checkFileExists(this.storageFolderPath + File.separator + databaseName)) {
        if (!checkFileExists(this.databasePath)) {
            this.responseCode = "[ERROR]";
            this.responseMessage = "Sorry, you cannot create a table in a database that does not exist.";
            return;
        }

        // Check if table already exists
        if (checkFileExists(this.tablePath + ".tab")) {
            this.responseCode = "[ERROR]";
            this.responseMessage = "This table already exists! Please choose a different name for your new table.";
            return;
        }
        // Create empty table - catch errors if permissions are not set
        try {
            newTableFile.createNewFile();
        } catch(IOException ioe) {
            this.responseCode = "[ERROR]";
            this.responseMessage = "Error: can't create database table " +
                    this.tablePath + ". Do you have permissions to create files?";
        }

        // Automatically add an 'id' column
        DBTable newTable;
        try {
            newTable = new DBTable(this.tablePath);
        } catch (DBException error) {
            this.responseCode = "[ERROR]";
            this.responseMessage = error.getMessage();
            return;
        }
        newTable.addColumn("id");

        // If there are any column names, write them to file
        for (int i = 0; i < this.parsedQuery.getAttributeList().size(); i++) {
            newTable.addColumn(this.parsedQuery.getAttributeList().get(i));
        }

        newTable.writeDataToFile();
        this.responseCode = "[OK]";
        this.responseMessage = "";

    }

    // Drop a database table
    //public String deleteDBTable(String databaseName, String tableName) {
    public void deleteDBTable() {
        // Check that the database and table exist
        File database = new File(this.databasePath);
        File tableToBeDeleted = new File(this.tablePath);
        if (!database.exists() || !tableToBeDeleted.exists()) {
            this.responseCode = "[ERROR]";
            this.responseMessage = "Sorry, you can't delete table  at " +
                    this.tablePath +
                    " because either the table or the database does not exist.";
            return;
        }
        try {
            tableToBeDeleted.delete();
            this.responseCode = "[OK]";
            this.responseMessage = "";
        } catch (Exception error) {
            this.responseCode = "[ERROR]";
            this.responseMessage = "Error: failed to delete table " +
                    this.parsedQuery.getTableName1() +
                    ". Do you have permission to delete this file? Error message: " + error;
        }
    }


    public String getResponseCode() {
        return this.responseCode;
    }

    public String getResponseMessage() {
        return this.responseMessage;
    }

}
