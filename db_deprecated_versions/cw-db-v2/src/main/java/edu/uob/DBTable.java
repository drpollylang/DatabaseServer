package edu.uob;

import java.io.*;
import java.util.*;
import java.util.regex.*;

public class DBTable {
    private String tableName;
    private String tablePath;
    private String metadataPath;
    public Hashtable<String, Integer> metadata;
    private ArrayList<String> tableHeaders;
    private ArrayList<ArrayList<String>> tableData;
    public int idCounter;

    public DBTable(String tablePath) throws DBException {
        this.tablePath = tablePath;
        this.tableName = this.getTableName();
        this.metadataPath = tablePath.substring(0, tablePath.lastIndexOf(File.separator)) + File.separator + "metadata.tab";
        this.metadata = this.readMetadata();
        this.idCounter = this.metadata.get(this.tableName);
        this.tableHeaders = new ArrayList<>();
        this.tableData = new ArrayList<>();
        try {
            // Try to open and read table data - throw IOException if you can't
            this.readDataFromFile(tablePath);
        } catch(IOException ioe) {
            String[] tableNameSplit = this.tablePath.split(Pattern.quote(File.separator));
            String tableName = tableNameSplit[tableNameSplit.length-1].split("\\.t")[0];
            System.out.println("Can't seem to open table at " + tableName + ". Does this table exist?");
        }
    }


    private Hashtable<String, Integer> readMetadata() throws DBException {
        // Metadata is a text file metadata.tab.
        // It contains two tab-separated columns. The first column contains names of all of the
        // tables inside the database. The second contains the current idCounter value for that
        // table. This is represented within DBTable as a Hashmap, with table names as keys
        // and idCounter values as values.
        try {
            Hashtable<String, Integer> metadata = new Hashtable<>();
            File metadataFile = new File(this.metadataPath);
            if (!metadataFile.exists()) {
                throw new DBException("Metadata file for this database does not exist.");
            }
            BufferedReader metadataReader = new BufferedReader(new FileReader(metadataFile));
            String metadataLine;
            ArrayList<String> metadataValues;
            while ((metadataLine = metadataReader.readLine()) != null) {
                metadataValues = new ArrayList<>(Arrays.asList(metadataLine.split("\\t")));
                if (!(metadataValues.isEmpty() || metadataValues.get(0).isEmpty())) {
                    metadata.put(metadataValues.get(0), Integer.parseInt(metadataValues.get(1)));
                }
            }
            metadataReader.close();
            return metadata;
        } catch(IOException ioe) {
            throw new DBException("Failed to read metadata from " + this.metadataPath);
        }
    }


    // Delete a row in metadata
    public void deleteMetadataRow() throws DBException {
        for (int i = 0; i < this.metadata.size(); i++) {
            String key = new ArrayList<>(this.metadata.keySet()).get(i);
            if (key.equals(this.tableName)) {
                this.metadata.remove(key);
            }
        }
        this.writeMetadataToFile();
    }


    private void readDataFromFile(String filePath) throws IOException {
        File tableFile = new File(filePath);
        if (!tableFile.exists()) {
            throw new FileNotFoundException("File " + filePath + " not found.");
        }
        FileReader tableDataReader = new FileReader(tableFile);
        BufferedReader tableDataBuffReader = new BufferedReader(tableDataReader);

        // Read headers - first line of file
        String tableRow;
        if ((tableRow = tableDataBuffReader.readLine()) != null) {
            this.tableHeaders = formatRow(tableRow);
        }

        try {
            while ((tableRow = tableDataBuffReader.readLine()) != null) {
                ArrayList<String> rowList = formatRow(tableRow);
                this.tableData.add(rowList);
            }
        } catch (IOException readDataException) {
            System.out.println("Error reading in data: malformed line in file " + filePath);
        }
        tableDataBuffReader.close();
        tableDataReader.close();
    }


    private ArrayList<String> formatRow(String rowToBeRead) {
        ArrayList<String> rowList = new ArrayList<>(Arrays.asList(rowToBeRead.split("\\t")));
        rowList.replaceAll(s -> s.isEmpty() ? null: s); // replace empty cells with null
        return rowList;
    }


    // Generic write IO helper
    private void writeToFile(String filePath, String text) throws IOException {
        try {
            FileWriter writer = new FileWriter(filePath);
            writer.write(text);
            writer.close();
        } catch (IOException e) {
            throw new IOException("Failed to write to file" + filePath);
        }
    }


    // Write data from memory to filesystem
    public void writeDataToFile() throws DBException {
        String fileToWriteTo = this.tablePath;
        int numberOfRows = this.getNumberOfRows();
        StringBuilder textToWrite = new StringBuilder();
        textToWrite.append(String.join("\t", this.tableHeaders));
        for (int rowIndex = 0; rowIndex < numberOfRows; rowIndex++) {
            String tableRow = String.join("\t", this.getTableRow(rowIndex));
            textToWrite.append("\n").append(tableRow);
        }
        try {
            writeToFile(fileToWriteTo, textToWrite.toString());
        } catch (IOException ioe) {
            throw new DBException("Failed to write table " + this.tablePath + " to file.");
        }
    }


    public void writeMetadataToFile() throws DBException {
        String fileToWriteTo = this.metadataPath;
        // Update idCounter value in the metadata hash table
        this.metadata.put(this.tableName, idCounter);
        StringBuilder textToWrite = new StringBuilder();
        for (String metadataKey: this.metadata.keySet()) {
            String metadataRow = metadataKey + "\t" + this.metadata.get(metadataKey);
            textToWrite.append(metadataRow).append("\n");
        }
        try {
            writeToFile(fileToWriteTo, textToWrite.toString());
        } catch (IOException ioe) {
            throw new DBException("Failed to write metadata to file.");
        }
    }


    // Getters for number of columns and rows
    public int getNumberOfColumns() {
        return this.tableHeaders.size();
    }


    public int getNumberOfRows() {
        return this.tableData.size();
    }


    public String getTableName() {
        String[] tableName = this.tablePath.split(Pattern.quote(File.separator));
        return tableName[tableName.length-1].split("\\.")[0];
    }


    // Add/drop columns
    public void addColumn(String columnName) throws DBException {
        // If the column already exists in the table, it cannot be added again
        if (this.getColumnNames(false).contains(columnName.toLowerCase())) {
            throw new DBException("Column " + columnName + " already exists in this table.");
        }
        // Add column name to headers list
        if (this.tableHeaders.isEmpty()) {
            this.tableHeaders.add(columnName);
        } else {
            this.tableHeaders.add(this.tableHeaders.size(), columnName);
        }

        // Add an empty cell to new column of each row in the table
        for (int row = 0; row < this.getNumberOfRows(); row++) {
            this.tableData.get(row).add(null);
        }
    }


    public void dropColumn(String columnName) throws DBException {
        // Dropping the id column is forbidden
        if (columnName.equals("id")) {
            throw new DBException("Dropping the 'id' column (primary key) is forbidden.");
        }
        int columnIndex = this.getColumnNames(false).indexOf(columnName.toLowerCase());
        if (columnIndex == -1) {
            throw new DBException("This column does not exist in this table.");
        }
        this.tableHeaders.remove(this.tableHeaders.get(columnIndex));
        // If there is data in the table, remove it
            for (int rowIndex = 0; rowIndex < this.getNumberOfRows(); rowIndex++) {
                this.tableData.get(rowIndex).remove(columnIndex);
            }
    }


    // Use this in condition handling etc - case of the column names is preserved in this.tableHeaders,
    // but the results of getColumnNames() are always lower case to ensure case-insensitivity when querying
    public ArrayList<String> getColumnNames(boolean preserveCase) {
        ArrayList<String> columnNames = new ArrayList<>();
        for (String name : this.tableHeaders) {
            if (!preserveCase) name = name.toLowerCase();
            columnNames.add(name);
        }
        return columnNames;
    }


    public int getColumnIndex(String columnName) throws DBException {
        int columnIndex = this.getColumnNames(false).indexOf(columnName);
        if (columnIndex == -1) {
            throw new DBException("Column " + columnName + " does not exist in this table.");
        }
        return columnIndex;
    }


    public ArrayList<String> getTableRow(int rowIndex) {
        return this.tableData.get(rowIndex);
    }


    public void deleteTableRow(int rowIndex) {
        this.tableData.remove(rowIndex);
    }


    public void filterColumnsByIndex(List<Integer> indices) {
        ArrayList<ArrayList<String>> filteredTable = new ArrayList<>();
        for (int rowIndex = 0; rowIndex < this.getNumberOfRows(); rowIndex++) {
            filteredTable.add(rowIndex, new ArrayList<>());
            for (int colIndex = 0; colIndex < this.getNumberOfColumns(); colIndex++) {
                if (indices.contains(colIndex)) {
                    filteredTable.get(rowIndex).add(this.getTableValue(colIndex, rowIndex, true));
                }
            }
        }
        this.tableData = filteredTable;
        ArrayList<String> newHeaders = new ArrayList<>();
        for (int index : indices) {
            newHeaders.add(this.tableHeaders.get(index));
        }
        this.tableHeaders = newHeaders;
    }


    public ArrayList<ArrayList<String>> getFilteredTable(Condition condition)
            throws DBException {
        // Loop over rows, testing condition on each. If condition is true
        // for that row, keep it in table. If not, remove it.
        ArrayList<ArrayList<String>> filteredTable = new ArrayList<>();

        for (int rowIndex = 0; rowIndex < this.getNumberOfRows(); rowIndex++) {
            try {
                if (this.filterRow(condition, rowIndex)) {
                    filteredTable.add(getTableRow(rowIndex));
                }
            } catch (DBException error) {
                throw new DBException(error.getMessage());
            }
        }
        return filteredTable;
    }


    public void filterTable(Condition condition) throws DBException {
        this.tableData = getFilteredTable(condition);
    }


    public ArrayList<String> filterRows(Condition condition)
            throws DBException {
        // Return the 'id' values of the columns where the condition is true
        ArrayList<ArrayList<String>> filteredTable = getFilteredTable(condition);
        int idColumnIndex = this.getColumnIndex("id");
        ArrayList<String> filteredDataIds = new ArrayList<>();
        for (int rowIndex = 0; rowIndex < filteredTable.size(); rowIndex++) {
            filteredDataIds.add(rowIndex, filteredTable.get(rowIndex).get(idColumnIndex));
        }
        return filteredDataIds;
    }


    public boolean filterRow(Condition condition, int rowIndex)
            throws DBException {
        if (condition.getTypeOfCondition().equals(Condition.ConditionType.TERMINAL)) {
            int columnIndex = this.getColumnIndex(condition.getAttributeName());
            String tableValue = this.getTableValue(columnIndex, rowIndex, false);
            return compareConditionToValues(tableValue, condition.getComparator(), condition.getValue());
        }

        if (condition.getBooleanOperator().equals(Token.BooleanOperator.AND)) {
            return filterRow(condition.getCondition1(), rowIndex) &&
                    filterRow(condition.getCondition2(), rowIndex);
        } else {
            return filterRow(condition.getCondition1(), rowIndex) ||
                    filterRow(condition.getCondition2(), rowIndex);
        }
    }


    private static boolean isStringNumeric(String string) {
        try {
            Integer.parseInt(string);
            return true;
        } catch (NumberFormatException error) {
            return false;
        }
    }


    private static boolean compareStrings(String comparisonValue1, String comparator, String comparisonValue2) {
        switch (comparator) {
            case "==":
                return comparisonValue1.equals(comparisonValue2);
            case "!=":
                return !(comparisonValue1.equals(comparisonValue2));
            case ">":
                return comparisonValue1.compareTo(comparisonValue2) > 0;
            case "<":
                return comparisonValue1.compareTo(comparisonValue2) < 0;
            case ">=":
                return comparisonValue1.compareTo(comparisonValue2) >= 0;
            case "<=":
                return comparisonValue1.compareTo(comparisonValue2) <= 0;
            case "like":
                // LIKE - regex pattern matching
                Pattern pattern = Pattern.compile(comparisonValue2);
                Matcher matcher = pattern.matcher(comparisonValue1);
                return matcher.find();
            default:
                return false; // no other valid comparisons for strings
        }
    }


    private static boolean compareNumerics(int comparisonValue1, String comparator, int comparisonValue2) {
        return switch (comparator) {
            case "==" -> comparisonValue1 == comparisonValue2;
            case ">" -> comparisonValue1 > comparisonValue2;
            case "<" -> comparisonValue1 < comparisonValue2;
            case ">=" -> comparisonValue1 >= comparisonValue2;
            case "<=" -> comparisonValue1 <= comparisonValue2;
            case "!=" -> comparisonValue1 != comparisonValue2;
            default -> false;
        };
    }


    private static boolean compareConditionToValues(String tableValue, String comparator, String conditionValue) {
        if (isStringNumeric(tableValue) && isStringNumeric(conditionValue) &&
                !comparator.equals("like")) {
            return compareNumerics(Integer.parseInt(tableValue), comparator, Integer.parseInt(conditionValue));
        } else if (!comparator.equals("like") &&
                (isStringNumeric(tableValue) || isStringNumeric(conditionValue))) {
            return false;
        } else {
            return compareStrings(tableValue, comparator, conditionValue);
        }
    }


    // by column index
    public String getTableValue(int columnIndex, int rowIndex, boolean preserveCase) {
        String tableValue = this.tableData.get(rowIndex).get(columnIndex);
        if (preserveCase) return tableValue;
        return tableValue.toLowerCase();
    }


    public void setTableValue(int columnIndex, int rowIndex, String value) {
        this.tableData.get(rowIndex).set(columnIndex, value);
    }


    public void insertRow(ArrayList<String> valueList) throws DBException {
        // Data validation: valueList must contain the same number of values as the table contains
        // columns. If not, error.
        if (valueList.size() != this.getNumberOfColumns()-1) {
            throw new DBException("The number of columns in the table does not match the number of values to be " +
                    "entered into the table. This table contains " + this.getNumberOfColumns() + " columns.");
        }

        // Add a new (empty...for now..) row to the tableData
        ArrayList<String> newRow = new ArrayList<>();
        for (int i = 0; i < this.getNumberOfColumns(); i++) {
            newRow.add(null);
        }

        // For each column (key), add value to table. Also add an auto-generated id value.
        // get index of id column and add idCounter to table
        int idColumnIndex = this.getColumnIndex("id");
        newRow.set(idColumnIndex, "" + idCounter);
        idCounter++;
        // populate other columns
        int valueListIndex = 0;
        try {
            for (int i = 0; i < this.getNumberOfColumns(); i++) {
                // Skip the id column. This has already been auto-populated
                if (i != idColumnIndex) {
                    newRow.set(i, valueList.get(valueListIndex));
                    valueListIndex++;
                }
            }
        } catch (Exception exception) {
            throw new DBException("Sorry, something went wrong during data insertion. Error message: " +
                    exception.getMessage());
        }
        this.tableData.add(newRow);
        // write changes to idCount to metadata file
        this.writeMetadataToFile();
    }


    // Join another table to this table
    public void joinTable(DBTable table2, int joinColIndex1, int joinColIndex2)
     throws DBException {
        ArrayList<ArrayList<Integer>> table2MatchIndex = this.matchRows(table2, joinColIndex1, joinColIndex2);

        // For JOINs: discard the ids from the original tables
        // discard the columns that the tables were matched on
        this.dropJoiningColumns(table2, joinColIndex1, joinColIndex2);

        // Join the rows together
        // if a row does not match, drop it
        this.joinTables(table2, table2MatchIndex);
    }


    public void editColumnName(String initialColumnName, String newColumnName) throws DBException {
        int colIndex = this.getColumnNames(false).indexOf(initialColumnName.toLowerCase());
        if (colIndex == -1) {
            throw new DBException("Column " + initialColumnName + " does not exist in this table.");
        }
        this.tableHeaders.set(colIndex, newColumnName);
    }

    private int searchColumnNames(String pattern) {
        int columnIndex = -1; // If pattern not found, returns -1
        for (String column : this.tableHeaders) {
            if (column.matches(pattern)) {
                columnIndex = this.getColumnNames(false).indexOf(column);
            }
        }
        return columnIndex;
    }


    private void dropJoiningColumns(DBTable table2, int joinColIndex1, int joinColIndex2)
    throws DBException {
        // For JOINs: discard the ids from the original tables
        // discard the columns that the tables were matched on
        int idColIndex1 = this.searchColumnNames(".*\\.id$");
        int idColIndex2 = table2.searchColumnNames(".*\\.id$");
        if (idColIndex1 == -1 || idColIndex2 == -1) {
            throw new DBException("An id column was not found in one of the tables.");
        }
        try {
            this.dropColumn(this.getColumnNames(false).get(joinColIndex1));
            if (joinColIndex1 != idColIndex1) {
                this.dropColumn(this.getColumnNames(false).get(idColIndex1));
            }
            table2.dropColumn(table2.getColumnNames(false).get(joinColIndex2));
            if (joinColIndex2 != idColIndex2) {
                table2.dropColumn(table2.getColumnNames(false).get(idColIndex2));
            }
        } catch (DBException error) {
            throw new DBException(error.getMessage());
        }
    }


    private void joinTables(DBTable table2, ArrayList<ArrayList<Integer>> table2MatchIndex) {
        ArrayList<ArrayList<String>> joinedTable = new ArrayList<>();
        ArrayList<String> joinedHeaders = new ArrayList<>();
        joinedHeaders.add("id");
        joinedHeaders.addAll(this.tableHeaders);
        joinedHeaders.addAll(table2.tableHeaders);

        // Add values
        ArrayList<String> table1Row;
        ArrayList<String> table2Row;
        ArrayList<String> joinedRow;
        for (int i = 0; i < table2MatchIndex.size(); i++) {
            table1Row = new ArrayList<>(this.getTableRow(table2MatchIndex.get(i).get(0)));
            table2Row = new ArrayList<>(table2.getTableRow(table2MatchIndex.get(i).get(1)));
            joinedRow = new ArrayList<>();
            joinedRow.add(Integer.toString(i+1));
            joinedRow.addAll(table1Row);
            joinedRow.addAll(table2Row);
            joinedTable.add(joinedRow);
        }
        this.tableData = joinedTable;
        this.tableHeaders = joinedHeaders;
    }


    // This method returns an arraylist of integers that represent the rowIndex
    // in the joinTable that matches that row in this table. For example, an
    // arraylist [2, 4, 1, ...] means that row0 in this table is matched by row 2
    // in the join table, etc. A value of -1 means there is no match in the join
    // table for that row in this table.
    private ArrayList<ArrayList<Integer>> matchRows(DBTable table2, int joinColIndex1, int joinColIndex2) {
        // Each row of the 2D array contains 2 values: row index of table1 + row index of table2 match
        ArrayList<ArrayList<Integer>> joinTableMatchedRows = new ArrayList<>();
        for (int table1Row = 0; table1Row < this.getNumberOfRows(); table1Row++) {
            for (int table2Row = 0; table2Row < table2.getNumberOfRows(); table2Row++) {
                String table1Value = this.getTableValue(joinColIndex1, table1Row, false);
                String table2Value = table2.getTableValue(joinColIndex2, table2Row, false);
                if ((table1Value).equals(table2Value)) {
                    joinTableMatchedRows.add(new ArrayList<>());
                    joinTableMatchedRows.get(joinTableMatchedRows.size()-1).add(table1Row);
                    joinTableMatchedRows.get(joinTableMatchedRows.size()-1).add(table2Row);
                }
            }
        }
        return joinTableMatchedRows;
    }


    public String toString() {
        // method that formats and returns the table as a String
        StringBuilder printableTable = new StringBuilder();
        // Headers - use this.tableHeaders to preserve header case in output
        for (String columnName : this.tableHeaders) {
            printableTable.append(columnName).append("\t");
        }
        printableTable.append("\r\n");

        // Values
        for (int rowIndex = 0; rowIndex < this.getNumberOfRows(); rowIndex++) {
            for (int columnIndex = 0; columnIndex < this.getNumberOfColumns(); columnIndex++) {
                String cellData = this.tableData.get(rowIndex).get(columnIndex);
                if (cellData.equals("null") || cellData.isBlank()) {
                    cellData = "";
                }
                printableTable.append(cellData).append("\t");
            }
            printableTable.append("\r\n");
        }
        return printableTable.toString();
    }
}