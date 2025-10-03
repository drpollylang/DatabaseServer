package edu.uob;

import java.io.*;
import java.util.*;
import java.util.stream.*;
import java.util.Arrays;
import java.nio.file.Paths;
import java.nio.file.Files;
import edu.uob.Condition.*;

// TODO: ensure compatability with QueryHandler.
// TODO automated script testing and error handling

public class DBTable {

    //private String tableFilePath;
    //private String responseStatus;
    //private String responseMessage;
    private String tablePath;
    private String metadataPath;
    public ArrayList<String> metadata;
    //private Hashtable<String, ArrayList<String>> tableData;
    private ArrayList<String> tableHeaders;
    private ArrayList<ArrayList<String>> tableData;
    public int idCounter;

    public DBTable(String tablePath) throws DBException {
        this.tablePath = tablePath;
        this.metadataPath = tablePath.substring(0, tablePath.lastIndexOf(File.separator)) + File.separator + "metadata.txt";
        this.metadata = this.readMetadata();
        this.idCounter = Integer.parseInt(metadata.get(0));
        //this.tableData = new Hashtable<String, ArrayList<String>>();
        this.tableHeaders = new ArrayList<>();
        this.tableData = new ArrayList<ArrayList<String>>();
        try {
            // Try to open and read table data - throw IOException if you can't
            this.readDataFromFile(tablePath);
        } catch(IOException ioe) {
            System.out.println("Can't seem to open table at " + tablePath + ". Does this table exist?");
        }

        // For testing only - print hash table to console
        //System.out.println(this.tableData.toString());
    }

    private ArrayList<String> readMetadata() throws DBException {
        try {
            ArrayList<String> metadata = new ArrayList<String>();
            File metadataFile = new File(this.metadataPath);
            if (!metadataFile.exists()) {
                throw new DBException("Metadata file for this database does not exist.");
            }
            BufferedReader metadataReader = new BufferedReader(new FileReader(metadataFile));
            String metadataLine;
            while ((metadataLine = metadataReader.readLine()) != null) {
                metadata.add(metadataLine);
            }
            return metadata;
        } catch(IOException ioe) {
            throw new DBException("Failed to read metadata from " + this.metadataPath);
        }
    }


    private void readDataFromFile(String filePath) throws IOException {
        File tableFile = new File(filePath);
        if (!tableFile.exists()) {
            throw new FileNotFoundException("Error: file " + filePath + " not found.");
        }
        FileReader tableDataReader = new FileReader(tableFile);
        BufferedReader tableDataBuffReader = new BufferedReader(tableDataReader);
        // Read table row-by-row. Add each value of each row (as a String) to the 2D
        // ArrayList tableRows
        /*ArrayList<String[]> tableRows = new ArrayList<String[]>();
        String tableRow;

        try {
            while ((tableRow = tableDataBuffReader.readLine()) != null) {
                //tableRows.add((tableRow.split("\\t"))); // split data on tab
                List<String> rowSplit = Arrays.asList(tableRow.split("\\t"));
                rowSplit.replaceAll(s -> s.equals("") ? null: s); // replace empty cells with null
                //System.out.println("TableRow: " + tableRow);
                tableRows.add(rowSplit.toArray(new String[rowSplit.size()]));
            }
        } catch (IOException readDataException) {
            System.out.println("Error reading in data: malformed line in file " + filePath);
        }

        if(tableRows.isEmpty()) return;

        try {
            checkDataNotMalformed(tableRows);
        } catch (Exception malformedDataException) {
            System.out.println("Error in file reading: one of the rows was malformed.");
        }
        addDataToHashtable(tableRows);
         */

        // Read headers - first line of file
        String tableRow;
        if ((tableRow = tableDataBuffReader.readLine()) != null) {
            this.tableHeaders = formatRow(tableRow);
        }

        try {
            while ((tableRow = tableDataBuffReader.readLine()) != null) {
                //ArrayList<String> rowList = new ArrayList<>(Arrays.asList(tableRow.split("\\t")));
                //rowList.replaceAll(s -> s.equals("") ? null: s); // replace empty cells with null
                //System.out.println("TableRow: " + tableRow);
                ArrayList<String> rowList = formatRow(tableRow);
                this.tableData.add(rowList);
            }
        } catch (IOException readDataException) {
            System.out.println("Error reading in data: malformed line in file " + filePath);
        }

    }

    private ArrayList<String> formatRow(String rowToBeRead) {
        ArrayList<String> rowList = new ArrayList<>(Arrays.asList(rowToBeRead.split("\\t")));
        rowList.replaceAll(s -> s.equals("") ? null: s); // replace empty cells with null
        return rowList;
    }

    private void checkDataNotMalformed(ArrayList<String[]> tableRows) throws Exception{
        int numberOfColumns = tableRows.get(0).length;
        //System.out.println("Number of columns: " + numberOfColumns);
        for (String[] tableRow : tableRows) {
            if (tableRow.length != numberOfColumns) {
                throw new Exception("Error in file reading: one of the rows was malformed.");
            }
        }
    }


/*
    private void addDataToTable(ArrayList<String[]> tableRows) {
        // Data is read in from file as a ArrayList of String arrays
        // Each array in the ArrayList corresponds to a row in the table
        // Each String in the String Array corresponds to a data value in the row
        // This function takes this structure and uses it to populate the tableData structure:
        // a Hashtable where each key is a column name and each linked list is the value of the column for that row (index)
        // Form each column into an ArrayList
        for (int columnIndex = 0; columnIndex < tableRows.get(0).length; columnIndex++) {
            String columnName = tableRows.get(0)[columnIndex];
            //System.out.println("columnName: " + columnName);
            ArrayList<String> newColumn = new ArrayList<String>();
            for (int rowIndex = 1; rowIndex < tableRows.size(); rowIndex++) {
                //System.out.println("row: " + rowIndex);
                newColumn.add(tableRows.get(rowIndex)[columnIndex]);
            }
            // Add column name and an ArrayList containing column data to the Hashtable
            //this.tableData.put(columnName, newColumn);

        }

    }
*/

    // Get data from hash table


    // Generic write IO helper
    private void writeToFile(String filePath, String text) throws IOException {
        try {
            // Append
            //FileWriter writer = new FileWriter(filePath, true);
            //BufferedWriter buffwriter = new BufferedWriter(writer);
            //buffwriter.write(text);
            //buffwriter.close();
            // Write
            FileWriter writer = new FileWriter(filePath);
            writer.write(text);
            writer.close();
        } catch (IOException e) {
            throw new IOException("Failed to write to file" + filePath);
        }
    }

    // Write data from memory to filesystem
    public void writeDataToFile() {
        String fileToWriteTo = this.tablePath;
        String textToWrite;
        int numberOfRows = this.getNumberOfRows();
        String columnNames = String.join("\t", this.tableHeaders);
        textToWrite = columnNames;
        //try {
        //    writeToFile(fileToWriteTo, columnNamesString);
        //} catch (IOException ioe) {
        //    System.out.println("Error: failed to write table " + this.tablePath + " to file.");
        //}
        for (int rowIndex = 0; rowIndex < numberOfRows; rowIndex++) {
            String row = String.join("\t", this.getTableRow(rowIndex));
            textToWrite = textToWrite + "\n" + row;
            //try {
            //    writeToFile(fileToWriteTo, row);
            //} catch (IOException ioe) {
            //    System.out.println("Error: failed to write table " + this.tablePath + " to file.");
            //}
        }
        try {
            writeToFile(fileToWriteTo, textToWrite);
        } catch (IOException ioe) {
            System.out.println("Error: failed to write table " + this.tablePath + " to file.");
        }
    }

    public void writeMetadataToFile() {
        String fileToWriteTo = this.metadataPath;
        String textToWrite = Integer.toString(this.idCounter);
        try {
            writeToFile(fileToWriteTo, textToWrite);
        } catch (IOException ioe) {
            System.out.println("Error: failed to write metadata to file.");
        }
    }

    // Getters for number of columns and rows
    public int getNumberOfColumns() {
        //return this.tableData.get(0).size();
        return this.tableHeaders.size();
    }

    public int getNumberOfRows() {
        return this.tableData.size();
    }

    // Get the actual data table
    public ArrayList<ArrayList<String>> getTableData() {
        return this.tableData;
    }

    public String getTableName() {
        String[] tableName = this.tablePath.split(File.separator);
        return tableName[tableName.length-1].split("\\.")[0];
    }

    // Add/drop columns
    public void addColumn(String columnName) {
        //ArrayList<String> columnValues = new ArrayList<String>();
        //this.tableData.put(columnName, columnValues);
        // TODO
        // Add column name to headers list
        if (this.tableHeaders.isEmpty()) {
            this.tableHeaders.add(columnName);
        } else {
            this.tableHeaders.add(this.tableHeaders.size(), columnName);
        }

        // Add null entries into table
        //int columnIndex = this.tableHeaders.indexOf(columnName);
        //for (int rowIndex = 0; rowIndex < this.getNumberOfRows(); rowIndex++) {
        //    this.tableData.get(rowIndex).add(columnIndex, null);
            //this.tableData.get(rowIndex).set(columnIndex, null);
        //}

        // write changes to file
        //this.writeDataToFile();
    }

    public void dropColumn(String columnName) throws DBException {
        // TODO
        /*
        if (this.tableData.containsKey(columnName)) {
            this.tableData.remove(columnName);
        }
        else {
            throw new DBException("Column " + columnName + " does not exist in this table.");
        }
         */
        int columnIndex = this.tableHeaders.indexOf(columnName);
        if (columnIndex == -1) {
            throw new DBException("This column does not exist in this table.");
        }
        this.tableHeaders.remove(columnName);
        // If there is data in the table, remove it
        if (this.getNumberOfColumns() > columnIndex) {
            for (int rowIndex = 0; rowIndex < this.getNumberOfRows(); rowIndex++) {
                this.tableData.get(rowIndex).remove(columnIndex);
            }
        }
        // write changes to file
        //this.writeDataToFile();
    }

    public ArrayList<String> getColumnNames() {
        //return Collections.list(this.tableData.keys());
        //return this.tableHeaders;
        ArrayList<String> colnames = new ArrayList<String>();
        for (String name : this.tableHeaders) {
            colnames.add(name.toLowerCase());
        }
        return colnames;
    }

    public ArrayList<String> getTableRow(int rowIndex) {
        return this.tableData.get(rowIndex);
        //ArrayList<String> columnNames = this.getColumnNames();
        //for (String columnName : columnNames) {
        //    tableRow.add(this.tableData.get(columnName).get(rowIndex));
        //}
    }

    public void deleteTableRow(int rowIndex) {
        this.tableData.remove(rowIndex);
        //this.writeDataToFile();
    }

    public void filterColumnsByIndex(List<Integer> indices) {
        ArrayList<ArrayList<String>> filteredTable = new ArrayList<ArrayList<String>>();
        for (int rowIndex = 0; rowIndex < this.getNumberOfRows(); rowIndex++) {
            filteredTable.add(rowIndex, new ArrayList<String>());
            for (int colIndex = 0; colIndex < this.getNumberOfColumns(); colIndex++) {
                if (indices.contains(colIndex)) {
                    filteredTable.get(rowIndex).add(this.getTableValue(colIndex, rowIndex));
                }
            }
        }
        this.tableData = filteredTable;
        ArrayList<String> newHeaders = new ArrayList<String>();
        for (int index : indices) {
            newHeaders.add(this.tableHeaders.get(index));
        }
        this.tableHeaders = newHeaders;
    }

    public ArrayList<ArrayList<String>> getFilteredTableWhereConditionTrue(Condition condition) throws DBException {
        // Loop over rows, testing condition on each. If condition is true
        // for that row, keep it in table. If not, remove it.
        ArrayList<ArrayList<String>> filteredTable = new ArrayList<ArrayList<String>>();

        for (int rowIndex = 0; rowIndex < this.getNumberOfRows(); rowIndex++) {
            try {
                if (this.filterRowByCondition(condition, rowIndex)) {
                    filteredTable.add(getTableRow(rowIndex));
                }
            } catch (DBException error) {
                throw new DBException(error.getMessage());
            }
        }
        return filteredTable;
    }

    public void filterTableByCondition(Condition condition) throws DBException {
        this.tableData = getFilteredTableWhereConditionTrue(condition);
    }

    public ArrayList<String> getRowIndexWhereConditionTrue(Condition condition) throws DBException {
        // Return the 'id' values of the columns where the condition is true
        ArrayList<ArrayList<String>> filteredTable = getFilteredTableWhereConditionTrue(condition);
        int idColumnIndex = this.getColumnNames().indexOf("id");
        ArrayList<String> filteredDataIds = new ArrayList<String>();
        for (int rowIndex = 0; rowIndex < filteredTable.size(); rowIndex++) {
            filteredDataIds.add(rowIndex, filteredTable.get(rowIndex).get(idColumnIndex));
        }
        return filteredDataIds;
    }

    public boolean filterRowByCondition(Condition condition, int rowIndex)
            throws DBException {
        try {
            ConditionType conditionType = condition.getTypeOfCondition();
        } catch (DBException error) {
            throw new DBException("Malformed condition.");
        }
        if (condition.getTypeOfCondition().equals(Condition.ConditionType.TERMINAL)) {
            int columnIndex = this.getColumnNames().indexOf(condition.getAttributeName());
            if (columnIndex == -1) {
                throw new DBException("Column " + condition.getAttributeName() +
                        " does not exist in this table.");
            }
            String tableValue = this.getTableValue(columnIndex, rowIndex);
            return compareConditionToValues(tableValue, condition.getComparator(), condition.getValue());
        }

        if (condition.getBooleanOperator().equals(Token.BooleanOperator.AND)) {
            return filterRowByCondition(condition.getCondition1(), rowIndex) &&
                    filterRowByCondition(condition.getCondition2(), rowIndex);
        } else {
            return filterRowByCondition(condition.getCondition1(), rowIndex) ||
                    filterRowByCondition(condition.getCondition2(), rowIndex);
        }
    }

    private static boolean compareConditionToValues(String tableValue, String comparator, String conditionValue) {
        if (!(comparator.equals("like") || comparator.equals("=="))) {
            int int1 = Integer.valueOf(tableValue);
            int int2 = Integer.valueOf(conditionValue);

            switch (comparator) {
                case "==":
                    return int1 == int2;
                case ">":
                    return int1 > int2;
                case "<":
                    return int1 < int2;
                case ">=":
                    return int1 >= int2;
                case "<=":
                    return int1 <= int2;
                case "!=":
                    return int1 != int2;
                default:
                    return false;
            }
        } else if (comparator.equals("==")) {
            return tableValue.equals(conditionValue);
        } else {
            // LIKE - match string case-insensitively
            return tableValue.toLowerCase().equals(conditionValue.toLowerCase());
        }

    }


    // by column index
    public String getTableValue(int columnIndex, int rowIndex) {
        //return this.tableData.get(columnName).get(rowIndex);
        return this.tableData.get(rowIndex).get(columnIndex);
    }

    public void setTableValue(int columnIndex, int rowIndex, String value) {
        //this.tableData.get(columnName).set(rowIndex, value);
        this.tableData.get(rowIndex).set(columnIndex, value);
        // Write to file
        //this.writeDataToFile();
    }


    public void insertRowOfData(ArrayList<String> valueList) throws DBException {
        // Data validation: valueList must contain the same number of values as the table contains
        // columns. If not, error.
        if (valueList.size() != this.getNumberOfColumns()-1) {
            throw new DBException("Error: the number of columns in the table does not match the number of values to be " +
                    "entered into the table. This table contains " + this.getNumberOfColumns() + " columns.");
        }

        // Add a new (empty...for now..) row to the tableData
        ArrayList<String> newRow = new ArrayList<>();
        for (int i = 0; i < this.getNumberOfColumns(); i++) {
            newRow.add(null);
        }

        // For each column (key), add value to table. Also add an auto-generated id value.
        int rowIndex = this.getNumberOfRows();
        idCounter++;
        // get index of id column and add idCounter to table
        int idColumnIndex = this.getColumnNames().indexOf("id");
        newRow.set(idColumnIndex, "" + idCounter);
        //this.setTableValue(idColumnIndex, rowIndex, Integer.toString(idCounter));
        // populate other columns
        int valueListIndex = 0;
        try {
            for (int i = 0; i < this.getNumberOfColumns(); i++) {
                // Skip the id column. This has already been auto-populated
                if (i != idColumnIndex) {
                    //this.setTableValue(i, rowIndex, valueList.get(valueListIndex));
                    newRow.set(i, valueList.get(valueListIndex));
                    valueListIndex++;
                }
            }
        } catch (Exception exception) {
            throw new DBException("Sorry, something went wrong during data insertion. Error message: " +
                    exception.getMessage());
        }
        this.tableData.add(newRow);

        /*
        this.setTableValue("id", rowIndex, Integer.toString(idCounter));
        try {
            ArrayList<String> columnNames = this.getColumnNames();
            for (int i = 0; i < valueList.size(); i++) {
                this.setTableValue(columnNames.get(i), rowIndex, valueList.get(i));
            }
        } catch (Exception exception) {
            throw new DBException("Sorry, something went wrong during data insertion. Error message: " +
                    exception.getMessage());
        }
         */

        // write changes to file
        //this.writeDataToFile();
        // write changes to idCount to metadata file
        this.writeMetadataToFile();
    }

    public void deleteRowOfData(int rowIndex) throws DBException {
        this.tableData.remove(rowIndex);
        // write changes to file
        //this.writeDataToFile();
    }
    
    
    // Join another table to this table
    public void joinTable(DBTable table2, int joinColIndex1, int joinColIndex2)
     throws DBException {
        ArrayList<Integer> table2MatchIndex = this.matchRows(table2, joinColIndex1, joinColIndex2);
        // For JOINs: discard the ids from the original tables
        // discard the columns that the tables were matched on
        this.dropJoiningColumns(table2, joinColIndex1, joinColIndex2);

        // Join the rows together
        // if a row does not match, drop it
        this.joinTablesByMatchIndex(table2, table2MatchIndex);

        // create a new unique id for each of row of the table produced
        this.addColumn("id");
        // Add an empty cell to new column of each row in the table
        for (int row = 0; row < this.getNumberOfRows(); row++) {
            this.tableData.get(row).add(null);
        }
        for (int rowIndex = 0; rowIndex < this.getNumberOfRows(); rowIndex++) {
            this.setTableValue(this.getColumnNames().indexOf("id"), rowIndex, Integer.toString(idCounter));
            idCounter++;
        }
    }

    public void editColumnName(String initialColumnName, String newColumnName) throws DBException {
        int colIndex = this.getColumnNames().indexOf(initialColumnName);
        if (colIndex == -1) {
            throw new DBException("Column " + initialColumnName + "does not exist in this table.");
        }
        this.tableHeaders.set(colIndex, newColumnName);
        //this.writeDataToFile();
    }

    private int searchColumnsForPattern(String pattern) {
        int columnIndex = -1; // If pattern not found, returns -1
        for (String column : this.tableHeaders) {
            if (column.matches(pattern)) {
                columnIndex = this.getColumnNames().indexOf(column);
            }
        }
        return columnIndex;
    }

    private void dropJoiningColumns(DBTable table2, int joinColIndex1, int joinColIndex2)
    throws DBException {
        // For JOINs: discard the ids from the original tables
        // discard the columns that the tables were matched on
        int idColIndex1 = this.searchColumnsForPattern(".*\\.id$");
        int idColIndex2 = table2.searchColumnsForPattern(".*\\.id$");
        if (idColIndex1 == -1 || idColIndex2 == -1) {
            throw new DBException("An id column was not found in one of the tables.");
        }
        try {
            this.dropColumn(this.getColumnNames().get(joinColIndex1));
            this.dropColumn(this.getColumnNames().get(idColIndex1));
            table2.dropColumn(table2.getColumnNames().get(joinColIndex2));
            table2.dropColumn(table2.getColumnNames().get(idColIndex1));
        } catch (DBException error) {
            throw new DBException(error.getMessage());
        }
    }

    private void joinTablesByMatchIndex(DBTable table2, ArrayList<Integer> table2MatchIndex) {
        for (String table2Column : table2.getColumnNames()) {
            // Add table2 columns to table1
            this.addColumn(table2Column);
        }
        int widthOfTable1 = this.getNumberOfColumns() - table2.getNumberOfColumns();
        // Add an empty cell to each new column of each row in the table
        for (int row = 0; row < this.getNumberOfRows(); row++) {
            for (int table2Column = 0; table2Column < table2.getNumberOfColumns(); table2Column++) {
                this.tableData.get(row).add(null);
            }
        }
        // Add values
        int rowIndex = 0;
        for (int matchRow = 0; matchRow < table2MatchIndex.size(); matchRow++) {
            if (table2MatchIndex.get(matchRow) != -1 ) {
                for (int colIndex = 0; colIndex < table2.getNumberOfColumns(); colIndex++) {
                    String table2Value = table2.getTableValue(colIndex, table2MatchIndex.get(matchRow));
                    this.setTableValue(widthOfTable1+colIndex, rowIndex, table2Value);
                }
                rowIndex++;
            } else {
                this.deleteTableRow(rowIndex); // If no match in joining table, remove row
            }
        }
    }

    // This method returns an arraylist of integers that represent the rowIndex
    // in the joinTable that matches that row in this table. For example, an
    // arraylist [2, 4, 1, ...] means that row0 in this table is matched by row 2
    // in the join table, etc. A value of -1 means there is no match in the join
    // table for that row in this table.
    private ArrayList<Integer> matchRows(DBTable table2, int joinColIndex1, int joinColIndex2) {
        ArrayList<Integer> joinTableMatchedRows = new ArrayList<Integer>();
        for (int table1Row = 0; table1Row < this.getNumberOfRows(); table1Row++) {
            for (int table2Row = 0; table2Row < table2.getNumberOfRows(); table2Row++) {
                String table1Value = this.getTableValue(joinColIndex1, table1Row);
                String table2Value = table2.getTableValue(joinColIndex2, table2Row);
                if ((table1Value).equals(table2Value)) {
                    joinTableMatchedRows.add(table1Row, table2Row);
                    break;
                }
            }
            // If there is no match found, add -1 to this position in the arraylist
            if (joinTableMatchedRows.size() < table1Row+1) {
                joinTableMatchedRows.add(table1Row, -1);
            }
        }
        return joinTableMatchedRows;
    }


    public String toString() {
        // TODO: method that formats and returns the table as a String
        String printableTable = "";
        // Headers
        for (String columnName : this.getColumnNames()) {
            printableTable = printableTable + "| " + columnName + "\t";
        }
        printableTable = printableTable + " |"  + "\n";

        // Values
        for (int rowIndex = 0; rowIndex < this.getNumberOfRows(); rowIndex++) {
            for (int columnIndex = 0; columnIndex < this.getNumberOfColumns(); columnIndex++) {
            //for (String columnName : this.getColumnNames()) {
                printableTable = printableTable + "| " + this.getTableValue(columnIndex, rowIndex) + " ";
            }
            printableTable = printableTable + " |"  + "\n";
        }
        return printableTable;
    }


}
