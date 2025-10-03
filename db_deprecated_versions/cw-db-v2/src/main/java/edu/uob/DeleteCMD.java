package edu.uob;

import java.util.ArrayList;

public class DeleteCMD extends DBcmd {
    private String tableName;
    private String tablePath;
    Condition condition;

    public DeleteCMD(String tableName, Condition condition) {
        this.tableName = tableName;
        this.tablePath = getFilePath(databaseName, tableName);
        this.condition = condition;

    }


    public String handleQuery() {
        DBTable databaseTable;
        try {
            databaseTable = instantiateDatabaseTable(this.tableName, this.tablePath);
        } catch (DBException exception) {
            return exception.getMessage();
        }

        // Apply condition - get ids of the rows the condition applies to
        ArrayList<String> filteredDataIds;
        try {
            filteredDataIds = filterTableByCondition(databaseTable, this.condition);
        } catch (DBException error) {
            return error.getMessage();
        }
        // Delete all rows where condition is true and write results to filesystem
        try {
            deleteRows(databaseTable, filteredDataIds);
        } catch (DBException error) {
            return error.getMessage();
        }
        return "[OK]";
    }


    private void deleteRows(DBTable databaseTable, ArrayList<String> filteredDataIds)
            throws DBException {
        int idColumnIndex = databaseTable.getColumnIndex("id");
        int rowIndex = 0;
        int numberOfRows = databaseTable.getNumberOfRows();
        for (int i = 0; i < numberOfRows; i++) {
            if (filteredDataIds.contains(databaseTable.getTableValue(idColumnIndex, rowIndex, false))) {
                databaseTable.deleteTableRow(rowIndex);
            } else {
                rowIndex++;
            }
        }
        databaseTable.writeDataToFile();
    }
}