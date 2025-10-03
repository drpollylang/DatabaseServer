package edu.uob;

import java.util.ArrayList;
import edu.uob.DBException.*;

public class UpdateCMD extends DBcmd {
    private String tableName;
    private String tablePath;
    ArrayList<NameValuePair> nameValueList;
    Condition condition;

    public UpdateCMD(String tableName, Condition condition, ArrayList<NameValuePair> nameValueList) {
        this.tableName = tableName;
        this.tablePath = getFilePath(databaseName, tableName);
        this.nameValueList = nameValueList;
        this.condition = condition;
    }


    public String handleQuery() {
        // Check the user isn't trying to change the id column
        for (NameValuePair nameValuePair : this.nameValueList) {
            if (nameValuePair.getAttributeName().equals("id")) {
                return new manuallyChangingIdColumnException().getMessage();
            }
        }
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
        // Set new values (according to name-value pairs) for rows where condition is true
        try {
            this.updateTable(databaseTable, filteredDataIds);
        } catch (DBException error) {
            return error.getMessage();
        }
        return "[OK]";
    }


    private DBTable updateValue(DBTable databaseTable, NameValuePair nameValuePair, int rowIndex)
            throws DBException {
        String attributeName = nameValuePair.getAttributeName();
        String value = nameValuePair.getValue();
        int columnIndex = databaseTable.getColumnNames(false).indexOf(attributeName);
        if (columnIndex == -1) {
            throw new DBException("Column name " + attributeName + " does not exist within this table");
        }
        databaseTable.setTableValue(columnIndex, rowIndex, value);
        return databaseTable;
    }


    private void updateTable(DBTable databaseTable, ArrayList<String> filteredDataIds)
            throws DBException {
        int idColumnIndex = databaseTable.getColumnNames(false).indexOf("id");
        for (NameValuePair nameValuePair : this.nameValueList) {
            for (int rowIndex = 0; rowIndex < databaseTable.getNumberOfRows(); rowIndex++) {
                if (filteredDataIds.contains(databaseTable.getTableValue(idColumnIndex, rowIndex, false))) {
                    databaseTable = updateValue(databaseTable, nameValuePair, rowIndex);
                }
            }
        }
        databaseTable.writeDataToFile();
    }
}
