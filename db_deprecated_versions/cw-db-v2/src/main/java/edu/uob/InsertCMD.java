package edu.uob;

import java.util.ArrayList;

public class InsertCMD extends DBcmd {
    private String tablePath;
    ArrayList<String> valueList;

    public InsertCMD(String tableName, ArrayList<String> valueList) {
        this.tablePath = getFilePath(databaseName, tableName);
        this.valueList = valueList;

    }


    public String handleQuery() {
        DBTable databaseTable;
        try {
            databaseTable = new DBTable(this.tablePath);
        } catch (DBException exception) {
            return exception.getMessage();
        }
        // Data validation: ValueList must contain the same number of values as the table contains
        // columns. If not, error.
        ArrayList<String> valueList = this.valueList;
        try {
            databaseTable.insertRow(valueList);
            databaseTable.writeDataToFile();
            return "[OK]";
        } catch (DBException exception) {
            return exception.getMessage();
        }
    }
}
