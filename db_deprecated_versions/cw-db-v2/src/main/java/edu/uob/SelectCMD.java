package edu.uob;

import java.util.ArrayList;
import java.util.List;

public class SelectCMD extends DBcmd {
    private String tableName;
    private String tablePath;
    ArrayList<String> attributeList;
    Condition condition;

    public SelectCMD(String tableName, Condition condition, ArrayList<String> attributeList) {
        this.tableName = tableName;
        this.tablePath = getFilePath(databaseName, tableName);
        this.attributeList = attributeList;
        this.condition = condition;
    }


    public String handleQuery() {
        DBTable databaseTable;
        try {
            databaseTable = instantiateDatabaseTable(this.tableName, this.tablePath);
        } catch (DBException exception) {
            return exception.getMessage();
        }

        // Handle WHERE condition (if there is one)
        if (this.condition != null) {
            try {
                databaseTable.filterTable(this.condition);
            } catch (DBException error) {
                return error.getMessage();
            }
        }
        // Select columns (or wildcard)
        try {
            this.selectColumns(databaseTable);
        } catch (DBException exception) {
            return exception.getMessage();
        }
        // Everything went fine, so return response code/message and printable table
        return "[OK]\r\n" + databaseTable;
    }


    private void selectColumns(DBTable databaseTable) throws DBException {
        List<String> selectColumns = this.attributeList;
        if (selectColumns.isEmpty()) {
            throw new DBException("No columns were selected. " +
                    "Please select one or more columns, or use the wildcard character *.");
        }
        // If WildAttribute is NOT *, filter table by selected columns (attribute
        // names) before proceeding.
        if (!(selectColumns.size() == 1 && selectColumns.get(0).equals("*"))) {
            selectFilterColumns(databaseTable);
        }
    }


    private void selectFilterColumns(DBTable databaseTable) throws DBException {
        List<String> selectColumns = this.attributeList;
        List<Integer> selectColumnIndices = new ArrayList<>();
        ArrayList<String> columnNames = databaseTable.getColumnNames(false);
        for (String column : selectColumns) {
            int index = columnNames.indexOf(column);
            if (index >= 0) {
                selectColumnIndices.add(index);
            }
        }
        if (selectColumnIndices.isEmpty()) {
            throw new DBException("None of the selected columns exist in this table.");
        }
        databaseTable.filterColumnsByIndex(selectColumnIndices);
    }
}