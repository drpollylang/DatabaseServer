package edu.uob;

import java.util.ArrayList;

public class DeleteCMD extends DBcmd {
    public DeleteCMD(ArrayList<Token> tokenStream) {
        this.tokenStream = tokenStream;
    }


    public void parseCommand() throws DBException {
        // <Delete> ::= "DELETE " "FROM " [TableName] " WHERE " <Condition>
        checkKeyword(1, "from");
        checkKeyword(3, "where");
        checkIdentifier(2);

        int conditionStartIndex = findToken(this.tokenStream, "where") + 1;
        this.tableName = this.tokenStream.get(2).getTokenValue();
        this.tablePath = this.getFilePath(this.tableName);
        this.parseConditions(this.tokenStream.subList(conditionStartIndex, this.tokenStream.size()-1));
    }


    public String handleQuery() {
        try {
            this.instantiateDatabaseTable();
        } catch (DBException exception) {
            return exception.getMessage();
        }

        // Apply condition - get ids of the rows the condition applies to
        ArrayList<String> filteredDataIds;
        try {
            filteredDataIds = this.filterTableByCondition();
            // Delete all rows where condition is true and write results to filesystem
            this.deleteRows(filteredDataIds);
        } catch (DBException error) {
            return error.getMessage();
        }
        return "[OK]";
    }


    private void deleteRows(ArrayList<String> filteredDataIds)
            throws DBException {
        int idColumnIndex = this.databaseTable.getColumnIndex("id");
        int rowIndex = 0;
        int numberOfRows = this.databaseTable.getNumberOfRows();
        for (int i = 0; i < numberOfRows; i++) {
            if (filteredDataIds.contains(this.databaseTable.getTableValue(idColumnIndex, rowIndex, false))) {
                this.databaseTable.deleteTableRow(rowIndex);
            } else {
                rowIndex++;
            }
        }
        this.databaseTable.writeDataToFile();
    }
}