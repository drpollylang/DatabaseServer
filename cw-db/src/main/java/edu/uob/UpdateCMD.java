package edu.uob;

import java.util.ArrayList;
import edu.uob.DBException.*;

public class UpdateCMD extends DBcmd {

    public UpdateCMD(ArrayList<Token> tokenStream) {
        this.tokenStream = tokenStream;
    }


    public void parseCommand() throws DBException {
        // <Update> ::= "UPDATE " [TableName] " SET " <NameValueList> " WHERE " <Condition>
        int endOfNameValueList = findToken(this.tokenStream, "where");
        checkIdentifier(1);
        checkKeyword(2, "set");
        if (endOfNameValueList == -1) {
            throw new malformedQueryException();
        }

        this.tableName = this.tokenStream.get(1).getTokenValue();
        this.tablePath = this.getFilePath(tableName);

        // Parse <NameValueList>
        this.parseNameValueList(this.tokenStream.subList(3, endOfNameValueList));

        // Parse "WHERE" <Condition> (if present)
        this.parseConditions(this.tokenStream.subList(endOfNameValueList+1, this.tokenStream.size()-1));
    }


    public String handleQuery() {
        // Check the user isn't trying to change the id column
        for (NameValuePair nameValuePair : this.nameValueList) {
            if (nameValuePair.getAttributeName().equals("id")) {
                return new manuallyChangingIdColumnException().getMessage();
            }
        }
        ArrayList<String> filteredDataIds;
        try {
            this.instantiateDatabaseTable();
            // Apply condition - get ids of the rows the condition applies to
            filteredDataIds = this.filterTableByCondition();
            // Set new values (according to name-value pairs) for rows where condition is true
            this.updateTable(filteredDataIds);
        } catch (DBException exception) {
            return exception.getMessage();
        }
        return "[OK]";
    }


    private void updateValue(NameValuePair nameValuePair, int rowIndex)
            throws DBException {
        String attributeName = nameValuePair.getAttributeName();
        String value = nameValuePair.getValue();
        int columnIndex = this.databaseTable.getColumnNames(false).indexOf(attributeName);
        if (columnIndex == -1) {
            throw new DBException("Column name " + attributeName + " does not exist within this table");
        }
        this.databaseTable.setTableValue(columnIndex, rowIndex, value);
    }


    private void updateTable(ArrayList<String> filteredDataIds)
            throws DBException {
        int idColumnIndex = this.databaseTable.getColumnNames(false).indexOf("id");
        for (NameValuePair nameValuePair : this.nameValueList) {
            for (int rowIndex = 0; rowIndex < this.databaseTable.getNumberOfRows(); rowIndex++) {
                if (filteredDataIds.contains(this.databaseTable.getTableValue(idColumnIndex, rowIndex, false))) {
                    this.updateValue(nameValuePair, rowIndex);
                }
            }
        }
        databaseTable.writeDataToFile();
    }
}
