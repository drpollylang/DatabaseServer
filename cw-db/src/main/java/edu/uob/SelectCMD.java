package edu.uob;

import java.util.ArrayList;
import java.util.List;

public class SelectCMD extends DBcmd {

    public SelectCMD(ArrayList<Token> tokenStream) {
        this.tokenStream = tokenStream;
    }


    public void parseCommand() throws DBException {
        // <Select> ::= "SELECT <WildAttributeList> " FROM " [TableName] | "SELECT "
        // <WildAttributeList> "FROM " [TableName] " WHERE " <Condition>
        int endOfAttributeList = findToken(this.tokenStream, "from");
        int conditionStartIndex = findToken(this.tokenStream, "where") + 1;
        // Check query is valid
        checkKeyword(endOfAttributeList, "from");
        checkIdentifier(endOfAttributeList+1);

        this.tableName = this.tokenStream.get(endOfAttributeList+1).getTokenValue();
        this.tablePath = this.getFilePath(tableName);
        // Parse <WildAttributeList>
        this.parseAttributeList(this.tokenStream.subList(1, endOfAttributeList));

        // Parse "WHERE" <Condition> (if present)
        if (conditionStartIndex > 0) {
            this.parseConditions(this.tokenStream.subList(endOfAttributeList + 3, this.tokenStream.size() - 1));
        }
    }


    public String handleQuery() {
        try {
            this.instantiateDatabaseTable();
        } catch (DBException exception) {
            return exception.getMessage();
        }

        // Handle WHERE condition (if there is one)
        if (this.condition != null) {
            try {
                this.databaseTable.filterTable(this.condition);
            } catch (DBException error) {
                return error.getMessage();
            }
        }
        // Select columns (or wildcard)
        try {
            this.selectColumns();
        } catch (DBException exception) {
            return exception.getMessage();
        }
        // Everything went fine, so return response code/message and printable table
        return "[OK]\r\n" + databaseTable;
    }


    private void selectColumns() throws DBException {
        if (this.attributeList.isEmpty()) {
            throw new DBException("No columns were selected. " +
                    "Please select one or more columns, or use the wildcard character *.");
        }
        // If WildAttribute is NOT *, filter table by selected columns (attribute
        // names) before proceeding.
        if (!(this.attributeList.size() == 1 && this.attributeList.get(0).equals("*"))) {
            selectFilterColumns();
        }
    }


    private void selectFilterColumns() throws DBException {
        List<Integer> selectColumnIndices = new ArrayList<>();
        ArrayList<String> columnNames = this.databaseTable.getColumnNames(false);
        for (String column : this.attributeList) {
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
}