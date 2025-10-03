package edu.uob;

import java.util.ArrayList;
import edu.uob.DBException.*;


public class JoinCMD extends DBcmd {
    private String tableName1;
    private String tablePath1;
    private String tableName2;
    private String tablePath2;
    private String joinAttribute1;
    private String joinAttribute2;
    private DBTable databaseTable1;
    private DBTable databaseTable2;

    public JoinCMD(ArrayList<Token> tokenStream) {
        this.tokenStream = tokenStream;
    }


    public void parseCommand() throws DBException {
        // <Join> ::= "JOIN "  [TableName] " " AND " [TableName] " ON " [AttributeName] "
        // AND " [AttributeName]
        checkQueryLength(9);
        checkKeyword(2, "and");
        checkKeyword(4, "on");
        checkKeyword(6, "and");
        checkIdentifier(1);
        checkIdentifier(3);
        checkIdentifier(5);
        checkIdentifier(7);

        // Get table names
        this.tableName1 = this.tokenStream.get(1).getTokenValue();
        this.tablePath1 = this.getFilePath(this.tableName1);
        this.tableName2 = this.tokenStream.get(3).getTokenValue();
        this.tablePath2 = this.getFilePath(this.tableName2);

        // Get attribute names
        this.joinAttribute1 = this.tokenStream.get(5).getTokenValue();
        this.joinAttribute2 = this.tokenStream.get(7).getTokenValue();
    }


    public String handleQuery() {
        try {
            this.instantiateTables();

            // Get indices of joining columns (handle errors if they dont exist)
            int joinColIndex1 = getJoinColumnIndex(databaseTable1, this.joinAttribute1);
            int joinColIndex2 = getJoinColumnIndex(databaseTable2, this.joinAttribute2);
            
            // attribute names are prepended with name of table from which they originated
            this.prependColumnName(databaseTable1);
            this.prependColumnName(databaseTable2);

            // Join tables
            databaseTable1.joinTable(databaseTable2, joinColIndex1, joinColIndex2);
        } catch (DBException exception) {
            return exception.getMessage();
        }
        return "[OK]\r\n" + databaseTable1;
    }


    private void instantiateTables() throws DBException {
        // Check the tables exist
        if (!checkFileExists(this.tablePath1)) {
            throw new tableDoesNotExistException(this.tableName1);
        }
        if (!checkFileExists(this.tablePath2)) {
            throw new tableDoesNotExistException(this.tableName2);
        }
        this.databaseTable1 = new DBTable(this.tablePath1);
        this.databaseTable2 = new DBTable(this.tablePath2);
    }


    private void prependColumnName(DBTable databaseTable) throws DBException {
        // attribute names are prepended with name of table from which they originated
        for (String columnName : databaseTable.getColumnNames(true)) {
            try {
                databaseTable.editColumnName(columnName, databaseTable.getTableName() +
                        "." + columnName);
            } catch (Exception error) {
                throw new DBException("Failed to prepend column " + columnName + " : " + error.getMessage());
            }
        }
    }


    private static int getJoinColumnIndex(DBTable databaseTable, String joinAttribute)
            throws DBException {
        int joinColIndex = databaseTable.getColumnNames(false).indexOf(joinAttribute.toLowerCase());
        if (joinColIndex == -1) {
            throw new DBException("Something went wrong while joining. " +
                    "The joining column " + joinAttribute + " does not exist in the table.");
        }
        return joinColIndex;
    }
}