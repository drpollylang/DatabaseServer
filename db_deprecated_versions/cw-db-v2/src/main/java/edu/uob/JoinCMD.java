package edu.uob;


public class JoinCMD extends DBcmd {
    private String tableName1;
    private String tablePath1;
    private String tableName2;
    private String tablePath2;
    private String joinAttribute1;
    private String joinAttribute2;

    public JoinCMD(String tableName1, String tableName2, String joinAttribute1, String joinAttribute2) {
        this.tableName1 = tableName1;
        this.tablePath1 = getFilePath(databaseName, tableName1);
        this.tableName2 = tableName2;
        this.tablePath2 = getFilePath(databaseName, tableName2);
        this.joinAttribute1 = joinAttribute1;
        this.joinAttribute2 = joinAttribute2;
    }


    public String handleQuery() {
        DBTable databaseTable1;
        DBTable databaseTable2;
        try {
            databaseTable1 = instantiateDatabaseTable(this.tableName1, this.tablePath1);
            databaseTable2 = instantiateDatabaseTable(this.tableName2, this.tablePath2);
        } catch (DBException exception) {
            return exception.getMessage();
        }
        try {
            // Get indices of joining columns (handle errors if they dont exist)
            int joinColIndex1 = getJoinColumnIndex(databaseTable1, this.joinAttribute1);
            int joinColIndex2 = getJoinColumnIndex(databaseTable2, this.joinAttribute2);
            // attribute names are prepended with name of table from which they originated
            this.prependColumnName(databaseTable1);
            this.prependColumnName(databaseTable2);
            // Join tables
            databaseTable1.joinTable(databaseTable2, joinColIndex1, joinColIndex2);
        } catch (DBException error) {
            return error.getMessage();
        }
        return "[OK]\r\n" + databaseTable1;
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