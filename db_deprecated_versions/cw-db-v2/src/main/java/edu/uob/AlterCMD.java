package edu.uob;

import edu.uob.Token.*;
import edu.uob.DBException.*;
import java.util.*;

public class AlterCMD extends DBcmd {
    private String tableName;
    private String tablePath;
    private AlterationType alterationType;
    private List<String> attributeList;

    public AlterCMD(String tableName, Token.AlterationType alterationType, List<String> attributeList) {
        this.alterationType = alterationType;
        this.tableName = tableName;
        this.tablePath = getFilePath(databaseName, tableName);
        this.attributeList = attributeList;
    }

    public String handleQuery() {
        DBTable databaseTable;
        try {
            checkParameters();
            databaseTable = instantiateDatabaseTable(this.tableName, this.tablePath);
        } catch (DBException exception) {
            return exception.getMessage();
        }
        if (this.alterationType == AlterationType.ADD) {
            try {
                databaseTable.addColumn(this.attributeList.get(0));
                databaseTable.writeDataToFile();

            } catch (DBException error) {
                return error.getMessage();
            }
        }
        else {
            try {
                databaseTable.dropColumn(this.attributeList.get(0));
                databaseTable.writeDataToFile();
            } catch (DBException exception) {
                return exception.getMessage();
            }
        }
        return "[OK]";
    }


    private void checkParameters() throws DBException {
        if (this.attributeList.get(0).equals("id")) {
            throw new manuallyChangingIdColumnException();
        }
    }
}
