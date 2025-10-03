package edu.uob;

import java.util.ArrayList;
import edu.uob.DBException.*;

public class InsertCMD extends DBcmd {

    public InsertCMD(ArrayList<Token> tokenStream) {
        this.tokenStream = tokenStream;
    }


    public void parseCommand() throws DBException {
        // <Insert> ::= "INSERT " "INTO " [TableName] "VALUES " "(" <ValueList> ")"
        checkKeyword(1, "into");
        checkKeyword(3, "values");
        checkKeyword(4, "(");
        checkKeyword(this.tokenStream.size()-2, ")");
        checkIdentifier(2);

        String tableName = this.tokenStream.get(2).getTokenValue();
        this.tablePath = this.getFilePath(tableName);
        this.parseValueList(this.tokenStream.subList(5, this.tokenStream.size()-2));
    }


    public String handleQuery() {
        try {
            this.instantiateDatabaseTable();
            this.databaseTable.insertRow(this.valueList);
            this.databaseTable.writeDataToFile();
            return "[OK]";
        } catch (DBException exception) {
            return exception.getMessage();
        }
    }
}