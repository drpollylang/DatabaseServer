package edu.uob;

import edu.uob.Token.*;
import edu.uob.DBException.*;
import java.util.*;

public class AlterCMD extends DBcmd {
    public AlterCMD(ArrayList<Token> tokenStream) {
        this.tokenStream = tokenStream;
    }

    public void parseCommand() throws DBException {
        // <Alter> ::= "ALTER" "TABLE" [TableName] " " <AlterationType> " " [AttributeName]
        // [AttributeName] ::= Plaintext
        checkQueryLength(6);
        checkTokenType(1, TokenType.keyword);
        checkKeyword(1, "table");
        checkIdentifier(2);
        checkIdentifier(4);

        this.tableName = this.tokenStream.get(2).getTokenValue();
        this.tablePath = this.getFilePath(this.tableName);
        this.alterationType = getAlterationType();
        this.attributeList = Arrays.asList(this.tokenStream.get(4).getTokenValue());
    }


    private AlterationType getAlterationType() throws DBException {
        AlterationType alterationType;
        if (this.tokenStream.get(3).getTokenValue().equals("add")) {
            alterationType = AlterationType.ADD;
        } else if (this.tokenStream.get(3).getTokenValue().equals("drop")) {
            alterationType = AlterationType.DROP;
        } else {
            throw new DBException("Malformed query. The third word of an ALTER query should be " +
                    "either ADD or DROP, but you input " + this.tokenStream.get(2).getTokenValue());
        }
        return alterationType;
    }


    public String handleQuery() {
        try {
            checkParameters();
            this.instantiateDatabaseTable();
        } catch (DBException exception) {
            return exception.getMessage();
        }
        if (this.alterationType == AlterationType.ADD) {
            try {
                this.databaseTable.addColumn(this.attributeList.get(0));
                this.databaseTable.writeDataToFile();

            } catch (DBException error) {
                return error.getMessage();
            }
        }
        else {
            try {
                this.databaseTable.dropColumn(this.attributeList.get(0));
                this.databaseTable.writeDataToFile();
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
